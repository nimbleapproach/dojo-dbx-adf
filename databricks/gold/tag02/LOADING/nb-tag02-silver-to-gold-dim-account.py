# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable

ENVIRONMENT = os.environ["__ENVIRONMENT__"]


# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE SCHEMA tag02

# COMMAND ----------

spark.sql(f"""
          
CREATE OR REPLACE VIEW staging_dim_account AS

SELECT DISTINCT account_code,
                account_description,
                account_description_extended,
                account_hash_key,
                start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN NULL ELSE end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM (
SELECT account_code,
       account_description,
       account_description_extended,
       account_hash_key,
       MIN(date_updated) OVER(PARTITION BY account_code, grp_id2) AS start_datetime,
       MAX(COALESCE(next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY account_code, grp_id2) AS end_datetime
FROM (
SELECT *,
      MAX(grp_id) OVER(PARTITION BY account_code, account_hash_key ORDER BY date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT account_code,
       account_description,
       account_description_extended,
       account_hash_key,
       date_updated,
      (CASE WHEN LAG(account_hash_key) OVER (PARTITION BY account_code ORDER BY date_updated) IS NULL OR 
                 account_hash_key <> LAG(account_hash_key) OVER (PARTITION BY account_code ORDER BY date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY account_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.account_code ORDER BY date_updated) AS row_id,
             a.account_code,
             a.account_description,
             a.account_description_extended,
             a.account_hash_key,
             a.date_updated     
      FROM ( SELECT DISTINCT 
                    TRIM(a.COD_CONTO) AS account_code,
                    TRIM(a.DESC_CONTO0) AS account_description,
                    TRIM(a.DESC_CONTO1) AS account_description_extended,
                    SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_CONTO0), ''), COALESCE(TRIM(a.DESC_CONTO1), '')), 256) AS account_hash_key,
                    CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
             FROM silver_{ENVIRONMENT}.tag02.conto a
             LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_account b
               ON LOWER(TRIM(a.COD_CONTO)) = LOWER(b.account_code)
             WHERE LOWER(b.account_code) IS NULL
             UNION    -- We either want to insert all account codes we haven't seen before or we want to insert only account codes with changed attributes
             SELECT DISTINCT 
                    TRIM(a.COD_CONTO) AS account_code,
                    TRIM(a.DESC_CONTO0) AS account_description,
                    TRIM(a.DESC_CONTO1) AS account_description_extended,
                    SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_CONTO0), ''), COALESCE(TRIM(a.DESC_CONTO1), '')), 256) AS account_hash_key,
                    CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
             FROM silver_{ENVIRONMENT}.tag02.conto a
             INNER JOIN gold_{ENVIRONMENT}.tag02.dim_account b
               ON LOWER(TRIM(a.COD_CONTO)) = LOWER(b.account_code)
              AND CAST(a.DATEUPD AS TIMESTAMP) > b.start_datetime
              AND SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_CONTO0), ''), COALESCE(TRIM(a.DESC_CONTO1), '')), 256) <> b.account_hash_key
              AND b.is_current = 1) a) hk) x) y) z
""")



# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT account_code, NOW() AS Sys_Gold_ModifiedDateTime_UTC, MIN(start_datetime) AS min_start_datetime FROM staging_dim_account GROUP BY account_code""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_account")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_account') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_account.is_current = 1 AND dim_account.account_code = updates.account_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "dim_account.end_datetime": "updates.min_start_datetime",
# MAGIC       "dim_account.is_current": lit(0),
# MAGIC       "dim_account.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()
# MAGIC

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT account_code,
# MAGIC        account_description,
# MAGIC        account_description_extended,
# MAGIC        account_hash_key,
# MAGIC        start_datetime,
# MAGIC        end_datetime,
# MAGIC        is_current,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_account     
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_account")
# MAGIC
