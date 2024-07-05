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
          
CREATE OR REPLACE VIEW staging_dim_vendor AS

SELECT DISTINCT vendor_code,
                vendor_name,
                vendor_hash_key,
                start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN NULL ELSE end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC               
FROM (
SELECT vendor_code,
       vendor_name,
       vendor_hash_key,
       MIN(date_updated) OVER(PARTITION BY vendor_code, grp_id2) AS start_datetime,
       MAX(COALESCE(next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY vendor_code, grp_id2) AS end_datetime
FROM (
SELECT *,
      MAX(grp_id) OVER(PARTITION BY vendor_code, vendor_name ORDER BY date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT vendor_code,
       vendor_name,
       vendor_hash_key,
       date_updated,
      (CASE WHEN LAG(vendor_name) OVER (PARTITION BY vendor_code ORDER BY date_updated) IS NULL OR 
                 vendor_name <> LAG(vendor_name) OVER (PARTITION BY vendor_code ORDER BY date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY vendor_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.vendor_code ORDER BY date_updated) AS row_id,
             a.vendor_code,
             a.vendor_name,
             a.vendor_hash_key,
             a.date_updated    
      FROM ( SELECT DISTINCT TRIM(a.COD_DEST1) AS vendor_code,
                             TRIM(a.DESC_DEST10) AS vendor_name,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST10), '')), 256) AS vendor_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.Dest1 a
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_vendor b
                ON LOWER(TRIM(a.COD_DEST1)) = LOWER(b.vendor_code)
              WHERE LOWER(b.vendor_code) IS NULL
              UNION -- We either want to insert all vendor codes we haven't seen before or we want to insert only vendor codes with changed attributes
              SELECT DISTINCT TRIM(a.COD_DEST1) AS vendor_code,
                             TRIM(a.DESC_DEST10) AS vendor_name,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST10), '')), 256) AS vendor_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.Dest1 a
              INNER JOIN gold_{ENVIRONMENT}.tag02.dim_vendor b
                ON LOWER(TRIM(a.COD_DEST1)) = LOWER(b.vendor_code)
               AND CAST(a.DATEUPD AS TIMESTAMP) > b.start_datetime
               AND SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST10), '')), 256) <> b.vendor_hash_key
               AND b.is_current = 1) a) hk) x) y) z
""")


# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT vendor_code, NOW() AS Sys_Gold_ModifiedDateTime_UTC, MIN(start_datetime) AS min_start_datetime FROM staging_dim_vendor GROUP BY vendor_code""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_vendor")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_vendor') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_vendor.is_current = 1 AND dim_vendor.vendor_code = updates.vendor_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "end_datetime": "updates.min_start_datetime",
# MAGIC       "is_current": lit(0),
# MAGIC       "dim_vendor.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT vendor_code,
# MAGIC        vendor_name,
# MAGIC        vendor_hash_key,
# MAGIC        start_datetime,
# MAGIC        end_datetime,
# MAGIC        is_current,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_vendor     
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_vendor")
