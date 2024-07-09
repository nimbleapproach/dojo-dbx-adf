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

CREATE OR REPLACE VIEW staging_dim_exchange_rate AS

SELECT DISTINCT exchange_rate_id,
                exchange_rate_code,
                scenario_code,
                period,
                currency_code,
                exchange_rate,
                exchange_rate_hash_key,
                start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN NULL ELSE end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC               
FROM (
SELECT grp_id2 AS exchange_rate_id,
       exchange_rate_code,
       scenario_code,
       period,
       currency_code,
       exchange_rate,
       exchange_rate_hash_key,
       MIN(date_updated) OVER(PARTITION BY exchange_rate_code, grp_id2) AS start_datetime,
       MAX(COALESCE(next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY exchange_rate_code, grp_id2) AS end_datetime
FROM (
SELECT *,
      MAX(grp_id) OVER(PARTITION BY exchange_rate_code, exchange_rate_hash_key ORDER BY date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT exchange_rate_code,
       scenario_code,
       period,
       currency_code,
       exchange_rate,
       exchange_rate_hash_key,
       date_updated,
      (CASE WHEN LAG(exchange_rate_hash_key) OVER (PARTITION BY exchange_rate_code ORDER BY date_updated) IS NULL OR 
                 exchange_rate_hash_key <> LAG(exchange_rate_hash_key) OVER (PARTITION BY exchange_rate_code ORDER BY date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY exchange_rate_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.exchange_rate_code  ORDER BY date_updated) AS row_id,
             a.exchange_rate_code,
             a.scenario_code,
             a.period,
             a.currency_code,
             a.exchange_rate,
             a.exchange_rate_hash_key,
             a.date_updated      
      FROM ( SELECT DISTINCT CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA)) AS exchange_rate_code,      
                             CAST(TRIM(a.COD_SCENARIO) AS STRING) AS scenario_code,
                             CAST(TRIM(a.COD_PERIODO) AS STRING) AS period,
                             CAST(TRIM(a.COD_VALUTA) AS STRING) AS currency_code,
                             CAST(a.CAMBIO_PERIODO as decimal(18, 4)) AS exchange_rate,
                             SHA2(COALESCE(TRIM(cast(a.CAMBIO_PERIODO as decimal(18, 4))),''), 256) AS exchange_rate_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.dati_cambio a
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_exchange_rate b
                ON LOWER(CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA))) = LOWER(b.exchange_rate_code)
              WHERE LOWER(b.exchange_rate_code) IS NULL
              UNION -- We either want to insert all exchagne rate codes we haven't seen before or we want to insert only exchange rate codes with changed attributes 
              SELECT DISTINCT CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA)) AS exchange_rate_code,      
                             CAST(TRIM(a.COD_SCENARIO) AS STRING) AS scenario_code,
                             CAST(TRIM(a.COD_PERIODO) AS STRING) AS period,
                             CAST(TRIM(a.COD_VALUTA) AS STRING) AS currency_code,
                             CAST(a.CAMBIO_PERIODO as decimal(18, 4)) AS exchange_rate,
                             SHA2(COALESCE(TRIM(cast(a.CAMBIO_PERIODO as decimal(18, 4))),''), 256) AS exchange_rate_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.dati_cambio a
              INNER JOIN gold_{ENVIRONMENT}.tag02.dim_exchange_rate b
                ON LOWER(CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA))) = LOWER(b.exchange_rate_code)
               AND CAST(a.DATEUPD AS TIMESTAMP) > b.start_datetime
               AND SHA2(COALESCE(TRIM(cast(a.CAMBIO_PERIODO as decimal(18, 4))),''), 256) <> b.exchange_rate_hash_key
               AND b.is_current = 1) a) hk) x) y) z
""")


# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT exchange_rate_code, NOW() AS Sys_Gold_ModifiedDateTime_UTC, MIN(start_datetime) AS min_start_datetime FROM staging_dim_exchange_rate GROUP BY exchange_rate_code""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_exchange_rate")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_exchange_rate') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_exchange_rate.is_current = 1 AND dim_exchange_rate.exchange_rate_code = updates.exchange_rate_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "end_datetime": "updates.min_start_datetime",
# MAGIC       "is_current": lit(0),
# MAGIC       "dim_exchange_rate.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT exchange_rate_id,
# MAGIC        exchange_rate_code,
# MAGIC        scenario_code,
# MAGIC        period,
# MAGIC        currency_code,
# MAGIC        exchange_rate,
# MAGIC        exchange_rate_hash_key,
# MAGIC        start_datetime,
# MAGIC        end_datetime,
# MAGIC        is_current,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_exchange_rate
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_exchange_rate")
