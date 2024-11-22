# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

spark.conf.set("tableObject.environment", ENVIRONMENT)

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE SCHEMA tag02

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE VIEW staging_dim_exchange_rate AS

SELECT a.exchange_rate_code,
       a.scenario_code,
       a.period,
       a.currency_code,
       a.exchange_rate,
       a.exchange_rate_hash_key,
       MIN(a.date_updated) OVER(PARTITION BY a.exchange_rate_code) AS start_datetime,
       CAST(NULL AS TIMESTAMP) AS end_datetime,
       CAST(1 AS INTEGER) AS is_current,
       NOW() AS Sys_Gold_InsertedDateTime_UTC,
       NOW() AS Sys_Gold_ModifiedDateTime_UTC           
FROM ( SELECT DISTINCT UPPER(CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA))) AS exchange_rate_code,      
                       CAST(TRIM(a.COD_SCENARIO) AS STRING) AS scenario_code,
                       CAST(TRIM(a.COD_PERIODO) AS STRING) AS period,
                       CAST(TRIM(a.COD_VALUTA) AS STRING) AS currency_code,
                       CAST(a.CAMBIO_PERIODO as decimal(18, 4)) AS exchange_rate,
                       SHA2(COALESCE(TRIM(cast(a.CAMBIO_PERIODO as decimal(18, 4))),''), 256) AS exchange_rate_hash_key,
                       CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
FROM silver_{ENVIRONMENT}.tag02.dati_cambio a
LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_exchange_rate b
  ON UPPER(CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA))) = b.exchange_rate_code
WHERE LOWER(b.exchange_rate_code) IS NULL
  AND a.Sys_Silver_IsCurrent
  AND (NOT a.Sys_Silver_IsDeleted OR a.Sys_Silver_IsDeleted IS NULL)
UNION -- We either want to insert all exchange rate codes we haven't seen before or we want to insert only exchange rate codes with changed attributes 
SELECT DISTINCT UPPER(CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA))) AS exchange_rate_code,      
                CAST(TRIM(a.COD_SCENARIO) AS STRING) AS scenario_code,
                CAST(TRIM(a.COD_PERIODO) AS STRING) AS period,
                CAST(TRIM(a.COD_VALUTA) AS STRING) AS currency_code,
                CAST(a.CAMBIO_PERIODO as decimal(18, 4)) AS exchange_rate,
                SHA2(COALESCE(TRIM(cast(a.CAMBIO_PERIODO as decimal(18, 4))),''), 256) AS exchange_rate_hash_key,
                CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
FROM silver_{ENVIRONMENT}.tag02.dati_cambio a
INNER JOIN gold_{ENVIRONMENT}.tag02.dim_exchange_rate b
  ON UPPER(CONCAT(TRIM(a.COD_SCENARIO),'_',TRIM(a.COD_PERIODO),'_',TRIM(a.COD_VALUTA))) = b.exchange_rate_code
WHERE a.Sys_Silver_IsCurrent
  AND (NOT a.Sys_Silver_IsDeleted OR a.Sys_Silver_IsDeleted IS NULL)
  AND SHA2(COALESCE(TRIM(cast(a.CAMBIO_PERIODO as decimal(18, 4))),''), 256) <> b.exchange_rate_hash_key) a
""")


# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT exchange_rate_code, exchange_rate, NOW() AS Sys_Gold_ModifiedDateTime_UTC FROM staging_dim_exchange_rate""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_exchange_rate")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_exchange_rate') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_exchange_rate.exchange_rate_code = updates.exchange_rate_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "exchange_rate": "updates.exchange_rate",
# MAGIC       "dim_exchange_rate.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT a.exchange_rate_code,
# MAGIC        a.scenario_code,
# MAGIC        a.period,
# MAGIC        a.currency_code,
# MAGIC        a.exchange_rate,
# MAGIC        a.exchange_rate_hash_key,
# MAGIC        a.start_datetime,
# MAGIC        a.end_datetime,
# MAGIC        a.is_current,
# MAGIC        a.Sys_Gold_InsertedDateTime_UTC,
# MAGIC        a.Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_exchange_rate a
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_exchange_rate b
# MAGIC     ON a.exchange_rate_code = b.exchange_rate_code
# MAGIC WHERE b.exchange_rate_code IS NULL
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_exchange_rate")
