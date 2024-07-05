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
          
CREATE OR REPLACE VIEW staging_dim_scenario AS

SELECT DISTINCT scenario_code,
                scenario_type,
                ScenarioGroup,
                original_scenario_code,
                scenario_description,
                COD_SCENARIO_PREC,
                COD_SCENARIO_SUCC,
                COD_SCENARIO_RIF1,
                COD_SCENARIO_RIF2,
                COD_SCENARIO_RIF3,
                COD_SCENARIO_RIF4,
                COD_SCENARIO_RIF5,
                COD_AZI_CAPOGRUPPO,
                scenario_currency_code,
                COD_CATEGORIA_GERARCHIA,
                COD_CATEGORIA_ELEGER,
                COD_ESERCIZIO,
                scenario_version_description,
                scenario_hash_key,
                start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN NULL ELSE end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC               
FROM (
SELECT scenario_code,
       scenario_type,
       ScenarioGroup,
       original_scenario_code,
       scenario_description,
       COD_SCENARIO_PREC,
       COD_SCENARIO_SUCC,
       COD_SCENARIO_RIF1,
       COD_SCENARIO_RIF2,
       COD_SCENARIO_RIF3,
       COD_SCENARIO_RIF4,
       COD_SCENARIO_RIF5,
       COD_AZI_CAPOGRUPPO,
       scenario_currency_code,
       COD_CATEGORIA_GERARCHIA,
       COD_CATEGORIA_ELEGER,
       COD_ESERCIZIO,
       scenario_version_description,
       scenario_hash_key,
       MIN(date_updated) OVER(PARTITION BY scenario_code, grp_id2) AS start_datetime,
       MAX(COALESCE(next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY scenario_code, grp_id2) AS end_datetime
FROM (
SELECT *,
      MAX(grp_id) OVER(PARTITION BY scenario_code, scenario_hash_key ORDER BY date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT scenario_code,
       scenario_type,
       ScenarioGroup,
       original_scenario_code,
       scenario_description,
       COD_SCENARIO_PREC,
       COD_SCENARIO_SUCC,
       COD_SCENARIO_RIF1,
       COD_SCENARIO_RIF2,
       COD_SCENARIO_RIF3,
       COD_SCENARIO_RIF4,
       COD_SCENARIO_RIF5,
       COD_AZI_CAPOGRUPPO,
       scenario_currency_code,
       COD_CATEGORIA_GERARCHIA,
       COD_CATEGORIA_ELEGER,
       COD_ESERCIZIO,
       scenario_version_description,
       scenario_hash_key,
       date_updated,
      (CASE WHEN LAG(scenario_hash_key) OVER (PARTITION BY scenario_code ORDER BY date_updated) IS NULL OR 
                 scenario_hash_key <> LAG(scenario_hash_key) OVER (PARTITION BY scenario_code ORDER BY date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY scenario_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.scenario_code ORDER BY date_updated) AS row_id,
             a.scenario_code,
             a.scenario_type,
             a.ScenarioGroup,
             a.original_scenario_code,
             a.scenario_description,
             a.COD_SCENARIO_PREC,
             a.COD_SCENARIO_SUCC,
             a.COD_SCENARIO_RIF1,
             a.COD_SCENARIO_RIF2,
             a.COD_SCENARIO_RIF3,
             a.COD_SCENARIO_RIF4,
             a.COD_SCENARIO_RIF5,
             a.COD_AZI_CAPOGRUPPO,
             a.scenario_currency_code,
             a.COD_CATEGORIA_GERARCHIA,
             a.COD_CATEGORIA_ELEGER,
             a.COD_ESERCIZIO,
             a.scenario_version_description,
             a.scenario_hash_key,
             a.date_updated     
      FROM ( SELECT DISTINCT TRIM(a.COD_SCENARIO) AS scenario_code,
                             TRIM(a.TIPO_SCENARIO) AS scenario_type,
                            (CASE WHEN a.COD_SCENARIO LIKE '%ACT%' THEN 'Actual'
                                  WHEN a.COD_SCENARIO LIKE '%FC%' THEN 'Forecast'
                                  WHEN a.COD_SCENARIO LIKE '%BUD%' THEN 'Plan' END) AS ScenarioGroup,
                             TRIM(a.COD_SCENARIO_ORIGINARIO) AS original_scenario_code,
                             TRIM(a.DESC_SCENARIO) AS scenario_description,
                             TRIM(a.COD_SCENARIO_PREC) AS COD_SCENARIO_PREC,
                             TRIM(a.COD_SCENARIO_SUCC) AS COD_SCENARIO_SUCC,
                             TRIM(a.COD_SCENARIO_RIF1) AS COD_SCENARIO_RIF1,
                             TRIM(a.COD_SCENARIO_RIF2) AS COD_SCENARIO_RIF2,
                             TRIM(a.COD_SCENARIO_RIF3) AS COD_SCENARIO_RIF3,
                             TRIM(a.COD_SCENARIO_RIF4) AS COD_SCENARIO_RIF4,
                             TRIM(a.COD_SCENARIO_RIF5) AS COD_SCENARIO_RIF5,
                             TRIM(a.COD_AZI_CAPOGRUPPO) AS COD_AZI_CAPOGRUPPO,
                             TRIM(a.COD_VALUTA) AS scenario_currency_code,
                             TRIM(a.COD_CATEGORIA_GERARCHIA) AS COD_CATEGORIA_GERARCHIA,
                             TRIM(a.COD_CATEGORIA_ELEGER) AS COD_CATEGORIA_ELEGER,
                             TRIM(a.COD_ESERCIZIO) AS COD_ESERCIZIO,
                             TRIM(a.DESC_VERSIONE) AS scenario_version_description,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.TIPO_SCENARIO), ''), COALESCE(TRIM(a.COD_SCENARIO_ORIGINARIO), ''), COALESCE(TRIM(a.DESC_SCENARIO), ''),                 COALESCE(TRIM(a.COD_SCENARIO_PREC), ''), COALESCE(TRIM(a.COD_SCENARIO_SUCC), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF1), ''), COALESCE(TRIM                (a.COD_SCENARIO_RIF2), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF3), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF4), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF5),                 ''), COALESCE(TRIM(a.COD_AZI_CAPOGRUPPO), ''), COALESCE(TRIM(a.COD_VALUTA), ''), COALESCE(TRIM(a.COD_CATEGORIA_GERARCHIA), ''), COALESCE(TRIM                (a.COD_CATEGORIA_ELEGER), ''), COALESCE(TRIM(a.COD_ESERCIZIO), ''), COALESCE(TRIM(a.DESC_VERSIONE), '')), 256) AS scenario_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
               FROM silver_{ENVIRONMENT}.tag02.scenario a
               LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_scenario b
                 ON LOWER(TRIM(a.COD_SCENARIO)) = LOWER(b.scenario_code)
               WHERE LOWER(b.scenario_code) IS NULL
               UNION -- We either want to insert all scenario codes we haven't seen before or we want to insert only scenario codes with changed attributes
               SELECT DISTINCT TRIM(a.COD_SCENARIO) AS scenario_code,
                             TRIM(a.TIPO_SCENARIO) AS scenario_type,
                            (CASE WHEN a.COD_SCENARIO LIKE '%ACT%' THEN 'Actual'
                                  WHEN a.COD_SCENARIO LIKE '%FC%' THEN 'Forecast'
                                  WHEN a.COD_SCENARIO LIKE '%BUD%' THEN 'Plan' END) AS ScenarioGroup,
                             TRIM(a.COD_SCENARIO_ORIGINARIO) AS original_scenario_code,
                             TRIM(a.DESC_SCENARIO) AS scenario_description,
                             TRIM(a.COD_SCENARIO_PREC) AS COD_SCENARIO_PREC,
                             TRIM(a.COD_SCENARIO_SUCC) AS COD_SCENARIO_SUCC,
                             TRIM(a.COD_SCENARIO_RIF1) AS COD_SCENARIO_RIF1,
                             TRIM(a.COD_SCENARIO_RIF2) AS COD_SCENARIO_RIF2,
                             TRIM(a.COD_SCENARIO_RIF3) AS COD_SCENARIO_RIF3,
                             TRIM(a.COD_SCENARIO_RIF4) AS COD_SCENARIO_RIF4,
                             TRIM(a.COD_SCENARIO_RIF5) AS COD_SCENARIO_RIF5,
                             TRIM(a.COD_AZI_CAPOGRUPPO) AS COD_AZI_CAPOGRUPPO,
                             TRIM(a.COD_VALUTA) AS scenario_currency_code,
                             TRIM(a.COD_CATEGORIA_GERARCHIA) AS COD_CATEGORIA_GERARCHIA,
                             TRIM(a.COD_CATEGORIA_ELEGER) AS COD_CATEGORIA_ELEGER,
                             TRIM(a.COD_ESERCIZIO) AS COD_ESERCIZIO,
                             TRIM(a.DESC_VERSIONE) AS scenario_version_description,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.TIPO_SCENARIO), ''), COALESCE(TRIM(a.COD_SCENARIO_ORIGINARIO), ''), COALESCE(TRIM(a.DESC_SCENARIO), ''),                 COALESCE(TRIM(a.COD_SCENARIO_PREC), ''), COALESCE(TRIM(a.COD_SCENARIO_SUCC), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF1), ''), COALESCE(TRIM                (a.COD_SCENARIO_RIF2), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF3), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF4), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF5),                 ''), COALESCE(TRIM(a.COD_AZI_CAPOGRUPPO), ''), COALESCE(TRIM(a.COD_VALUTA), ''), COALESCE(TRIM(a.COD_CATEGORIA_GERARCHIA), ''), COALESCE(TRIM                (a.COD_CATEGORIA_ELEGER), ''), COALESCE(TRIM(a.COD_ESERCIZIO), ''), COALESCE(TRIM(a.DESC_VERSIONE), '')), 256) AS scenario_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
               FROM silver_{ENVIRONMENT}.tag02.scenario a
               INNER JOIN gold_{ENVIRONMENT}.tag02.dim_scenario b
                 ON LOWER(TRIM(a.COD_SCENARIO)) = LOWER(b.scenario_code)
                AND CAST(a.DATEUPD AS TIMESTAMP) > b.start_datetime
                AND SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.TIPO_SCENARIO), ''), COALESCE(TRIM(a.COD_SCENARIO_ORIGINARIO), ''), COALESCE(TRIM(a.DESC_SCENARIO), ''),                 COALESCE(TRIM(a.COD_SCENARIO_PREC), ''), COALESCE(TRIM(a.COD_SCENARIO_SUCC), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF1), ''), COALESCE(TRIM                (a.COD_SCENARIO_RIF2), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF3), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF4), ''), COALESCE(TRIM(a.COD_SCENARIO_RIF5),                 ''), COALESCE(TRIM(a.COD_AZI_CAPOGRUPPO), ''), COALESCE(TRIM(a.COD_VALUTA), ''), COALESCE(TRIM(a.COD_CATEGORIA_GERARCHIA), ''), COALESCE(TRIM                (a.COD_CATEGORIA_ELEGER), ''), COALESCE(TRIM(a.COD_ESERCIZIO), ''), COALESCE(TRIM(a.DESC_VERSIONE), '')), 256) <> b.scenario_hash_key
                AND b.is_current = 1) a) hk) x) y) z
""")


# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT scenario_code, NOW() AS Sys_Gold_ModifiedDateTime_UTC, MIN(start_datetime) AS min_start_datetime FROM staging_dim_scenario GROUP BY scenario_code""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_scenario")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_scenario') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_scenario.is_current = 1 AND dim_scenario.scenario_code = updates.scenario_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "end_datetime": "updates.min_start_datetime",
# MAGIC       "is_current": lit(0),
# MAGIC       "dim_scenario.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT scenario_code,
# MAGIC        scenario_type,
# MAGIC        ScenarioGroup,
# MAGIC        original_scenario_code,
# MAGIC        scenario_description,
# MAGIC        COD_SCENARIO_PREC,
# MAGIC        COD_SCENARIO_SUCC,
# MAGIC        COD_SCENARIO_RIF1,
# MAGIC        COD_SCENARIO_RIF2,
# MAGIC        COD_SCENARIO_RIF3,
# MAGIC        COD_SCENARIO_RIF4,
# MAGIC        COD_SCENARIO_RIF5,
# MAGIC        COD_AZI_CAPOGRUPPO,
# MAGIC        scenario_currency_code,
# MAGIC        COD_CATEGORIA_GERARCHIA,
# MAGIC        COD_CATEGORIA_ELEGER,
# MAGIC        COD_ESERCIZIO,
# MAGIC        scenario_version_description,
# MAGIC        scenario_hash_key,
# MAGIC        start_datetime,
# MAGIC        end_datetime,
# MAGIC        is_current,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_scenario     
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_scenario")
