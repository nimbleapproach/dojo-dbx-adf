# Databricks notebook source
import os
from pyspark.sql.functions import lit, col
from delta.tables import DeltaTable

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

spark.conf.set("tableObject.environment", ENVIRONMENT)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE SCHEMA tag02

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE VIEW staging_dim_entity AS

SELECT DISTINCT entity_code,
                entity_description,
                entity_type,
                legal_headquarters,
                administrative_city,
                date_established,
                consolidation_type,
                entity_local_currency,
                entity_group,
                entity_code_legacy,                  
                entity_hash_key,
                start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN NULL ELSE end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC               
FROM (
SELECT entity_code,
       entity_description,
       entity_type,
       legal_headquarters,
       administrative_city,
       date_established,
       consolidation_type,
       entity_local_currency,
       entity_group,
       entity_code_legacy,       
       entity_hash_key,
       MIN(date_updated) OVER(PARTITION BY entity_code, grp_id2) AS start_datetime,
       MAX(COALESCE(next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY entity_code, grp_id2) AS end_datetime
FROM (
SELECT *,
      MAX(grp_id) OVER(PARTITION BY entity_code, entity_hash_key ORDER BY date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT entity_code,
       entity_description,
       entity_type,
       legal_headquarters,
       administrative_city,
       date_established,
       consolidation_type,
       entity_local_currency,
       entity_group,
       entity_code_legacy,       
       entity_hash_key,
       date_updated,
      (CASE WHEN LAG(entity_hash_key) OVER (PARTITION BY entity_code ORDER BY date_updated) IS NULL OR 
                 entity_hash_key <> LAG(entity_hash_key) OVER (PARTITION BY entity_code ORDER BY date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY entity_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.entity_code ORDER BY date_updated) AS row_id,
             a.entity_code,
             a.entity_description,
             a.entity_type,
             a.legal_headquarters,
             a.administrative_city,
             a.date_established,
             a.consolidation_type,
             a.entity_local_currency,
             a.entity_group,
             a.entity_code_legacy,
             a.entity_hash_key,
             a.date_updated     
      FROM ( SELECT DISTINCT TRIM(a.COD_AZIENDA) AS entity_code,
                             TRIM(a.DESC_AZIENDA0) AS entity_description,
                             TRIM(a.FLAG_AZIENDA) AS entity_type,
                             TRIM(a.SEDE_LEGALE) AS legal_headquarters,
                             TRIM(a.SEDE_AMMINISTRATIVA) AS administrative_city,
                             CAST(TRIM(a.DATA_COSTITUZIONE) AS TIMESTAMP) AS date_established,
                             TRIM(a.TIPO_CONSOLIDAMENTO) AS consolidation_type,
                             TRIM(a.COD_VALUTA) AS entity_local_currency,
                             egm.entity_group,
                             egm.entity_code_legacy,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_AZIENDA0), ''), COALESCE(TRIM(a.FLAG_AZIENDA), ''), COALESCE(TRIM(a.SEDE_LEGALE), ''), COALESCE(TRIM(a.SEDE_AMMINISTRATIVA), ''), COALESCE(TRIM(a.DATA_COSTITUZIONE), ''), COALESCE(TRIM(a.TIPO_CONSOLIDAMENTO), ''), COALESCE(TRIM(a.COD_VALUTA), ''), COALESCE(TRIM(egm.entity_group), ''), COALESCE(TRIM(egm.entity_code_legacy), '')), 256) AS entity_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.azienda a
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_entity b
                ON LOWER(TRIM(a.COD_AZIENDA)) = LOWER(b.entity_code)
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.entity_group_mapping egm
                ON LOWER(TRIM(a.COD_AZIENDA)) = LOWER(egm.entity_code)            
              WHERE LOWER(b.entity_code) IS NULL
              UNION -- We either want to insert all entity codes we haven't seen before or we want to insert only entity codes with changed attributes 
              SELECT DISTINCT TRIM(a.COD_AZIENDA) AS entity_code,
                              TRIM(a.DESC_AZIENDA0) AS entity_description,
                              TRIM(a.FLAG_AZIENDA) AS entity_type,
                              TRIM(a.SEDE_LEGALE) AS legal_headquarters,
                              TRIM(a.SEDE_AMMINISTRATIVA) AS administrative_city,
                              CAST(TRIM(a.DATA_COSTITUZIONE) AS TIMESTAMP) AS date_established,
                              TRIM(a.TIPO_CONSOLIDAMENTO) AS consolidation_type,
                              TRIM(a.COD_VALUTA) AS entity_local_currency,
                              egm2.entity_group,
                              egm2.entity_code_legacy,                              
                              SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_AZIENDA0), ''), COALESCE(TRIM(a.FLAG_AZIENDA), ''), COALESCE(TRIM(a.SEDE_LEGALE), ''), COALESCE(TRIM(a.SEDE_AMMINISTRATIVA), ''), COALESCE(TRIM(a.DATA_COSTITUZIONE), ''), COALESCE(TRIM(a.TIPO_CONSOLIDAMENTO), ''), COALESCE(TRIM(a.COD_VALUTA), ''), COALESCE(TRIM(egm2.entity_group), ''), COALESCE(TRIM(egm2.entity_code_legacy), '')), 256) AS entity_hash_key,
                              CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.azienda a
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.entity_group_mapping egm2
                ON LOWER(TRIM(a.COD_AZIENDA)) = LOWER(egm2.entity_code)  
              INNER JOIN gold_{ENVIRONMENT}.tag02.dim_entity b
                ON LOWER(TRIM(a.COD_AZIENDA)) = LOWER(b.entity_code)
               AND CAST(a.DATEUPD AS TIMESTAMP) > b.start_datetime
               AND SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_AZIENDA0), ''), COALESCE(TRIM(a.FLAG_AZIENDA), ''), COALESCE(TRIM(a.SEDE_LEGALE), ''), COALESCE(TRIM(a.SEDE_AMMINISTRATIVA), ''), COALESCE(TRIM(a.DATA_COSTITUZIONE), ''), COALESCE(TRIM(a.TIPO_CONSOLIDAMENTO), ''), COALESCE(TRIM(a.COD_VALUTA), ''), COALESCE(TRIM(egm2.entity_group), ''), COALESCE(TRIM(egm2.entity_code_legacy), '')), 256) <> b.entity_hash_key
               AND b.is_current = 1) a) hk) x) y) z
""")


# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT entity_code, NOW() AS Sys_Gold_ModifiedDateTime_UTC, MIN(start_datetime) AS min_start_datetime FROM staging_dim_entity GROUP BY entity_code""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_entity")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_entity') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_entity.is_current = 1 AND dim_entity.entity_code = updates.entity_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "end_datetime": "updates.min_start_datetime",
# MAGIC       "is_current": lit(0),
# MAGIC       "dim_entity.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT entity_code,
# MAGIC        entity_description,
# MAGIC        entity_type,
# MAGIC        legal_headquarters,
# MAGIC        administrative_city,
# MAGIC        date_established,
# MAGIC        consolidation_type,
# MAGIC        entity_local_currency,
# MAGIC        entity_group,
# MAGIC        entity_code_legacy,       
# MAGIC        entity_hash_key,
# MAGIC        start_datetime,
# MAGIC        end_datetime,
# MAGIC        is_current,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_entity     
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_entity")

# COMMAND ----------

# MAGIC %md **Add in those any entity's that are being adjusted for cross selling/cross allocation E.G. UK2 and IE1 transactions being allocated to VU**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO gold_${tableObject.environment}.tag02.dim_entity (entity_code, entity_description, entity_type, legal_headquarters, administrative_city, date_established, consolidation_type, entity_local_currency, entity_group, entity_code_legacy, entity_hash_key, start_datetime, end_datetime, is_current, Sys_Gold_InsertedDateTime_UTC, Sys_Gold_ModifiedDateTime_UTC)
# MAGIC SELECT 'VU' AS entity_code, 
# MAGIC        'Infinigate Cloud' AS entity_description, 
# MAGIC        1 AS entity_type, 
# MAGIC        NULL AS legal_headquarters, 
# MAGIC        NULL AS administrative_city, 
# MAGIC        NULL AS date_established,
# MAGIC        NULL AS consolidation_type, 
# MAGIC        NULL AS entity_local_currency,
# MAGIC        'IG Cloud' AS entity_group, 
# MAGIC        'VU' AS entity_code_legacy,
# MAGIC         NULL AS entity_hash_key, 
# MAGIC         NOW() AS start_datetime, 
# MAGIC         NULL AS end_datetime, 
# MAGIC         1 AS is_current, 
# MAGIC         NOW() AS Sys_Gold_InsertedDateTime_UTC, 
# MAGIC         NOW() AS Sys_Gold_ModifiedDateTime_UTC
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM gold_${tableObject.environment}.tag02.dim_entity
# MAGIC   WHERE entity_code = 'VU'
# MAGIC )