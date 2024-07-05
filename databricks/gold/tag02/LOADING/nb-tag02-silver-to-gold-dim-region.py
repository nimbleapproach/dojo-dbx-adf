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
          
CREATE OR REPLACE VIEW staging_dim_region AS

SELECT DISTINCT region_code,
                region_name,
                region_hash_key,
                start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN NULL ELSE end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC               
FROM (
SELECT grp_id2 AS region_id,
       region_code,
       region_name,
       region_hash_key,
       MIN(date_updated) OVER(PARTITION BY region_code, grp_id2) AS start_datetime,
       MAX(COALESCE(next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY region_code, grp_id2) AS end_datetime
FROM (
SELECT *,
      MAX(grp_id) OVER(PARTITION BY region_code, region_name ORDER BY date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT region_code,
       region_name,
       region_hash_key,
       date_updated,
      (CASE WHEN LAG(region_name) OVER (PARTITION BY region_code ORDER BY date_updated) IS NULL OR 
                 region_name <> LAG(region_name) OVER (PARTITION BY region_code ORDER BY date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY region_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.region_code ORDER BY date_updated) AS row_id,
             a.region_code,
             a.region_name,
             a.region_hash_key,
             a.date_updated      
      FROM ( SELECT DISTINCT TRIM(a.COD_DEST2) AS region_code,
                             TRIM(a.DESC_DEST20) AS region_name,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST20), '')), 256) AS region_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.Dest2 a
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_region b
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(b.region_code)
              WHERE LOWER(b.region_code) IS NULL
              UNION -- We either want to insert all region codes we haven't seen before or we want to insert only region codes with changed attributes
              SELECT DISTINCT TRIM(a.COD_DEST2) AS region_code,
                              TRIM(a.DESC_DEST20) AS region_name,
                              SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST20), '')), 256) AS region_hash_key,
                              CAST(DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.Dest2 a
              INNER JOIN gold_{ENVIRONMENT}.tag02.dim_region b
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(b.region_code)
               AND CAST(DATEUPD AS TIMESTAMP) > b.start_datetime
               AND SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST20), '')), 256) <> b.region_hash_key
               AND b.is_current = 1) a) hk) x) y) z
""")


# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf = spark.sql("""SELECT region_code, NOW() AS Sys_Gold_ModifiedDateTime_UTC, MIN(start_datetime) AS min_start_datetime FROM staging_dim_region GROUP BY region_code""")
# MAGIC
# MAGIC deltaTableAccount = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.dim_region")
# MAGIC
# MAGIC deltaTableAccount.alias('dim_region') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC     'dim_region.is_current = 1 AND dim_region.region_code = updates.region_code'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "end_datetime": "updates.min_start_datetime",
# MAGIC       "is_current": lit(0),
# MAGIC       "dim_region.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT region_code,
# MAGIC        region_name,
# MAGIC        region_hash_key,
# MAGIC        start_datetime,
# MAGIC        end_datetime,
# MAGIC        is_current,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC
# MAGIC FROM staging_dim_region     
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_region")
