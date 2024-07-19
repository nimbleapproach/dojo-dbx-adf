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

# MAGIC %py
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC           
# MAGIC CREATE OR REPLACE VIEW region_with_country_code AS
# MAGIC
# MAGIC WITH level_1 as(
# MAGIC   SELECT
# MAGIC     COD_DEST2_ELEGER as Level_1_Code
# MAGIC   FROM
# MAGIC     silver_{ENVIRONMENT}.tag02.dest2_gerarchia
# MAGIC   where
# MAGIC     COD_DEST2_ELEGER_PADRE = 'LS01'
# MAGIC     AND COD_DEST2_GERARCHIA = '03'
# MAGIC     and Sys_Silver_IsCurrent = 1
# MAGIC ),
# MAGIC level_2 as (
# MAGIC   SELECT
# MAGIC     COD_DEST2_ELEGER as Level_2_Code,
# MAGIC     COD_DEST2_ELEGER_PADRE as Level_2_join
# MAGIC   FROM
# MAGIC     silver_{ENVIRONMENT}.tag02.dest2_gerarchia
# MAGIC   where
# MAGIC     Sys_Silver_IsCurrent = 1
# MAGIC ),
# MAGIC level_3 as (
# MAGIC   SELECT
# MAGIC     distinct COD_DEST2 Level_3_Code,
# MAGIC     COD_DEST2_ELEGER Level_3_join
# MAGIC   FROM
# MAGIC     silver_{ENVIRONMENT}.tag02.dest2_gerarchia_abbi
# MAGIC   where
# MAGIC     Sys_Silver_IsCurrent = 1
# MAGIC     and COD_DEST2_GERARCHIA = '03'
# MAGIC ),
# MAGIC region as (
# MAGIC   select
# MAGIC     distinct level12.Level_1_Code,
# MAGIC     level12.Level_2_Code,
# MAGIC     Level_3_Code,
# MAGIC     Level_3_join
# MAGIC   from
# MAGIC     level_3
# MAGIC     left join (
# MAGIC       select
# MAGIC         distinct level_1.*,
# MAGIC         case
# MAGIC           when Level_2_Code is null then level_1.Level_1_Code
# MAGIC           else Level_2_Code
# MAGIC         end as Level_2_Code
# MAGIC       from
# MAGIC         level_1
# MAGIC         left join level_2 on level_1.Level_1_Code = level_2.Level_2_join
# MAGIC     ) level12 on level_3.Level_3_join = level12.Level_2_Code
# MAGIC )
# MAGIC select
# MAGIC   case
# MAGIC     when Level_1_Code is null then Level_3_join
# MAGIC     else Level_1_Code
# MAGIC   end as RegionCode,
# MAGIC   Level_2_Code as CountryCode,
# MAGIC   Level_3_Code as RegionID
# MAGIC from
# MAGIC   region
# MAGIC WHERE Level_2_Code IS NOT NULL
# MAGIC
# MAGIC """)

# COMMAND ----------

spark.sql(f"""
          
CREATE OR REPLACE VIEW staging_dim_region AS

SELECT DISTINCT z.region_code,
                z.region_name,
                z.country_code,
                z.country,
                z.country_detail,
                z.country_visuals,
                z.region_group,                  
                z.region_hash_key,
                z.start_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = z.end_datetime THEN NULL ELSE z.end_datetime END) AS end_datetime,
               (CASE WHEN CAST('9999-12-31' AS TIMESTAMP) = z.end_datetime THEN 1 ELSE 0 END) AS is_current,
                NOW() AS Sys_Gold_InsertedDateTime_UTC,
                NOW() AS Sys_Gold_ModifiedDateTime_UTC               
FROM (
SELECT y.grp_id2 AS region_id,
       y.region_code,
       y.region_name,
       y.country_code,
       y.country,
       y.country_detail,
       y.country_visuals,
       y.region_group,        
       y.region_hash_key,
       MIN(y.date_updated) OVER(PARTITION BY y.region_code, y.grp_id2) AS start_datetime,
       MAX(COALESCE(y.next_date_updated,CAST('9999-12-31' AS TIMESTAMP))) OVER(PARTITION BY y.region_code, y.grp_id2) AS end_datetime
FROM (
SELECT x.*,
      MAX(x.grp_id) OVER(PARTITION BY x.region_code, x.region_name ORDER BY x.date_updated ROWS UNBOUNDED PRECEDING) as grp_id2
FROM (
SELECT hk.region_code,
       hk.region_name,
       hk.country_code,
       hk.country,
       hk.country_detail,
       hk.country_visuals,
       hk.region_group,       
       hk.region_hash_key,
       hk.date_updated,
      (CASE WHEN LAG(hk.region_name) OVER (PARTITION BY hk.region_code ORDER BY hk.date_updated) IS NULL OR 
                 hk.region_name <> LAG(hk.region_name) OVER (PARTITION BY hk.region_code ORDER BY hk.date_updated) THEN row_id
                 ELSE NULL END) AS grp_id,
       LEAD(date_updated) OVER (PARTITION BY region_code ORDER BY date_updated) AS next_date_updated
FROM (SELECT row_number() OVER(PARTITION BY a.region_code ORDER BY date_updated) AS row_id,
             a.region_code,
             a.region_name,
             a.country_code,
             a.country,
             a.country_detail,
             a.country_visuals,
             a.region_group,
             a.region_hash_key,
             a.date_updated      
      FROM ( SELECT DISTINCT TRIM(a.COD_DEST2) AS region_code,
                             TRIM(a.DESC_DEST20) AS region_name,
                             c.CountryCode AS country_code,
                             d.country,
                             d.country_detail,
                             d.country_visuals,
                             d.region_group,
                             SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST20), ''), COALESCE(TRIM(c.CountryCode), ''), COALESCE(TRIM(d.country), ''), COALESCE(TRIM(d.country_detail), ''), COALESCE(TRIM(d.country_visuals), ''), COALESCE(TRIM(d.region_group), '')), 256) AS region_hash_key,
                             CAST(a.DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.Dest2 a
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_region b
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(b.region_code)
              LEFT OUTER JOIN region_with_country_code c
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(c.RegionID) 
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.region_group_country_mapping d
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(d.region_code)                
              WHERE LOWER(b.region_code) IS NULL
              UNION -- We either want to insert all region codes we haven't seen before or we want to insert only region codes with changed attributes
              SELECT DISTINCT TRIM(a.COD_DEST2) AS region_code,
                              TRIM(a.DESC_DEST20) AS region_name,
                              c.CountryCode AS country_code,
                              d.country,
                              d.country_detail,
                              d.country_visuals,
                              d.region_group,                            
                              SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST20), ''), COALESCE(TRIM(c.CountryCode), ''), COALESCE(TRIM(d.country), ''), COALESCE(TRIM(d.country_detail), ''), COALESCE(TRIM(d.country_visuals), ''), COALESCE(TRIM(d.region_group), '')), 256) AS region_hash_key,
                              CAST(DATEUPD AS TIMESTAMP) AS date_updated
              FROM silver_{ENVIRONMENT}.tag02.Dest2 a
              LEFT OUTER JOIN region_with_country_code c
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(c.RegionID) 
              LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.region_group_country_mapping d
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(d.region_code)
              INNER JOIN gold_{ENVIRONMENT}.tag02.dim_region b
                ON LOWER(TRIM(a.COD_DEST2)) = LOWER(b.region_code)
               AND CAST(DATEUPD AS TIMESTAMP) > b.start_datetime
               AND SHA2(CONCAT_WS(' ', COALESCE(TRIM(a.DESC_DEST20), ''), COALESCE(TRIM(c.CountryCode), ''), COALESCE(TRIM(d.country), ''), COALESCE(TRIM(d.country_detail), ''), COALESCE(TRIM(d.country_visuals), ''), COALESCE(TRIM(d.region_group), '')), 256) <> b.region_hash_key
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
# MAGIC        country_code,
# MAGIC        country,
# MAGIC        country_detail,
# MAGIC        country_visuals,
# MAGIC        region_group,    
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
