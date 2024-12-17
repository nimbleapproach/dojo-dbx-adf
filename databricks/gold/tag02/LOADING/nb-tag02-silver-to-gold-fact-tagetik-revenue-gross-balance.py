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

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_fact_tagetik_revenue_dsl_to_load AS
# MAGIC
# MAGIC SELECT CAST(date_format(CAST((CASE WHEN dsl.COD_PERIODO BETWEEN '01' AND '09' THEN DATEADD(YEAR,-1,(DATEADD(MONTH,3,CONCAT(LEFT(dsl.COD_SCENARIO,4),'-', CAST(dsl.COD_PERIODO AS STRING),'-01'))))
# MAGIC                                    WHEN dsl.COD_PERIODO BETWEEN '10' AND '12' THEN DATEADD(MONTH,-9,CONCAT(LEFT(dsl.COD_SCENARIO, 4),'-',CAST(dsl.COD_PERIODO AS STRING),'-01')) END) AS DATE), 'yyyyMMdd') AS INT) AS date_sk,
# MAGIC        dsl.COD_PERIODO AS period,
# MAGIC        COALESCE(er.dim_exchange_rate_pk,-1) AS exchange_rate_sk,
# MAGIC        LOWER(CONCAT(TRIM(dsl.COD_SCENARIO),'_',TRIM(dsl.COD_PERIODO),'_',TRIM(dsl.COD_VALUTA))) AS exchange_rate_code,
# MAGIC        COALESCE(acc.dim_account_pk,-1) AS account_sk,
# MAGIC        LOWER(dsl.COD_CONTO) AS account_code,
# MAGIC        COALESCE(reg.dim_region_pk,-1) AS region_sk,
# MAGIC        LOWER(dsl.COD_DEST2) AS region_code,      
# MAGIC        COD_DEST4 AS special_deal_code,      
# MAGIC        COALESCE(v.dim_vendor_pk,-1) AS vendor_sk, 
# MAGIC        LOWER(dsl.COD_DEST1) AS vendor_code,           
# MAGIC        COALESCE(cc.dim_cost_centre_pk,-1) AS cost_centre_sk,
# MAGIC        LOWER(dsl.COD_DEST3) AS cost_centre_code,
# MAGIC        COALESCE(s.dim_scenario_pk,-1) AS scenario_sk,
# MAGIC        LOWER(dsl.COD_SCENARIO) AS scenario_code,
# MAGIC        COALESCE(e.dim_entity_pk,-1) AS entity_sk,
# MAGIC        LOWER(dsl.COD_AZIENDA) AS entity_code,      
# MAGIC        dsl.COD_CATEGORIA as Category,
# MAGIC        CAST((dsl.IMPORTO * -1) AS DECIMAL(20,2)) AS revenue_LCY,
# MAGIC        CAST(1 AS INTEGER) AS silver_source,       
# MAGIC        CAST(dsl.SID AS INTEGER) AS silver_SID,
# MAGIC        CAST(dsl.Sys_Silver_ModifedDateTime_UTC AS TIMESTAMP) AS source_date_updated,
# MAGIC        NOW() AS Sys_Gold_InsertedDateTime_UTC,
# MAGIC        NOW() AS Sys_Gold_ModifiedDateTime_UTC,
# MAGIC        CAST(1 AS INTEGER) AS Sys_Gold_is_active,
# MAGIC        dsl.Sys_Silver_IsDeleted
# MAGIC FROM (SELECT SID
# MAGIC       FROM silver_${tableObject.environment}.tag02.dati_saldi_lordi a
# MAGIC       LEFT OUTER JOIN (SELECT silver_sid FROM gold_${tableObject.environment}.tag02.fact_tagetik_revenue WHERE silver_source = 1) b
# MAGIC         ON a.SID = b.silver_sid
# MAGIC       WHERE b.silver_sid IS NULL AND a.Sys_Silver_IsCurrent
# MAGIC       UNION -- we either want to load the transactional record where it hasn't been seen before or we want to load a more recent version of one that has and is now inactive
# MAGIC       SELECT SID
# MAGIC       FROM silver_${tableObject.environment}.tag02.dati_saldi_lordi a2
# MAGIC       INNER JOIN (SELECT silver_sid, source_date_updated FROM gold_${tableObject.environment}.tag02.fact_tagetik_revenue WHERE silver_source = 1 AND Sys_Gold_is_active = 1) b2
# MAGIC         ON a2.SID = b2.silver_sid
# MAGIC       WHERE a2.Sys_Silver_IsDeleted OR NOT a2.Sys_Silver_IsCurrent) sid
# MAGIC INNER JOIN silver_${tableObject.environment}.tag02.dati_saldi_lordi dsl
# MAGIC   ON sid.SID = dsl.SID
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_exchange_rate er
# MAGIC   ON LOWER(CONCAT(TRIM(dsl.COD_SCENARIO),'_',TRIM(dsl.COD_PERIODO),'_',TRIM(dsl.COD_VALUTA))) = LOWER(er.exchange_rate_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN er.start_datetime AND COALESCE(er.end_datetime,'9999-12-31')
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_account acc
# MAGIC   ON LOWER(dsl.COD_CONTO) = LOWER(acc.account_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN acc.start_datetime AND COALESCE(acc.end_datetime,'9999-12-31')
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_region reg
# MAGIC   ON LOWER(dsl.COD_DEST2) = LOWER(reg.region_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN reg.start_datetime AND COALESCE(reg.end_datetime,'9999-12-31')
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_vendor v
# MAGIC   ON LOWER(dsl.COD_DEST1) = LOWER(v.vendor_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN v.start_datetime AND COALESCE(v.end_datetime,'9999-12-31')
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_cost_centre cc
# MAGIC   ON LOWER(dsl.COD_DEST3) = LOWER(cc.cost_centre_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN cc.start_datetime AND COALESCE(cc.end_datetime,'9999-12-31')
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_scenario s
# MAGIC   ON LOWER(dsl.COD_SCENARIO) = LOWER(s.scenario_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN s.start_datetime AND COALESCE(s.end_datetime,'9999-12-31')
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_entity e
# MAGIC   ON LOWER(dsl.COD_AZIENDA) = LOWER(e.entity_code)
# MAGIC  AND CAST(dsl.DATE_UPD AS TIMESTAMP) BETWEEN e.start_datetime AND COALESCE(e.end_datetime,'9999-12-31')
# MAGIC WHERE (COD_SCENARIO NOT LIKE '%OB%')
# MAGIC   AND (LEFT(dsl.COD_CONTO, 1) IN ('1', '2', '3', '4', '5', '6'))
# MAGIC   AND ( dsl.COD_CATEGORIA LIKE "%AMOUNT"
# MAGIC       OR dsl.COD_CATEGORIA IN ( 'ADJ01',
# MAGIC                             'ADJ02',
# MAGIC                             'ADJ03',
# MAGIC                             'CF_GROUP',
# MAGIC                             'CF_LOCAL',
# MAGIC                             'CF_TDA',
# MAGIC                             'INP_HQ03',
# MAGIC                             'INP_HQ04',
# MAGIC                             'INP_HQ05',
# MAGIC                             'INP_HQ06',
# MAGIC                             'INP_HQ07',
# MAGIC                             'INP_MSP2018',
# MAGIC                             'SYN2',
# MAGIC                             'GP_ALLOC')
# MAGIC       OR dsl.COD_CATEGORIA LIKE 'IFCSYN%'
# MAGIC
# MAGIC        )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_fact_tagetik_revenue_dsl_to_load_altered_keys AS
# MAGIC
# MAGIC SELECT x.date_sk,
# MAGIC        x.Period,
# MAGIC       (CASE WHEN x.exchange_rate_sk = -1 AND adj_exchange_rate_key.exchange_rate_code IS NOT NULL THEN adj_exchange_rate_key.dim_exchange_rate_pk ELSE x.exchange_rate_sk END) AS exchange_rate_sk, 
# MAGIC       (CASE WHEN x.account_sk = -1 AND adj_account_key.account_code IS NOT NULL THEN adj_account_key.dim_account_pk ELSE x.account_sk END) AS account_sk, 
# MAGIC       (CASE WHEN x.region_sk = -1 AND adj_region_key.region_code IS NOT NULL THEN adj_region_key.dim_region_pk ELSE x.region_sk END) AS region_sk,      
# MAGIC        x.special_deal_code,  
# MAGIC       (CASE WHEN x.vendor_sk = -1 AND adj_vendor_key.vendor_code IS NOT NULL THEN adj_vendor_key.dim_vendor_pk ELSE x.vendor_sk END) AS vendor_sk,  
# MAGIC       (CASE WHEN x.cost_centre_sk = -1 AND adj_cost_centre_key.cost_centre_code IS NOT NULL THEN adj_cost_centre_key.dim_cost_centre_pk ELSE x.cost_centre_sk END) AS cost_centre_sk,
# MAGIC       (CASE WHEN x.scenario_sk = -1 AND adj_scenario_key.scenario_code IS NOT NULL THEN adj_scenario_key.dim_scenario_pk ELSE x.scenario_sk 
# MAGIC        END) AS scenario_sk,
# MAGIC       (CASE WHEN x.entity_sk = -1 AND adj_entity_key.entity_code IS NOT NULL THEN adj_entity_key.dim_entity_pk ELSE x.entity_sk 
# MAGIC        END) AS entity_sk,
# MAGIC        x.Category,
# MAGIC        x.revenue_LCY,
# MAGIC        x.silver_SID,
# MAGIC        x.source_date_updated,
# MAGIC        x.Sys_Gold_InsertedDateTime_UTC,
# MAGIC        x.Sys_Gold_ModifiedDateTime_UTC,      
# MAGIC        x.Sys_Silver_IsDeleted,
# MAGIC        x.silver_source,
# MAGIC        x.Sys_Gold_is_active     
# MAGIC FROM vw_fact_tagetik_revenue_dsl_to_load x
# MAGIC LEFT OUTER JOIN ( SELECT dim_exchange_rate_pk, exchange_rate_code, start_datetime
# MAGIC                   FROM ( SELECT dim_exchange_rate_pk, exchange_rate_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY exchange_rate_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_exchange_rate ) s2
# MAGIC                   WHERE row_id = 1 ) adj_exchange_rate_key
# MAGIC   ON x.exchange_rate_code = LOWER(adj_exchange_rate_key.exchange_rate_code)
# MAGIC LEFT OUTER JOIN ( SELECT dim_account_pk, account_code, start_datetime
# MAGIC                   FROM ( SELECT dim_account_pk, account_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY account_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_account ) s2
# MAGIC                   WHERE row_id = 1 ) adj_account_key
# MAGIC   ON x.account_code = LOWER(adj_account_key.account_code)
# MAGIC LEFT OUTER JOIN ( SELECT dim_region_pk, region_code, start_datetime
# MAGIC                   FROM ( SELECT dim_region_pk, region_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY region_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_region ) s2
# MAGIC                   WHERE row_id = 1 ) adj_region_key
# MAGIC   ON x.region_code = LOWER(adj_region_key.region_code)
# MAGIC LEFT OUTER JOIN ( SELECT dim_vendor_pk, vendor_code, start_datetime
# MAGIC                   FROM ( SELECT dim_vendor_pk, vendor_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY vendor_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_vendor ) s2
# MAGIC                   WHERE row_id = 1 ) adj_vendor_key
# MAGIC   ON x.vendor_code = LOWER(adj_vendor_key.vendor_code)
# MAGIC LEFT OUTER JOIN ( SELECT dim_cost_centre_pk, cost_centre_code, start_datetime
# MAGIC                   FROM ( SELECT dim_cost_centre_pk, cost_centre_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY cost_centre_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_cost_centre ) s2
# MAGIC                   WHERE row_id = 1 ) adj_cost_centre_key
# MAGIC   ON x.cost_centre_code = LOWER(adj_cost_centre_key.cost_centre_code)
# MAGIC LEFT OUTER JOIN ( SELECT dim_scenario_pk, scenario_code, start_datetime
# MAGIC                   FROM ( SELECT dim_scenario_pk, scenario_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY scenario_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_scenario ) s2
# MAGIC                   WHERE row_id = 1 ) adj_scenario_key
# MAGIC   ON x.scenario_code = LOWER(adj_scenario_key.scenario_code)
# MAGIC LEFT OUTER JOIN ( SELECT dim_entity_pk, entity_code, start_datetime
# MAGIC                   FROM ( SELECT dim_entity_pk, entity_code, start_datetime, ROW_NUMBER() OVER (PARTITION BY entity_code ORDER BY 
# MAGIC                                 start_datetime) AS row_id FROM gold_${tableObject.environment}.tag02.dim_entity ) s2
# MAGIC                   WHERE row_id = 1 ) adj_entity_key
# MAGIC   ON x.entity_code = LOWER(adj_entity_key.entity_code)
# MAGIC   

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC # Convert the SQL query result into a DataFrame
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT date_sk,
# MAGIC        period,
# MAGIC        exchange_rate_sk,
# MAGIC        account_sk,
# MAGIC        region_sk,
# MAGIC        special_deal_code,
# MAGIC        vendor_sk,            
# MAGIC        cost_centre_sk,
# MAGIC        scenario_sk,
# MAGIC        entity_sk,       
# MAGIC        Category,
# MAGIC        revenue_LCY,
# MAGIC        silver_source,
# MAGIC        silver_SID,
# MAGIC        source_date_updated,
# MAGIC        Sys_Gold_InsertedDateTime_UTC,
# MAGIC        Sys_Gold_ModifiedDateTime_UTC,
# MAGIC        Sys_Gold_is_active
# MAGIC FROM vw_fact_tagetik_revenue_dsl_to_load_altered_keys
# MAGIC """)
# MAGIC
# MAGIC deltaTableFactRevenue = DeltaTable.forName(spark, f"gold_{ENVIRONMENT}.tag02.fact_tagetik_revenue")
# MAGIC
# MAGIC deltaTableFactRevenue.alias('fact_revenue') \
# MAGIC   .merge(
# MAGIC     sqldf.alias('updates'),
# MAGIC           'fact_revenue.period = updates.period AND fact_revenue.exchange_rate_sk = updates.exchange_rate_sk AND fact_revenue.account_sk = updates.account_sk AND fact_revenue.region_sk = updates.region_sk AND fact_revenue.vendor_sk = updates.vendor_sk AND fact_revenue.cost_centre_sk = updates.cost_centre_sk AND fact_revenue.scenario_sk = updates.scenario_sk AND fact_revenue.entity_sk = updates.entity_sk AND fact_revenue.Category = updates.Category AND fact_revenue.silver_source = updates.silver_source AND fact_revenue.silver_SID = updates.silver_SID'
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC       "fact_revenue.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
# MAGIC       "fact_revenue.Sys_Gold_is_active": lit(0)
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()
# MAGIC

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf2 = sqldf.where((col("Sys_Silver_IsDeleted") == lit(0)) | (col("Sys_Silver_IsDeleted").isNull()) | (col("Sys_Silver_IsDeleted") == 'false'))
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf2.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.fact_tagetik_revenue")
# MAGIC
