# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Databricks notebook source
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
# MAGIC -- MAGIC If there is no widget defined, Data Factory will automatically create them.
# MAGIC -- MAGIC For us while developing we can use the try and excep trick here.
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %python
# MAGIC -- MAGIC import os
# MAGIC -- MAGIC
# MAGIC -- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC The target catalog depens on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %python
# MAGIC -- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC USE SCHEMA tag02;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE OR REPLACE TABLE fact_tagetik_revenue
# MAGIC   ( fact_tagetik_revenue_pk BIGINT
# MAGIC     GENERATED ALWAYS AS IDENTITY
# MAGIC     ,date_sk INTEGER
# MAGIC       COMMENT 'Date Id derived from the date at which the related silver record was processed'
# MAGIC     ,period STRING
# MAGIC        COMMENT 'period of the Tagetik revenue transaction'
# MAGIC     ,exchange_rate_sk BIGINT
# MAGIC        COMMENT 'Currency id related to the Exchange Rate dimension'
# MAGIC     ,account_sk BIGINT
# MAGIC        COMMENT 'Account id related to the Account dimension'
# MAGIC     ,special_deal_code STRING
# MAGIC        COMMENT 'Indicates whether this fact record relates to a special deal or not'
# MAGIC     ,region_sk BIGINT
# MAGIC        COMMENT 'Region id related to the Region dimension'
# MAGIC     ,vendor_sk BIGINT
# MAGIC        COMMENT 'Vendor id related to the Vendor dimension'
# MAGIC     ,cost_centre_sk BIGINT
# MAGIC        COMMENT 'Cost Center id related to the Cost Center dimension'
# MAGIC     ,scenario_sk BIGINT
# MAGIC        COMMENT 'Scenario id related to the Scenario dimension'
# MAGIC     ,entity_sk BIGINT
# MAGIC        COMMENT 'Entity id related to the Entity dimension'
# MAGIC     ,category STRING
# MAGIC        COMMENT 'The Tagetik category e.g. Amount, ADJ01'
# MAGIC     ,revenue_LCY DECIMAL(20,2)
# MAGIC     ,silver_source INTEGER
# MAGIC        COMMENT 'The source of the fact data, 1 for dati_saldi_lordi, 2 for dati_rett_riga'
# MAGIC     ,silver_SID INTEGER
# MAGIC        COMMENT 'The SID from the relevant silver source table'
# MAGIC     ,tagetik_date_updated TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry was last updated in Tagetik'    
# MAGIC     ,Sys_Gold_InsertedDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this record was inserted into gold'
# MAGIC     ,Sys_Gold_ModifiedDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this record was last updated in gold'      
# MAGIC     ,Sys_Gold_is_active INTEGER
# MAGIC       COMMENT 'Whether this record has been soft deleted in silver or been updated by a future record'    
# MAGIC ,CONSTRAINT fact_tagetik_revenue_pk PRIMARY KEY(date_sk, period, exchange_rate_sk, account_sk, region_sk, vendor_sk, cost_centre_sk, scenario_sk, entity_sk)
# MAGIC   )
# MAGIC
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (date_sk);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC ALTER TABLE fact_tagetik_revenue ADD CONSTRAINT dateWithinRange_Gold_InsertDateTime CHECK (Sys_Gold_InsertedDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE fact_tagetik_revenue ADD CONSTRAINT dateWithinRange_Gold_UpdateDateTime CHECK (Sys_Gold_ModifiedDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE fact_tagetik_revenue ADD CONSTRAINT valid_is_active_flag CHECK (Sys_Gold_is_active IN (1,0));
