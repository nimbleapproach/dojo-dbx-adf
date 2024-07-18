# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

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
# MAGIC CREATE OR REPLACE TABLE scenario
# MAGIC   ( SID bigint
# MAGIC     GENERATED ALWAYS AS IDENTITY
# MAGIC     ,COD_SCENARIO STRING NOT NULL
# MAGIC       COMMENT 'Scenario code'
# MAGIC     ,TIPO_SCENARIO STRING NOT NULL
# MAGIC       COMMENT 'Scenario type'    
# MAGIC     ,COD_SCENARIO_ORIGINARIO STRING
# MAGIC       COMMENT 'Scenario original code'    
# MAGIC     ,DESC_SCENARIO STRING
# MAGIC       COMMENT 'Scenario description'
# MAGIC     ,COD_SCENARIO_PREC STRING
# MAGIC     ,COD_SCENARIO_SUCC STRING
# MAGIC     ,COD_SCENARIO_RIF1 STRING
# MAGIC     ,COD_SCENARIO_RIF2 STRING
# MAGIC     ,COD_SCENARIO_RIF3 STRING
# MAGIC     ,COD_SCENARIO_RIF4 STRING
# MAGIC     ,COD_SCENARIO_RIF5 STRING
# MAGIC     ,COD_AZI_CAPOGRUPPO STRING
# MAGIC     ,COD_VALUTA STRING
# MAGIC       COMMENT 'Currency code'    
# MAGIC     ,COD_CATEGORIA_GERARCHIA STRING
# MAGIC     ,COD_CATEGORIA_ELEGER STRING
# MAGIC     ,COD_ESERCIZIO STRING NOT NULL
# MAGIC     ,DESC_VERSIONE STRING NOT NULL
# MAGIC       COMMENT 'Version description'
# MAGIC     ,DATEUPD TIMESTAMP
# MAGIC       COMMENT 'Last Update Timestamp'
# MAGIC     ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC     ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC     ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC     ,Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys and DATEUPD columns.'
# MAGIC     ,Sys_Silver_IsCurrent    BOOLEAN
# MAGIC     ,Sys_Silver_IsDeleted  BOOLEAN
# MAGIC ,CONSTRAINT scenario_pk PRIMARY KEY(COD_SCENARIO, DATEUPD)
# MAGIC   )
# MAGIC
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (COD_SCENARIO);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC ALTER TABLE scenario ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE scenario ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE scenario ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
