-- Databricks notebook source
-- MAGIC %md
-- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
-- MAGIC If there is no widget defined, Data Factory will automatically create them.
-- MAGIC For us while developing we can use the try and excep trick here.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The target catalog depens on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA tag02;

-- COMMAND ----------

CREATE OR REPLACE TABLE dest2_gerarchia_abbi
  ( SID bigint
    GENERATED ALWAYS AS IDENTITY
    ,COD_DEST2 STRING NOT NULL 
      COMMENT 'Entity Code'
    ,COD_DEST2_ELEGER STRING
    ,COD_DEST2_GERARCHIA STRING
    ,ORDINAMENTO int
    ,PROVENIENZA STRING
    ,DATEUPD TIMESTAMP
      COMMENT 'Last Update Timestamp'
    ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
      COMMENT 'The timestamp when this entry landed in bronze.'
    ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry landed in silver.'
    ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modifed in silver.'
    ,Sys_Silver_HashKey BIGINT NOT NULL
      COMMENT 'HashKey over all but Sys and DATEUPD columns.'
      ,Sys_Silver_IsCurrent BOOLEAN
      ,Sys_Silver_IsDeleted BOOLEAN
,CONSTRAINT dest2_gerarchia_abbi_pk PRIMARY KEY(COD_DEST2,COD_DEST2_ELEGER,COD_DEST2_GERARCHIA,ORDINAMENTO, DATEUPD)
  )

TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (COD_DEST2,COD_DEST2_ELEGER,COD_DEST2_GERARCHIA,ORDINAMENTO)

-- COMMAND ----------

ALTER TABLE dest2_gerarchia_abbi ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE dest2_gerarchia_abbi ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE dest2_gerarchia_abbi ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
