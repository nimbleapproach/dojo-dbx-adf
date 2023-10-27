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

CREATE OR REPLACE TABLE dati_saldi_lordi
  ( SID bigint
    GENERATED ALWAYS AS IDENTITY
    ,OID_DATI_SALDI_LORDI STRING
      COMMENT 'todo'
    ,COD_SCENARIO     STRING
      COMMENT 'todo'
    ,COD_PERIODO    STRING
      COMMENT 'todo'
    ,COD_AZIENDA    STRING
      COMMENT 'todo'
    ,COD_CONTO    STRING
      COMMENT 'todo'
    ,COD_DEST1    STRING
      COMMENT 'todo'
    ,COD_DEST2    STRING
      COMMENT 'todo'
    ,COD_DEST3    STRING
      COMMENT 'todo'
    ,COD_DEST4    STRING
      COMMENT 'todo'
    ,COD_CATEGORIA    STRING
      COMMENT 'todo'
    ,IMPORTO    DECIMAL(20,2)
      COMMENT 'todo'
    ,COD_VALUTA   STRING
      COMMENT 'todo'
    ,PROVENIENZA    STRING
      COMMENT 'todo'

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
,CONSTRAINT dati_saldi_lordi_pk PRIMARY KEY(COD_PERIODO,COD_SCENARIO,COD_AZIENDA,COD_CONTO,COD_DEST1,COD_DEST2,COD_DEST3,COD_DEST4,PROVENIENZA,COD_CATEGORIA ,DATEUPD)
  )
COMMENT 'This table contains the "fact information".'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (COD_PERIODO,COD_SCENARIO,COD_AZIENDA)

-- COMMAND ----------

ALTER TABLE dati_saldi_lordi ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE dati_saldi_lordi ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE dati_saldi_lordi ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
