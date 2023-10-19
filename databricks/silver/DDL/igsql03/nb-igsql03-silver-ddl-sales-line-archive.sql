-- Databricks notebook source
-- DBTITLE 1,Define Sales Line Archive at Silver
-- MAGIC %md
-- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
-- MAGIC If there is no widget defined, Data Factory will automatically create them.
-- MAGIC For us while developing we can use the try and except trick here.

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

USE SCHEMA igsql03;

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_line_archive
  ( 
        SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,DocumentNo_ STRING NOT NULL 
      COMMENT 'Business Key'
    ,DocumentType INT NOT NULL
     COMMENT 'identifier of sales quote(0) and sales order (1)'
    ,VersionNo_ INT
      COMMENT 'The version of archive for each document'
    ,LineNo_ INT
      COMMENT 'Doc line'
    ,Doc_No_Occurrence INT
      COMMENT 'TODO'
    ,No_ String
      COMMENT 'Line item number'
    ,Type INT
      COMMENT 'item type'
   ,Quantity DECIMAL
      COMMENT 'Line quantity'
    ,Amount DECIMAL
      COMMENT 'Line Amount'
    ,AmountIncludingVAT DECIMAL
      COMMENT 'Line amount including VAT'
    ,CostAmountLCY DECIMAL
      COMMENT 'Line Cost'
    ,Alternative INT
      COMMENT 'flag whether a sales quote is alternative or not'
    ,ShortcutDimension1Code STRING
      COMMENT 'vendor code'
    ,ShortcutDimension2Code STRING
      COMMENT 'business type code'
    ,Sys_RowNumber BIGINT NOT NULL
      COMMENT 'Globally unqiue Number in the source database to capture changes. Was calculated by casting the "timestamp" column to integer.'
    ,Sys_DatabaseName STRING NOT NULL
      COMMENT 'Name of the Source Database.'
    ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
      COMMENT 'The timestamp when this entry landed in bronze.'
    ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry landed in silver.'
    ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modifed in silver.'
    ,Sys_Silver_HashKey BIGINT NOT NULL
      COMMENT 'HashKey over all but Sys columns.'
,CONSTRAINT sales_line_archivee_pk PRIMARY KEY(DocumentNo_,VersionNo_,LineNo_,Sys_DatabaseName, Sys_RowNumber)
  )
COMMENT 'This table contains the header data for sales line archive. \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
CLUSTER BY (DocumentNo_,VersionNo_,LineNo_,Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE sales_line_archive ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE sales_line_archive ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE sales_line_archive ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
