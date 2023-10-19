-- Databricks notebook source
-- DBTITLE 1,Define Sales Credit Memo Line at Silver
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

CREATE OR REPLACE TABLE sales_cr_memo_line
  ( 
        SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,DocumentNo_ STRING NOT NULL 
      COMMENT 'Business Key'
    ,LineNo_ STRING NOT NULL 
      COMMENT 'Business Key'
    ,No_ STRING 
      COMMENT 'TODO'
    ,Amount DECIMAL(10,2) 
      COMMENT 'TODO'
    ,AmountIncludingVAT DECIMAL(10,2) 
      COMMENT 'TODO'
    ,CostAmountLCY DECIMAL(10,2) 
      COMMENT 'TODO'
    ,Quantity DECIMAL(10,2) 
      COMMENT 'TODO'
    ,UnitPrice DECIMAL(10,2) 
      COMMENT 'TODO'
    ,UnitCostLCY DECIMAL(10,2) 
      COMMENT 'TODO'
    ,ShortcutDimension1Code STRING 
      COMMENT 'TODO'
    ,OrderNo_ STRING 
      COMMENT 'Sales Order number'
    ,OrderLineNo_ int 
      COMMENT 'Sales Order line number'
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
,CONSTRAINT sales_cr_memo_line_pk PRIMARY KEY(DocumentNo_,LineNo_,Sys_DatabaseName, Sys_RowNumber)
  )
COMMENT 'This table contains the line data for sales credit memo line. \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (DocumentNo_,LineNo_,Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE sales_cr_memo_line ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE sales_cr_memo_line ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE sales_cr_memo_line ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
