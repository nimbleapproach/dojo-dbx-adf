-- Databricks notebook source
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

CREATE OR REPLACE TABLE value_entry
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,Sys_RowNumber BIGINT NOT NULL
      COMMENT 'Globally unqiue Number in the source database to capture changes. Was calculated by casting the "timestamp" column to integer.'
    ,EntryNo_ int NOT NULL 
      COMMENT 'Business Key'
    ,ItemLedgerEntryType int
    ,ItemNo_ STRING 
    ,PostingDate TIMESTAMP
    ,DocumentNo_ STRING
    ,DocumentType int
    ,DocumentLineNo_ int
    ,Description STRING
    ,InventoryPostingGroup STRING
    ,ItemLedgerEntryNo_  int
    ,InvoicedQuantity DECIMAL(18,2)
    ,CostperUnit DECIMAL(18,2)
    ,CostAmountActual DECIMAL(18,2)
    ,CostPostedtoG_L DECIMAL(18,2) 
    ,OrderNo_ STRING
    ,OrderLineNo_ int
    ,EntryType int
    ,Adjustment int
    ,DimensionSetID int
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
    ,Sys_Silver_IsCurrent BOOLEAN
,CONSTRAINT value_entry_pk PRIMARY KEY(EntryNo_,Sys_DatabaseName, Sys_RowNumber)
  )
COMMENT 'This table contains the line data for value_entry. \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (EntryNo_,Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE value_entry ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE value_entry ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE value_entry ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
