-- Databricks notebook source
-- DBTITLE 1,Define Sales Invoice Header at Silver
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

CREATE OR REPLACE TABLE inf_msp_usage_line
  ( 
      SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,BizTalkGuid STRING
      COMMENT 'Business Key'
    ,LineNo_ STRING NOT NULL 
      COMMENT 'Business Key'
    ,ItemNo_ STRING
      COMMENT 'Business Key'
    ,Description STRING
      COMMENT 'Description'
    ,Description2 STRING 
      COMMENT 'Description'
    ,Description3 STRING
      COMMENT 'Description'
    ,Description4 STRING
      COMMENT 'Description'
    ,Quantity Decimal(18,8)
      COMMENT 'TODO'
    ,UnitPrice Decimal(18,8)
      COMMENT 'TODO'
    ,TotalPrice Decimal(18,8)
      COMMENT 'TODO'
    ,EndUserName STRING
      COMMENT 'Description'
    ,SalesInvoiceLineNo_ STRING
      COMMENT 'Description'
    ,VendorNo_ STRING
      COMMENT 'Description'
    ,SalesAccountNo_ STRING
      COMMENT 'GL Account for sales'
    ,COGSAccountNo_ STRING
      COMMENT 'GL Account for cost'
    ,UnitCostPCY Decimal(18,8)
       COMMENT 'TODO'
    ,TotalCostPCY Decimal(18,8)
      COMMENT 'TODO'
    ,PurchaseCurrencyCode STRING
      COMMENT 'Purchase currency'
    ,SalesCreditMemoLineNo_ STRING
      COMMENT 'Description'
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
    ,Sys_Silver_IsCurrent BOOLEAN
,CONSTRAINT inf_msp_usage_line_pk PRIMARY KEY(BizTalkGuid,LineNo_,Sys_DatabaseName, Sys_RowNumber)
  )
COMMENT 'This table contains the header data for MSP. \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
CLUSTER BY (BizTalkGuid,LineNo_,Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE inf_msp_usage_line ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE inf_msp_usage_line ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE inf_msp_usage_line ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
