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

USE SCHEMA deltalink;

-- COMMAND ----------

CREATE OR REPLACE TABLE invoicedata
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,InvoiceNumber STRING
        COMMENT 'TODO'  
    ,LineNo   int
      COMMENT 'TODO'
    ,InvoiceDate STRING
        COMMENT 'TODO'   
    ,OrderType STRING
        COMMENT 'TODO'   
    ,OrderNumber int
        COMMENT 'TODO'   
    ,ClientNumber int
        COMMENT 'TODO'   
    ,Company STRING
        COMMENT 'TODO'   
    ,Artcode STRING
        COMMENT 'TODO'   
    ,SupplierID STRING
        COMMENT 'TODO'   
    ,Supplier STRING
        COMMENT 'TODO'   
    ,ArtSupCode STRING
        COMMENT 'TODO'   
    ,Description STRING
        COMMENT 'TODO'   
    ,Quantity FLOAT
        COMMENT 'TODO'   
    ,ArticleProductType STRING
        COMMENT 'TODO'   
    ,Currency STRING
        COMMENT 'TODO'   
    ,RevenueTransaction DECIMAL(10,2)
        COMMENT 'TODO'   
    ,CostTransaction DECIMAL(10,2)
        COMMENT 'TODO'   
    ,MarginTransaction DECIMAL(10,2)
        COMMENT 'TODO'
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
,CONSTRAINT deltalink_invoicedata_pk PRIMARY KEY(InvoiceNumber,LineNo,Sys_Bronze_InsertDateTime_UTC)
  )
COMMENT 'This table contains the line data for gl_entry. \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (InvoiceNumber,LineNo)

-- COMMAND ----------

ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
