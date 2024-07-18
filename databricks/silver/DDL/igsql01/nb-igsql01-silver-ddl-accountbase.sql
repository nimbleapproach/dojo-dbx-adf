-- Databricks notebook source
-- DBTITLE 1,Define Customer at Silver
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
-- MAGIC The target catalog depends on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS igsql01

-- COMMAND ----------

USE SCHEMA igsql01;

-- COMMAND ----------

CREATE OR REPLACE TABLE accountbase (
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,AccountId STRING NOT NULL
      COMMENT 'Business Key'
    ,OwningBusinessUnit STRING 
      COMMENT 'TODO'
    ,Name STRING 
      COMMENT 'Name of Customer'
    ,AccountNumber STRING 
      COMMENT 'Business Customer ID'
    ,Telephone1 STRING 
      COMMENT 'TODO'
    ,Fax STRING 
      COMMENT 'TODO'
    ,CreatedOn TIMESTAMP 
      COMMENT 'TODO'
    ,ModifiedOn TIMESTAMP NOT NULL
      COMMENT 'Watermark'
    ,ParentAccountId STRING
      COMMENT 'TODO'
    ,StatusCode INT 
      COMMENT 'TODO'
    ,inf_businessrel_VENDOR BOOLEAN 
      COMMENT 'TODO'
    ,Inf_CountryId STRING 
      COMMENT 'TODO'
    ,inf_businessrel_NPP BOOLEAN 
      COMMENT 'TODO'
    ,inf_businessrel_PP BOOLEAN 
      COMMENT 'TODO'
    ,inf_businessrel_COMP BOOLEAN 
      COMMENT 'TODO'
    ,inf_businessrel_NPP_COR BOOLEAN 
      COMMENT 'TODO'
    ,Inf_LastModifiedOn TIMESTAMP 
      COMMENT 'TODO'
    ,inf_CurrencyCode STRING 
      COMMENT 'TODO'
    ,inf_customerno STRING 
      COMMENT 'Customer ID'
    ,inf_Name2 STRING 
      COMMENT 'Alternative Name'
    ,inf_businessrelationinfo STRING 
      COMMENT 'TODO'
    ,inf_businessrel_CUSTOMER BOOLEAN 
      COMMENT 'TODO'
    ,inf_businessrel_SUPPLIER BOOLEAN 
      COMMENT 'TODO'
    ,inf_invoicedispatchtype INT 
      COMMENT 'TODO'
    ,inf_businessrel_endcust BOOLEAN 
      COMMENT 'TODO'
    ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP NOT NULL
      COMMENT 'The timestamp when this entry landed in bronze.'
    ,Sys_DatabaseName STRING NOT NULL
      COMMENT 'Name of the Source Database.'
    ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry landed in silver.'
    ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modifed in silver.'
    ,Sys_Silver_HashKey BIGINT NOT NULL
      COMMENT 'HashKey over all but Sys columns.'
    ,Sys_Silver_IsCurrent BOOLEAN
      COMMENT 'Flag if this is the current version.'
  ,CONSTRAINT accountbase_pk PRIMARY KEY(AccountId, Sys_DatabaseName, ModifiedOn)
)
COMMENT 'This table contains the data for customer account.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (AccountId, Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE accountbase ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE accountbase ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE accountbase ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
