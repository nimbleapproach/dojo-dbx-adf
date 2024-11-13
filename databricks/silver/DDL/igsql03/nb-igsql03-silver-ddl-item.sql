-- Databricks notebook source
-- DBTITLE 1,Define Item at Silver
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

CREATE OR REPLACE TABLE item
  ( 
        SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
  ,No_ STRING NOT NULL 
    COMMENT 'Business Key'
  ,Description STRING
    COMMENT 'TODO'
  ,Description2 STRING
    COMMENT 'TODO'
	,Description3 STRING
	  COMMENT 'TODO'
	,Description4 STRING
	  COMMENT 'TODO'
	,GlobalDimension1Code STRING
    COMMENT 'TODO'
	,GlobalDimension2Code STRING
    COMMENT 'TODO'
	,Type INT
    COMMENT 'TODO'
	,ProductType STRING
    COMMENT 'TODO'
  ,PhysicalGoods BIGINT
    COMMENT 'TODO'
  ,ManufacturerItemNo_ STRING
    COMMENT 'The key linking to the item_dds'
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
    COMMENT 'Flag if this is the current version.'
,CONSTRAINT item_pk PRIMARY KEY(No_,Sys_DatabaseName, Sys_RowNumber)
  )
COMMENT 'This table contains the line data for item. \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (No_,Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE item ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE item ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE item ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
