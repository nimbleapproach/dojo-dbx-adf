-- Databricks notebook source
-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The target catalog depends on the environment. Since we are using Unity Catalog we need to use a unique name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA igsql03;

-- COMMAND ----------

CREATE OR REPLACE TABLE end_customer
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,timestamp BIGINT
      COMMENT 'Row Version Number in the source database.'
    ,No_ STRING NOT NULL 
      COMMENT 'Business Key'
    ,Entity STRING NOT NULL 
      COMMENT 'TODO'
    ,Contact_No_ STRING NOT NULL 
      COMMENT 'TODO'
    ,Name STRING 
      COMMENT 'TODO'
    ,Name_2 STRING 
      COMMENT 'TODO'
    ,PostCode STRING 
      COMMENT 'TODO'
    ,City STRING 
      COMMENT 'TODO'
    ,Country_Region_Code STRING 
      COMMENT 'TODO'
    ,NACE_2_Code STRING 
      COMMENT 'TODO'
    ,Description STRING 
      COMMENT 'TODO'
    ,Max_Quantity_Long_Term_License DECIMAL(38,20) 
      COMMENT 'TODO'
    ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
      COMMENT 'The timestamp when this entry landed in bronze.'
    ,Sys_DatabaseName STRING NOT NULL
      COMMENT 'Name of the Source Database.'
    ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry landed in silver.'
    ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modified in silver.'
    ,Sys_Silver_HashKey BIGINT NOT NULL
      COMMENT 'HashKey over all but Sys columns.'
    ,Sys_Silver_IsCurrent BOOLEAN
      COMMENT 'Flag if this is the current version.'
  ,CONSTRAINT end_customer_pk PRIMARY KEY(No_, Sys_DatabaseName)
  )
COMMENT 'This table contains the data for end customers.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (No_,Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE end_customer ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE end_customer ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE end_customer ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
