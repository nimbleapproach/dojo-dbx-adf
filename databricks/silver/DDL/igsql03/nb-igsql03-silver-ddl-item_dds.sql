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

CREATE OR REPLACE TABLE item_dds
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,timestamp BIGINT
      COMMENT 'Row Version Number in the source database.'
    ,Source_Entity STRING NOT NULL 
      COMMENT 'Source entity.'
    ,Vendor_DIM_Code STRING NOT NULL
      COMMENT 'TODO'
    ,Manufacturer_Item_No_ STRING NOT NULL
      COMMENT 'TODO'
    ,Item_Category_Code STRING
      COMMENT 'TODO'
    ,PG1_Code STRING
      COMMENT 'TODO'
    ,PG2_Code STRING
      COMMENT 'TODO'
    ,Description STRING
      COMMENT 'TODO'
    ,Description_2 STRING
      COMMENT 'TODO'
    ,Description_3 STRING
      COMMENT 'TODO'
    ,Description_4 STRING
      COMMENT 'TODO'
    ,Item_Disc_Group STRING
      COMMENT 'TODO'
    ,BUSINESSTYPE_Dimension_Code STRING
      COMMENT 'TODO'
    ,Life_Cycle_Formula STRING
      COMMENT 'TODO'
    ,Physical_Goods INT
      COMMENT 'TODO'
    ,Lic_Band_Min_ STRING
      COMMENT 'TODO'
    ,Lic_Band_Max_ STRING
      COMMENT 'TODO'
    ,Technology_Code STRING
      COMMENT 'Code from technology table'
    ,Default_Technology BOOLEAN
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
  ,CONSTRAINT item_dds_pk PRIMARY KEY(Source_Entity, Manufacturer_Item_No_, Vendor_DIM_Code)
  )
COMMENT 'This table is enabling join of item and technol.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (Source_Entity, Manufacturer_Item_No_, Vendor_DIM_Code)

-- COMMAND ----------

ALTER TABLE item_dds ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE item_dds ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE item_dds ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
