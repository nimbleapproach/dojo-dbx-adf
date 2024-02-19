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
-- MAGIC spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

-- COMMAND ----------

CREATE SCHEMA if not EXISTS orion

-- COMMAND ----------

USE SCHEMA orion

-- COMMAND ----------

CREATE
OR REPLACE TABLE dim_products (
  ProductID BIGINT COMMENT 'Technical ID build by using SKU and key_products table.',
  SKU STRING COMMENT 'SKU of the item sold. The Stock Keeping Unit code for the transaction row which uniquely defines the product that is the subject of the row',
  Description STRING COMMENT 'Description of the item sold.',
  ProductType STRING COMMENT 'Type of the Item. Originates from our ERP systems',
  CommitmentDuration1 STRING COMMENT 'TO DO',
  CommitmentDuration2 STRING COMMENT 'TO DO',
  BillingFrequency STRING COMMENT 'TO DO',
  ConsumptionModel STRING COMMENT 'TO DO',
  Sys_Gold_InsertDateTime_UTC TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when this entry landed in gold.',
  Sys_Gold_ModifedDateTime_UTC TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when this entry was last modifed in gold.',
  CONSTRAINT orion_dim_products_pk PRIMARY KEY(ProductID)
) COMMENT 'This table contains the product data data for the management reports. \n' TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
) CLUSTER BY (ProductType, SKU)

-- COMMAND ----------

ALTER TABLE dim_products ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Gold_InsertDateTime_UTC >= '1900-01-01');
ALTER TABLE dim_products ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Gold_ModifedDateTime_UTC >= '1900-01-01');
