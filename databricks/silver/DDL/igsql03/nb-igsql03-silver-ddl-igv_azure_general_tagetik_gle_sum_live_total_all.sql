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

CREATE OR REPLACE TABLE igv_azure_general_tagetik_gle_sum_live_total_all
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,Scenario  STRING
        comment 'TODO'   
    ,CompanyID STRING
        comment 'TODO'   
    ,CompanyName STRING
        comment 'TODO'   
    ,FunctionalCur STRING
        comment 'TODO'   
    ,FiscalYear INT
        comment 'TODO'   
    ,FiscalMonth INT
        comment 'TODO'   
    ,IncomeBalanceDesc STRING
        comment 'TODO'   
    ,ConsolCreditAcc STRING
        comment 'TODO'   
    ,ICPartnerID STRING
        comment 'TODO'   
    ,ICPartnerDesc STRING
        comment 'TODO'   
    ,TransactionalCur STRING
        comment 'TODO'   
    ,CalendarYear INT
        comment 'TODO'   
    ,CalendarMonth INT
        comment 'TODO'   
    ,Category STRING
        comment 'TODO'   
    ,FunctionalAmount decimal(29,10)
        comment 'TODO'   
    ,TransactionalAmount decimal(29,10)
        comment 'TODO'   
    ,CostCenter STRING
        comment 'TODO'   
    ,Vendor STRING
        comment 'TODO'   
    ,ReportingRegion STRING
        comment 'TODO'   
    ,SpecialDeal STRING
        comment 'TODO'   
    ,ProjectDimension STRING
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

  )
COMMENT 'This table contains the data for general ledger entry aggregated for import to tagetik .' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')


-- COMMAND ----------

ALTER TABLE igv_azure_general_tagetik_gle_sum_live_total_all ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE igv_azure_general_tagetik_gle_sum_live_total_all ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE igv_azure_general_tagetik_gle_sum_live_total_all ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
