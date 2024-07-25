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

USE SCHEMA igsql01;

-- COMMAND ----------

CREATE OR REPLACE TABLE tbl_account (
    account_pk BIGINT
      COMMENT 'Surrogate Key'
    ,account_id STRING
      COMMENT 'Business Key'
    ,account_number STRING
      COMMENT 'Business Customer ID'
    ,account_extended_additional STRING
      COMMENT 'TODO'
    ,name STRING
      COMMENT 'TODO'
    ,description STRING
      COMMENT 'TODO'
    ,country_fk STRING
      COMMENT 'TODO'
    ,fax STRING
      COMMENT 'TODO'
    ,telephone STRING
      COMMENT 'TODO'
    ,parent_account_fk STRING
      COMMENT 'account_id of the parent'
    ,created_on TIMESTAMP
      COMMENT 'TODO'
    ,created_by STRING
      COMMENT 'TODO'
    ,modified_on TIMESTAMP
      COMMENT 'TODO'
    ,modified_by STRING
      COMMENT 'TODO'
    ,entity_fk STRING
      COMMENT 'TODO'
    ,sys_bronze_insert_date_time_utc TIMESTAMP
      COMMENT 'TODO'
    ,sys_database_name STRING
      COMMENT 'TODO'
    ,sys_silver_insert_date_time_utc TIMESTAMP
      COMMENT 'TODO'
    ,sys_silver_modified_date_time_utc TIMESTAMP
      COMMENT 'TODO'
    ,sys_silver_hash_key BIGINT
      COMMENT 'TODO'
    ,sys_silver_is_current BOOLEAN
      COMMENT 'TODO'
)
COMMENT 'This table contains the data for customer account.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (account_id, sys_database_name)

-- COMMAND ----------

CREATE OR REPLACE TABLE tbl_country (
    country_pk BIGINT
      COMMENT 'Surrogate Key'
    ,country_id STRING
      COMMENT 'Business Key'
    ,country STRING
      COMMENT 'TODO'
    ,country_code STRING
      COMMENT 'TODO'
    ,country_iso STRING
      COMMENT 'TODO'
    ,created_on TIMESTAMP
      COMMENT 'TODO'
    ,created_by STRING
      COMMENT 'TODO'
    ,modified_on TIMESTAMP
      COMMENT 'TODO'
    ,modified_by STRING
      COMMENT 'TODO'
    ,sys_bronze_insert_date_time_utc TIMESTAMP
      COMMENT 'TODO'
    ,sys_database_name STRING
      COMMENT 'TODO'
    ,sys_silver_insert_date_time_utc TIMESTAMP
      COMMENT 'TODO'
    ,sys_silver_modified_date_time_utc TIMESTAMP
      COMMENT 'TODO'
    ,sys_silver_hash_key BIGINT
      COMMENT 'TODO'
    ,sys_silver_is_current BOOLEAN
      COMMENT 'TODO'
)
COMMENT 'This table contains the data for countries.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY    (country_id, sys_database_name)

-- COMMAND ----------

CREATE OR REPLACE TABLE tbl_entity (
entity_pk BIGINT
  COMMENT 'Surrogate Key'
,entity_id STRING
  COMMENT 'Business Key'
,entity_code STRING
  COMMENT 'TODO'
,entity_description STRING
  COMMENT 'TODO'
,created_on TIMESTAMP
  COMMENT 'TODO'
,created_by STRING
  COMMENT 'TODO'
,modified_on TIMESTAMP
  COMMENT 'TODO'
,modified_by STRING
  COMMENT 'TODO'
,sys_bronze_insert_date_time_utc TIMESTAMP
  COMMENT 'TODO'
,sys_database_name STRING
  COMMENT 'TODO'
,sys_silver_insert_date_time_utc TIMESTAMP
  COMMENT 'TODO'
,sys_silver_modified_date_time_utc TIMESTAMP
  COMMENT 'TODO'
,sys_silver_hash_key BIGINT
  COMMENT 'TODO'
,sys_silver_is_current BOOLEAN
  COMMENT 'TODO'
)
COMMENT 'This table contains the data for business entities (business units).' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY    (entity_id, sys_database_name)

-- COMMAND ----------

CREATE OR REPLACE TABLE tbl_reseller (
  reseller_pk BIGINT
    COMMENT 'Surrogate key from account table'
  ,reseller_id STRING
    COMMENT 'Business key from account table'
  ,reseller STRING
    COMMENT 'Name of reseller'
  ,reseller_code STRING
    COMMENT 'Code recognised across systems'
  ,address_line_1 STRING
    COMMENT 'Not populated'
  ,address_line_2 STRING
    COMMENT 'Not populated'
  ,city STRING
    COMMENT 'Not populated'
  ,country_fk STRING
    COMMENT 'TODO'
  ,created_on TIMESTAMP
    COMMENT 'TODO'
  ,created_by STRING
    COMMENT 'Not populated'
  ,modified_on TIMESTAMP
    COMMENT 'TODO'
  ,modified_by STRING
    COMMENT 'Not populated'
  ,entity_fk STRING
    COMMENT 'TODO'
  ,sys_bronze_insert_date_time_utc TIMESTAMP
    COMMENT 'TODO'
  ,sys_database_name STRING
    COMMENT 'TODO'
  ,sys_silver_insert_date_time_utc TIMESTAMP
    COMMENT 'TODO'
  ,sys_silver_modified_date_time_utc TIMESTAMP
    COMMENT 'TODO'
  ,sys_silver_hash_key BIGINT
    COMMENT 'TODO'
  ,sys_silver_is_current BOOLEAN
    COMMENT 'TODO'
)
COMMENT 'This table contains the resellers data from accounts.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY    (reseller_id, sys_database_name)

-- COMMAND ----------

CREATE OR REPLACE TABLE tbl_reseller_group (
  reseller_group_pk BIGINT
    COMMENT 'TODO'
  ,reseller_group_id STRING
    COMMENT 'TODO'
  ,reseller_group STRING
    COMMENT 'TODO'
  ,reseller_group_code STRING
    COMMENT 'TODO'
  ,created_on TIMESTAMP
    COMMENT 'TODO'
  ,created_by STRING
    COMMENT 'TODO'
  ,modified_on TIMESTAMP
    COMMENT 'TODO'
  ,modified_by STRING
    COMMENT 'TODO'
  ,sys_bronze_insert_date_time_utc TIMESTAMP
    COMMENT 'TODO'
  ,sys_database_name STRING
    COMMENT 'TODO'
  ,sys_silver_insert_date_time_utc TIMESTAMP
    COMMENT 'TODO'
  ,sys_silver_modified_date_time_utc TIMESTAMP
    COMMENT 'TODO'
  ,sys_silver_hash_key BIGINT
    COMMENT 'TODO'
  ,sys_silver_is_current BOOLEAN
    COMMENT 'TODO'
)
COMMENT 'This table contains the reseller groups data from accounts.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY    (reseller_group_id, sys_database_name)

-- COMMAND ----------

CREATE OR REPLACE TABLE tbl_reseller_group_link (
  reseller_fk BIGINT
    COMMENT 'Key of reseller (tbl_reseller.reseller_pk)'
  ,reseller_group_fk BIGINT
    COMMENT 'Key of reseller group (tbl_reseller_group.reseller_group_pk)'
)
COMMENT 'This table links resellers to reseller groups.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
