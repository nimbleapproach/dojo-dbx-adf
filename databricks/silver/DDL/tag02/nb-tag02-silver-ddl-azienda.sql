-- Databricks notebook source
-- MAGIC %python
-- MAGIC try:
-- MAGIC     ENVIRONMENT = dbutils.widgets.get("wg_environment")
-- MAGIC except:
-- MAGIC     dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices =  ['dev','uat','prod'])
-- MAGIC     ENVIRONMENT = dbutils.widgets.get("wg_environment")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA tag02;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azienda
  ( 
    COD_AZIENDA STRING NOT NULL 
      COMMENT 'Entity Code'
      CONSTRAINT azienda_pk PRIMARY KEY
    ,RAGIONE_SOCIALE STRING
      COMMENT 'Business Name'
    ,FLAG_AZIENDA TINYINT
      COMMENT 'Tells if the entity is a division or real legal entity. \n Possible values are: \n
      0: Divison \n
      1: Real Entity'
    ,SEDE_LEGALE STRING
      COMMENT 'The legal headquarters'
    ,SEDE_AMMINISTRATIVA STRING
      COMMENT 'The administrative city'
    ,DATA_COSTITUZIONE TIMESTAMP
      COMMENT 'The establishment date'
    ,TIPO_CONSOLIDAMENTO STRING
      COMMENT 'Consolidation type \n Possible values are: \n
      I: Line by line \n
      P: Proportional \n
      C: Cost \n
      E: Equity \n'
    ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
      COMMENT 'The timestamp when this entry landed in bronze.'
    ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry landed in silver.'
    ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modifed in silver.'
  )
COMMENT 'This table contains the "entity". \n
  The Entity dimension in Tagetik can be used to represent the Legal Entity (since the legal structure is represented through relationships between
  "Entities") and/or to identify, the Entities to submit in a data collection process. \n
  In general: \n
  • in case of having to operate a consolidation process, the Entity or a "part" of the Entity should be added to the Entity dimension \n
  • otherwise, the list of Entities to submit can be added on the Entity dimension' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')

-- COMMAND ----------

--ALTER TABLE azienda ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');
