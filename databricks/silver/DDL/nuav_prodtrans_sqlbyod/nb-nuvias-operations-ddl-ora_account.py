# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuav_prodtrans_sqlbyod;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ora_account
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,PartyId STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PartyNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OrganizationProfileId STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OrganizationName STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PartyUniqueName STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,Type STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,SalesProfileNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OwnerPartyId STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OwnerPartyNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OwnerEmailAddress STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OwnerName STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PrimaryContactPartyNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PrimaryContactName STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PrimaryContactJobTitle STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PrimaryContactEmail STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PrimaryContactPhone STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,SalesProfileStatus STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,ParentAccountPartyId STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,ParentAccountPartyNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,ParentAccountName STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,CurrencyCode STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,CorpCurrencyCode STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PartyStatus STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PartyType STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,CreationDate STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,LastUpdateDate STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,FormattedPhoneNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,EmailAddress STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PrimaryAddressId STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PartyNumberKey STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,SourceObjectType STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,AddressNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,AddressLine1 STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,AddressLine2 STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,City STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,County STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,Country STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,PostalCode STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,FormattedAddress STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,UltimateParentPartyId STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,UltimateParentName STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,UltimateParentPartyNumber STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,TotalAccountsInHierarchy STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OrganizationDEO_AccountId_c STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OrganizationDEO_OwnersOrganisation_c STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,OrganizationDEO_CompanyName_c STRING
# MAGIC     COMMENT 'TODO'
# MAGIC     ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC     ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC     ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC     ,Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys columns.'
# MAGIC     ,Sys_Silver_IsCurrent BOOLEAN
# MAGIC       COMMENT 'Flag if this is the current version.'
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC       COMMENT 'Flag if this is the deleted version.'
# MAGIC ,CONSTRAINT ora_account_pk PRIMARY KEY(PartyId, Type, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ara_dim_company. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (PartyId, Type)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ora_account ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ora_account ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ora_account ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
