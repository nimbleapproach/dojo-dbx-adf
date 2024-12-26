# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

# REMOVE ONCE SOLUTION IS LIVE
if ENVIRONMENT == 'dev':
    spark.sql(f"""
              DROP TABLE IF EXISTS {catalog}.{schema}.globaltransactions_arr
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.globaltransactions_arr (
  GroupEntityCode STRING,
  EntityCode STRING,
  DocumentNo STRING COMMENT 'Business Key',
  TransactionDate TIMESTAMP,
  SalesOrderDate STRING,
  SalesOrderID STRING,
  SalesOrderItemID STRING,
  SKUInternal STRING,
  SKUMaster STRING,
  Description STRING,
  Technology STRING COMMENT 'Display Name of the Technology',
  ProductTypeInternal STRING,
  VendorCode STRING,
  VendorNameInternal STRING,
  VendorNameMaster STRING,
  VendorGeography STRING,
  VendorStartDate STRING,
  ResellerCode STRING,
  ResellerNameInternal STRING,
  ResellerGeographyInternal STRING COMMENT 'TODO',
  ResellerStartDate STRING,
  ResellerGroupCode STRING,
  ResellerGroupName STRING,
  ResellerGroupStartDate STRING,
  EndCustomer STRING COMMENT 'TODO',
  IndustryVertical STRING COMMENT 'Text to be used as display',
  CurrencyCode STRING,
  RevenueAmount DOUBLE,
  Period_FX_rate DECIMAL(38,6),
  RevenueAmount_Euro DECIMAL(30,2),
  GP1 DOUBLE,
  GP1_Euro DECIMAL(38,14),
  COGS DOUBLE,
  COGS_Euro DECIMAL(38,14),
  GL_Group STRING,
  TopCostFlag INT,
  ProductTypeMaster STRING,
  CommitmentDuration1Master STRING,
  CommitmentDuration2Master STRING,
  BillingFrequencyMaster STRING,
  ConsumptionModelMaster STRING,
  matched_arr_type STRING,
  matched_type STRING,
  is_matched INT)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""")
