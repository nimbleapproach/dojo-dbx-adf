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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_globaltransactions_obt_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_globaltransactions_obt_staging as (
select
  monotonically_increasing_id() AS Id
, 0  is_matched
, ''  matched_type
, '' product_vendor_code_arr
, f.SKUInternal
, f.SKUMaster
, f.VendorCode
, f.VendorNameInternal
, f.VendorNameMaster
, f.ProductTypeMaster
, f.CommitmentDuration1Master
, f.CommitmentDuration2Master
, f.BillingFrequencyMaster
, f.ConsumptionModelMaster
, f.GroupEntityCode
, f.EntityCode
, f.DocumentNo
, f.TransactionDate
, f.SalesOrderDate
, f.SalesOrderID
, f.SalesOrderItemID
, f.Description
, f.Technology
, f.ProductTypeInternal
, f.VendorGeography
, f.VendorStartDate
, f.ResellerCode
, f.ResellerNameInternal
, f.ResellerGeographyInternal
, f.ResellerStartDate
, f.ResellerGroupCode
, f.ResellerGroupName
, f.ResellerGroupStartDate
, f.EndCustomer
, f.IndustryVertical
, f.CurrencyCode
, f.RevenueAmount
, f.Period_FX_rate
, f.RevenueAmount_Euro
, f.GP1
, f.GP1_Euro
, f.COGS
, f.COGS_Euro
, f.GL_Group
, f.TopCostFlag
 from platinum_{ENVIRONMENT}.obt.globaltransactions f
)
""")
