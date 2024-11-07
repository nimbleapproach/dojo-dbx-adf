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
              DROP VIEW IF EXISTS {catalog}.{schema}.globaltransactions
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.globaltransactions as (
select 
  f.GroupEntityCode
, f.EntityCode
, f.DocumentNo
, f.TransactionDate
, f.SalesOrderDate
, f.SalesOrderID
, f.SalesOrderItemID
, f.SKUInternal
, f.SKUMaster
, f.Description
, f.Technology
, f.ProductTypeInternal
, x.product_type ProductTypeMaster
, x.Commitment_Duration_in_months CommitmentDuration1Master
, x.Commitment_Duration_Value CommitmentDuration2Master
, x.Billing_Frequency BillingFrequencyMaster
, x.Consumption_Model ConsumptionModelMaster
, f.VendorCode
, f.VendorNameInternal
, f.VendorNameMaster
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
 from platinum_dev.obt.globaltransactions f 
left join  {catalog}.{schema}.link_product_to_vendor_arr x on x.product_code=SKUInternal and x.vendor_code=VendorNameMaster
)
""")
