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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_globaltransactions_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_globaltransactions_staging as (
select
Id
, case when x.product_type is not null then 1 else 0 end  is_matched
, 'direct'  matched_type
, '' product_vendor_code_arr
, x.product_type ProductTypeMaster
, x.Commitment_Duration_in_months CommitmentDuration1Master
, x.Commitment_Duration_Value CommitmentDuration2Master
, x.Billing_Frequency BillingFrequencyMaster
, x.Consumption_Model ConsumptionModelMaster
, f.GroupEntityCode
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
 from {catalog}.{schema}.globaltransactions_obt f 
left join 
  (select distinct 
        lower(product_code) product_code, lower(vendor_code)  vendor_code
        ,product_type
        ,Commitment_Duration_in_months
        ,Commitment_Duration_Value
        ,Billing_Frequency
        ,Consumption_Model
        from {catalog}.{schema}.link_product_to_vendor_arr
        where is_current=1 
  )x 
    on x.product_code= lower(SKUInternal)
    and x.vendor_code= lower(VendorNameMaster)
    --case when VendorNameMaster ='NaN' then VendorNameInternal else VendorNameMaster end 
)
""")

