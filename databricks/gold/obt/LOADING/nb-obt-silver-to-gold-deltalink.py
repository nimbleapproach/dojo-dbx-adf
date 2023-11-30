# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Deltalink
spark.sql(
    f"""         
CREATE OR REPLACE VIEW deltalink_globaltransactions AS

with cte as 
(select 

'NU' AS GroupEntityCode,
'BE3' AS EntityCode,
invoice.InvoiceNumber,
TO_DATE(CAST(UNIX_TIMESTAMP(invoice.InvoiceDate, 'dd/MM/yyyy') AS timestamp)) as TransactionDate,
TO_DATE('1900-01-01' ) AS SalesOrderDate,
RIGHT(invoice.OrderNumber,9) AS SalesOrderID,
"NaN" AS SalesOrderItemID,
COALESCE(Artcode, "NaN") AS SKUInternal,
COALESCE(datanowarr.SKU, 'NaN')  AS SKUMaster,
COALESCE(Description,'NaN') AS Description,
COALESCE(ArticleProductType,'NaN') AS ProductTypeInternal,
COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
coalesce(SupplierID,   'NaN') AS VendorCode,
coalesce(Supplier, 'NaN') AS VendorNameInternal,
coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
'BE3' AS VendorGeography,
to_date('1900-01-01') AS VendorStartDate,
coalesce(ClientNumber,'NaN') AS ResellerCode,
coalesce(Company,'NaN') AS ResellerNameInternal,
'BE3' AS ResellerGeographyInternal,
to_date( '1900-01-01' ) AS ResellerStartDate,
'NaN' AS ResellerGroupCode,
'NaN' AS ResellerGroupName,
to_date('1900-01-01' ) AS ResellerGroupStartDate,
Currency AS CurrencyCode,
cast(RevenueTransaction as DECIMAL(10, 2)) AS RevenueAmount,
cast(case when CostTransaction <0 then CostTransaction
    else CostTransaction*(-1) end  as DECIMAL(10, 2)) as CostAmount,
cast(MarginTransaction  as DECIMAL(10, 2)) as GP1
 from silver_{ENVIRONMENT}.deltalink.invoicedata as invoice
LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = invoice.ArtSupCode
where invoice.Sys_Silver_IsCurrent = 1)

  select
  GroupEntityCode,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  SKUMaster,
  Description,
  ProductTypeInternal,
  ProductTypeMaster,
  CommitmentDuration1Master,
  CommitmentDuration2Master,
  BillingFrequencyMaster,
  ConsumptionModelMaster,
  VendorCode,
  VendorNameInternal,
  VendorNameMaster,
  EntityCode as VendorGeography,
  case
    when VendorStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, VendorCode)
    else VendorStartDate
  end as VendorStartDate,
  substring_index(ResellerCode, ' ', 1)ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount,
  CostAmount,
  GP1
from
  cte""")

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")

df_sl = spark.read.table(f"gold_{ENVIRONMENT}.obt.deltalink_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col


target_columns = df_obt.columns

source_columns = df_sl.columns

intersection_columns = [column for column in target_columns if column in source_columns]

selection_columns = [
    col(column) for column in intersection_columns if column not in ["SID"]
]

# COMMAND ----------

df_selection = df_sl.select(selection_columns)
df_selection = df_selection.fillna(value= 'NaN').replace('', 'NaN')

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NU' AND EntityCode = 'BE3'").saveAsTable("globaltransactions")
