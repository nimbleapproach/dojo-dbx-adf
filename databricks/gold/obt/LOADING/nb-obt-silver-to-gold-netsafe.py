# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Netsafe
spark.sql(
    f"""         
CREATE OR REPLACE VIEW netsafe_globaltransactions AS

WITH initial_query 
AS
(
SELECT 
'NU' AS GroupEntityCode,
CASE
WHEN invoice.Country = 'Romania' THEN 'RO2'
WHEN invoice.Country = 'Croatia' THEN 'HR2'
WHEN invoice.Country = 'Slovenia' THEN 'SI1'
WHEN invoice.Country = 'Bulgaria' THEN 'BG1'
END AS EntityCode,
to_date(invoice.Invoice_Date) AS TransactionDate,
to_date(invoice.Invoice_Date) AS SalesOrderDate,
coalesce(invoice.Invoice_Number,'NaN') AS SalesOrderID,
coalesce(invoice.Item_ID,'NaN') AS SalesOrderItemID,
COALESCE(invoice.SKU, "NaN") AS SKUInternal,
COALESCE(datanowarr.SKU, 'NaN')  AS SKUMaster,
COALESCE(invoice.SKU_Description,'NaN') AS Description,
COALESCE(invoice.Item_Type,'NaN') AS ProductTypeInternal,
COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
coalesce(invoice.Vendor_ID, 'NaN') AS VendorCode,
coalesce(invoice.Vendor_Name, 'NaN') AS VendorNameInternal,
coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
'NaN' AS VendorGeography,
to_date('1900-01-01') AS VendorStartDate,
coalesce(invoice.Customer_Account,'NaN') AS ResellerCode,
coalesce(invoice.Customer_Name,'NaN') AS ResellerNameInternal,
'NaN' AS ResellerGeographyInternal,
to_date('1900-01-01') AS ResellerStartDate,
coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
to_date('1900-01-01') AS ResellerGroupStartDate,
Transaction_Currency AS CurrencyCode,
cast(Revenue_Transaction_Currency as DECIMAL(10, 2)) AS RevenueAmount,
cast(Cost_Transaction_Currency as DECIMAL(10, 2)) AS CostAmount,
cast(Margin_Transaction_Currency as DECIMAL(10, 2)) AS GP1
FROM 
  silver_{ENVIRONMENT}.netsafe.invoicedata AS invoice
LEFT JOIN
  gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr
ON
  datanowarr.SKU = invoice.SKU
LEFT JOIN 
(
  SELECT DISTINCT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_dev.masterdata.resellergroups
  WHERE InfinigateCompany = 'Nuvias'
  AND Sys_Silver_IsCurrent = true
) rg
ON 
  cast(invoice.Customer_Account as string) = rg.ResellerID
WHERE
  invoice.Sys_Silver_IsCurrent = true
)

SELECT
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
  EntityCode AS VendorGeography,
  CASE
    WHEN VendorStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, VendorCode)
    ELSE VendorStartDate
  END AS VendorStartDate,
  substring_index(ResellerCode, ' ', 1)ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  CASE
    WHEN ResellerStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    ELSE ResellerStartDate
  END AS ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
  CASE
    WHEN ResellerStartDate <= '1900-01-01' THEN min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    ELSE ResellerStartDate
  END AS ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount
FROM
  initial_query""")

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")
df_netsafe = spark.read.table(f"gold_{ENVIRONMENT}.obt.netsafe_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_netsafe.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_netsafe.select(selection_columns)

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NU' AND EntityCode IN ('RO2', 'HR2', 'SI1', 'BG1')").saveAsTable("globaltransactions")
