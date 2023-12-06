# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold D2B
spark.sql(
    f"""         
CREATE OR REPLACE VIEW d2b_globaltransactions AS

WITH initial_query 
AS
(
SELECT
'IG' AS GroupEntityCode,
'FR2' EntityCode,
to_date(sales.INVOICE_DATE) AS TransactionDate,
to_date(sales.INVOICE_DATE) AS SalesOrderDate,
coalesce(sales.INVOICE,'NaN') AS SalesOrderID,
'NaN' AS SalesOrderItemID,
COALESCE(sales.SKU,'NaN') AS SKUInternal,
COALESCE(datanowarr.SKU,'NaN') AS SKUMaster,
COALESCE(sales.Description,'NaN') AS Description,
'NaN'  AS ProductTypeInternal,
COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
coalesce(datanowarr.Commitment_Duration_in_months,'NaN') AS CommitmentDuration1Master,
coalesce(datanowarr.Commitment_Duration_Value,'NaN') AS CommitmentDuration2Master,
coalesce(datanowarr.Billing_Frequency,'NaN') AS BillingFrequencyMaster,
coalesce(datanowarr.Consumption_Model,'NaN') AS ConsumptionModelMaster,
coalesce(sales.VENDOR,'NaN') AS VendorCode,
coalesce(sales.VENDOR,'NaN') AS VendorNameInternal,
coalesce(datanowarr.Vendor_Name,'NaN') AS VendorNameMaster,
'NaN' AS VendorGeography,
to_date('1900-01-01') AS VendorStartDate,
coalesce(sales.CUSTOMER_PO,'NaN') AS ResellerCode,
coalesce(sales.CUSTOMER_ID,'NaN') AS ResellerNameInternal,
'NaN' AS ResellerGeographyInternal,
to_date('1900-01-01') AS ResellerStartDate,
coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
to_date('1900-01-01') AS ResellerGroupStartDate,
'EUR' AS CurrencyCode,
cast(sales.SALES_PRICE as DECIMAL(10, 2)) AS RevenueAmount
FROM 
  silver_{ENVIRONMENT}.d2b.sales AS sales
LEFT JOIN
  gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr
ON
  datanowarr.SKU = sales.SKU
LEFT JOIN 
(
  SELECT DISTINCT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
  WHERE InfinigateCompany = 'Infinigate'
  AND Sys_Silver_IsCurrent = true
) rg
ON 
  cast(sales.CUSTOMER_PO as string) = rg.ResellerID
WHERE
  sales.Sys_Silver_IsCurrent = true
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
  ResellerCode,
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
df_d2b = spark.read.table(f"gold_{ENVIRONMENT}.obt.d2b_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_d2b.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_d2b.select(selection_columns)

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'IG' AND EntityCode IN ('FR2')").saveAsTable("globaltransactions")
