# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Navision
spark.sql(f"""

CREATE OR Replace VIEW infinigate_globaltransactions AS

--Sales Invoice Header/Line
SELECT
sil.SID AS SID
,'IG' AS GroupEntityCode
,'Invoice' AS DocumentType
,to_date(sih.PostingDate) AS TransactionDate
,RIGHT(sih.Sys_DatabaseName,2) AS InfinigateEntity
,sil.Amount AS RevenueAmount
,sih.CurrencyCode
,it.No_ AS SKU
,concat_ws(' ',it.Description,it.Description2,it.Description3,it.Description4) AS Description
,it.ProductType AS ProductTypeInternal
,sk.Product_Type AS ProductTypeMaster
,'' AS ProductSubtype
,sk.Commitment_Duration AS CommitmentDuration
,sk.Billing_Frequency AS BillingFrequency
,sk.Consumption_Model AS ConsumptionModel
,ven.Code AS VendorCode
,ven.Name AS VendorName
,'' AS VendorGeography
,to_date('1900-01-01','yyyy-MM-dd') AS VendorStartDate
,cu.No_ AS ResellerCode
,concat_ws(' ',cu.Name,cu.Name2) AS ResellerNameInternal
,to_date(cu.Createdon) AS ResellerStartDate
,rg.ResellerGroupCode AS ResellerGroupCode
,rg.ResellerGroupName AS ResellerGroupName
,cu.Country_RegionCode  AS ResellerGeographyInternal
,to_date('1900-01-01','yyyy-MM-dd') AS ResellerGroupStartDate
,rg.ResellerName AS ResellerNameMaster
,rg.Entity AS IGEntityOfReseller
,'' AS ResellerGeographyMaster
,rg.ResellerID AS ResellerID
FROM 
  silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
INNER JOIN 
  silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil
ON
  sih.No_ = sil.DocumentNo_
AND
  sih.Sys_DatabaseName = sil.Sys_DatabaseName
AND
  sih.Sys_Silver_IsCurrent = true
AND
  sil.Sys_Silver_IsCurrent = true
INNER JOIN 
  silver_{ENVIRONMENT}.igsql03.item it
ON 
  sil.No_ = it.No_
AND
  sil.Sys_DatabaseName = it.Sys_DatabaseName
AND
  it.Sys_Silver_IsCurrent = true
LEFT JOIN
  (
  SELECT
  Code,
  Name,
  Sys_DatabaseName
  FROM silver_{ENVIRONMENT}.igsql03.dimension_value
  WHERE DimensionCode = 'VENDOR'
  AND Sys_Silver_IsCurrent = true
  ) ven
ON 
  sil.ShortcutDimension1Code = ven.Code
AND
  sil.Sys_DatabaseName = ven.Sys_DatabaseName
AND
  sil.Sys_Silver_IsCurrent = true  
LEFT JOIN 
  silver_{ENVIRONMENT}.igsql03.customer cu
ON 
  sih.`Sell-toCustomerNo_` = cu.No_
AND
  sih.Sys_DatabaseName = cu.Sys_DatabaseName
AND
  cu.Sys_Silver_IsCurrent = true
LEFT JOIN 
(select distinct  
InfinigateCompany,
ResellerID,
ResellerGroupCode,
ResellerGroupName,
ResellerName,
Entity
FROM silver_{ENVIRONMENT}.masterdata.resellergroups) rg
ON 
  concat_ws(RIGHT(sih.Sys_DatabaseName,2), sih.`Sell-toCustomerNo_`) = concat_ws(rg.Entity, rg.ResellerID)
LEFT JOIN
(select SKU, Commitment_Duration, Billing_Frequency, Consumption_Model, Product_Type
from silver_{ENVIRONMENT}.masterdata.sku) sk
ON 
  it.No_ = sk.sku
AND
  it.Sys_Silver_IsCurrent = true

UNION All

--Sales Credit Memo Header/Line  
SELECT
sih.Sys_Silver_HashKey as HashKey
,'IG' AS GroupEntityCode
,'Credit' AS DocumentType
,to_date(sih.PostingDate) AS TransactionDate
,RIGHT(sih.Sys_DatabaseName,2) AS InfinigateEntity
,sil.Amount AS RevenueAmount
,sih.CurrencyCode
,it.No_ AS SKU
,concat_ws(' ',it.Description,it.Description2,it.Description3,it.Description4) AS Description
,'' AS ProductTypeInternal
,sk.Product_Type AS ProductTypeMaster
,'' AS ProductSubtype
,sk.Commitment_Duration AS CommitmentDuration
,sk.Billing_Frequency AS BillingFrequency
,sk.Consumption_Model AS ConsumptionModel
,ven.Code AS VendorCode
,ven.Name AS VendorName
,'' AS VendorGeography
,to_date('1900-01-01','yyyy-MM-dd') AS VendorStartDate
,cu.No_ AS ResellerCode
,concat_ws(' ',cu.Name,cu.Name2) AS ResellerNameInternal
,to_date(cu.Createdon) AS ResellerStartDate
,rg.ResellerGroupCode AS ResellerGroupCode
,rg.ResellerGroupName AS ResellerGroupName
,cu.Country_RegionCode AS ResellerGeographyInternal
,to_date('1900-01-01','yyyy-MM-dd') AS ResellerGroupStartDate
,rg.ResellerName AS ResellerNameMaster
,rg.Entity AS IGEntityOfReseller
,'' AS ResellerGeographyMaster
,rg.ResellerID AS ResellerID
From 
  silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
Inner Join 
  silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil
ON
  sih.No_ = sil.DocumentNo_
AND
  sih.Sys_DatabaseName = sil.Sys_DatabaseName
AND
  sih.Sys_Silver_IsCurrent = true
AND
  sil.Sys_Silver_IsCurrent = true
INNER JOIN 
  silver_{ENVIRONMENT}.igsql03.item it
ON 
  sil.No_ = it.No_
AND
  sil.Sys_DatabaseName = it.Sys_DatabaseName
AND
  it.Sys_Silver_IsCurrent = true
LEFT JOIN
  (
  SELECT
  Code,
  Name,
  Sys_DatabaseName
  FROM silver_{ENVIRONMENT}.igsql03.dimension_value
  WHERE DimensionCode = 'VENDOR'
  AND Sys_Silver_IsCurrent = true
  ) ven
ON 
  sil.ShortcutDimension1Code = ven.Code
AND
  sil.Sys_DatabaseName = ven.Sys_DatabaseName
AND
  sil.Sys_Silver_IsCurrent = true  
LEFT JOIN 
  silver_{ENVIRONMENT}.igsql03.customer cu
ON 
  sih.`Sell-toCustomerNo_` = cu.No_
AND
  sih.Sys_DatabaseName = cu.Sys_DatabaseName  
AND
  cu.Sys_Silver_IsCurrent = true
LEFT JOIN 
(select distinct  
InfinigateCompany,
ResellerID,
ResellerGroupCode,
ResellerGroupName,
ResellerName,
Entity
FROM silver_{ENVIRONMENT}.masterdata.resellergroups) rg
ON 
  concat_ws(RIGHT(sih.Sys_DatabaseName,2), sih.`Sell-toCustomerNo_`) = concat_ws(rg.Entity, rg.ResellerID)
LEFT JOIN
(select SKU, Commitment_Duration, Billing_Frequency, Consumption_Model, Product_Type
from silver_{ENVIRONMENT}.masterdata.sku) sk
ON 
  it.No_ = sk.sku
AND
  it.Sys_Silver_IsCurrent = true
""")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

df_obt = spark.read.table('globaltransactions')
df_infinigate = spark.read.table(f'gold_{ENVIRONMENT}.obt.infinigate_globaltransactions')

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_infinigate.columns
intersection_columns = [column for column in target_columns if column  in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_infinigate.select(selection_columns)

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'IG'").saveAsTable("globaltransactions")
