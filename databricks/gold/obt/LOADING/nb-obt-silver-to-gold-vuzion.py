# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Vuzion
spark.sql(f"""

CREATE OR Replace VIEW vuzion_globaltransactions AS

SELECT
sa.SID AS SID
,'VU' AS GroupEntityCode
,sa.OrderDate AS TransactionDate
--,'' AS InfinigateEntity
,sa.Total_Value AS RevenueAmount
,sa.CurrencyID AS CurrencyCode
,pl.ResourceID AS SKU
,pl.SetupFeeDescr AS Description
--,'' AS ProductTypeInternal
,sk.Product_Type AS ProductTypeMaster
--,'' AS ProductSubtype
,sk.Commitment_Duration AS CommitmentDuration
,sk.Billing_Frequency AS BillingFrequency
,sk.Consumption_Model AS ConsumptionModel
,s.serviceTemplateID AS VendorCode
,s.VendorName AS VendorName
--,'' AS VendorGeography
--,'' AS VendorStartDate
,r.CompanyID AS ResellerCode
,r.CompanyName AS ResellerNameInternal
--,'' AS ResellerStartDate
,rg.ResellerGroupCode AS ResellerGroupCode
,rg.ResellerGroupName AS ResellerGroupName
--,'' AS ResellerGeographyInternal
--,'' AS ResellerGroupStartDate
,rg.ResellerName AS ResellerNameMaster
,rg.InfinigateCompany AS IGEntityOfReseller
,rg.Entity AS ResellerGeographyMaster
,rg.ResellerID AS ResellerID
FROM
  silver_{ENVIRONMENT}.cloudblue_pba.salesorder sa
LEFT JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.orddet od
ON 
  sa.OrderID = od.OrderID
AND
  sa.Sys_Silver_IsCurrent = true
AND
  od.Sys_Silver_IsCurrent = true
LEFT JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.planrate pl
ON 
  od.resourceID = pl.resourceID
AND
  pl.Sys_Silver_IsCurrent = true
LEFT JOIN
(
  SELECT DISTINCT a2.AccountID AS ResellerID, a2.CompanyName AS ResellerName, a.AccountID AS CompanyID, a.CompanyName AS CompanyName
  FROM silver_{ENVIRONMENT}.cloudblue_pba.account a
  INNER JOIN silver_{ENVIRONMENT}.cloudblue_pba.account a2
  ON a.VendorAccountID = a2.AccountID
  AND a.Sys_Silver_IsCurrent = true
  AND a2.Sys_Silver_IsCurrent = true
) r
LEFT JOIN 
(
  SELECT DISTINCT
  InfinigateCompany,
  ResellerID,
  ResellerGroupCode,
  ResellerGroupName,
  ResellerName,
  Entity
  FROM silver_{ENVIRONMENT}.masterdata.reseller_groups
) rg
ON 
  r.ResellerID = rg.ResellerID
LEFT JOIN
  (
  SELECT DISTINCT subscriptionID, serviceTemplateID,
  CASE
  WHEN serviceTemplateID IN (42,88,66,96,94,95,59,98,223,99,100,93,84,87,86,85,92,208,220,166,195,221,222,259,91,200,210,97,203,261,213,229,240,239,238,209,196,217,204,211,198,89,197,101,201,205,206,212,207,202,214,215,282,283,290,286,289,321,291,346,310,361,392,450,447,448,449,457) THEN 'Acronis'
  WHEN serviceTemplateID IN (146,134,128,147,421) THEN 'Avepoint'
  WHEN serviceTemplateID IN (316,317,319,318) THEN 'Ba Boom Cloud'
  WHEN serviceTemplateID IN (441) THEN 'Barracuda'
  WHEN serviceTemplateID IN (230,260,228,231) THEN 'BitTitan'
  WHEN serviceTemplateID IN (278,275,274,279,315,276,314,277) THEN 'BlueDog'
  WHEN serviceTemplateID IN (143,137,135,139,141) THEN 'ClipTraining'
  WHEN serviceTemplateID IN (1000006,11,1000003,1000049,1000010,1000011,1000005,1000012,1000020,1000009,2,1000002,1000013,1000007,1000004,1000052,1000076,1000083,1000008,1000048,1000060,1000088,1000074,1000072,1000073,1000075,1000092,1000093,1000094,1000095,1000096,1000097,258,1000098,1000099,1000101,1000102,1000103,1000104,1000105,1000107,1000106,462) THEN 'Domain'
  WHEN serviceTemplateID IN (285,284) THEN 'DropBox'
  WHEN serviceTemplateID IN (73,104,125,132,359,360) THEN 'Exclaimer'
  WHEN serviceTemplateID IN (461) THEN 'Gamma'
  WHEN serviceTemplateID IN (271,272,273) THEN 'HRLocker'
  WHEN serviceTemplateID IN (225,242,246,168,169,352,412,413,307,358,309,167) THEN 'Infinigate Cloud'
  WHEN serviceTemplateID IN (463) THEN 'KnowBe4'
  WHEN serviceTemplateID IN (308,305,306,320) THEN 'LastPass'
  WHEN serviceTemplateID IN (162,185,181,180,224) THEN 'Letsignit'
  WHEN serviceTemplateID IN (22,5,20,49,33,6,172,7,39,10,34,36,113,40,1000038,189,176,190,188,186,187,157,177,269,263,237,292,270,23,37,48,115,311,19,111,262,38,304,46) THEN 'Microsoft'
  WHEN serviceTemplateID IN (77,75,76,78,79,80,107) THEN 'Mimecast'
  WHEN serviceTemplateID IN (1000110) THEN 'Nuvem'
  WHEN serviceTemplateID IN (253,254,256,255,294,312) THEN 'Orchestrator'
  WHEN serviceTemplateID IN (1000017,1000054,1000050,1000068,1000031,1000032,1000069,1000051,1000045,1000043,1000037,1000036,1000044,1000077,1000057,1000081,1000065,179,178,232,218,250,233,1000082,351,1000040,1000035,1000042,1000033,1000064,1000028,303,349,350,302,423,464,466) THEN 'PS'
  WHEN serviceTemplateID IN (129,192,150,156,154,460) THEN 'Qunifi'
  WHEN serviceTemplateID IN (1000001,27,1000111,1000021,1000015,1000023,1000016,52,15,1000024,1000067,28,53,1000014,51,1000026,1000025,1000027,54,57,55,1000053,119,1000041,216,102,82,121,280,122,1000085,1000084,1000078,1000062,1000047,108,1000087,184,127,148,58,1000086,130,288,1000056,1000055,1000058,1000059,1000066,1000063,1000079,1000108,1000070,1000089,1000090,1000091,281,1000100,1000061,458,459,1000114,1000115,1000112,1000113,1000116,465,1000117,126) THEN 'Reseller Branding'
  WHEN serviceTemplateID IN (243,244,241,245) THEN 'signNow'
  WHEN serviceTemplateID IN (470,471,467,472,468,469) THEN 'WatchGuard'
  WHEN serviceTemplateID IN (235,251,236,234,446,455,451) THEN 'Wavenet'
  ELSE 'Other' END AS VendorName
  FROM silver_{ENVIRONMENT}.cloudblue_pba.subscription
  WHERE Sys_Silver_IsCurrent = true
  ) s
ON
  od.subscriptionID = s.subscriptionID
LEFT JOIN
  (
  SELECT SKU, Commitment_Duration, Billing_Frequency, Consumption_Model, Product_Type
  FROM silver_{ENVIRONMENT}.masterdata.sku
  WHERE Sys_Silver_IsCurrent = true
  ) sk
ON 
  od.resourceID = sk.sku
""")

# COMMAND ----------

df_obt = spark.read.table('globaltransactions')
df_vuzion = spark.read.table(f'gold_{ENVIRONMENT}.obt.vuzion_globaltransactions')

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_infinigate.columns
intersection_columns = [column for column in target_columns if column  in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_vuzion.select(selection_columns)

# COMMAND ----------

df_selection.write.mode('overwrite').partitionBy('GroupEntityCode').saveAsTable("globaltransactions")
