# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Nuvias
spark.sql(
    f"""         
CREATE OR REPLACE VIEW nuvias_globaltransactions AS

SELECT DISTINCT
  'NUV' AS GroupEntityCode 
  ,trans.SID
  ,trans.InvoiceId
  ,trans.InvoiceDate  AS TransactionDate
  ,trans.LineAmountMST AS RevenueAmount
  ,trans.CurrencyCode
  ,COALESCE(it.Description, "NaN") AS SKU 
  ,itdesc.Description AS Description
  ,invitgroup.Name AS ProductTypeInternal
  ,skumaster.Product_Type AS ProductTypeMaster
  ,'' AS ProductSubtype
  ,skumaster.Commitment_Duration
  ,skumaster.Billing_Frequency
  ,skumaster.Consumption_Model
  ,prod.SAG_NGS1VendorID AS VendorCode
  ,dirpartytable.name AS VendorName
  ,'' AS VendorGeography
  ,to_date('1900-01-01') AS VendorStartDate
  ,inv.InvoiceAccount AS ResellerCode
  ,inv.InvoicingName AS ResellerNameInternal
  ,COALESCE(cust.CREATEDDATETIME, to_timestamp('1900-01-01'))  AS ResellerStartDate
  ,rg.ResellerGroupCode AS ResellerGroupCode
  ,rg.ResellerGroupName AS ResellerGroupName
  ,'' AS ResellerGeographyInternal
  ,to_date('1900-01-01')AS ResellerGroupStartDate
  ,rg.ResellerName AS ResellerNameMaster
  ,rg.Entity AS IGEntityOfReseller
  ,case when len(rg.Entity) = 2 then rg.Entity
        else entmap.ADDRESSCOUNTRYREGIONISOCODE end  AS ResellerGeographyMaster
  ,rg.ResellerID AS ResellerID
FROM
  silver_{ENVIRONMENT}.nuvias_operations.custinvoicetrans AS trans
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custinvoicejour AS inv ON trans.InvoiceId = inv.InvoiceId
  and trans.DataAreaId = inv.DataAreaId
  and inv.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem AS it ON trans.ItemId = it.ItemId
  AND trans.DataAreaId = it.DataAreaId
  AND it.Sys_Silver_IsCurrent = 1
  LEFT JOIN (
    select
      distinct Name,
      max(Description) AS Description
    FROM
      silver_{ENVIRONMENT}.nuvias_operations.ecoresproducttranslation
    WHERE Sys_Silver_IsCurrent = 1
    GROUP BY Name
  ) AS itdesc ON it.Description = itdesc.Name
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.inventitemgroupitem AS invit ON trans.DataAreaId = invit.ItemDataAreaId
  and invit.ItemId = trans.ItemId
  AND invit.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.inventitemgroup AS invitgroup ON invit.ItemGroupId = invitgroup.ItemGroupId
  AND invit.ItemGroupDataAreaId = invitgroup.DataAreaId
  and invitgroup.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.ecoresproduct AS prod ON trans.ItemId = prod.DisplayProductNumber
  AND prod.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.vendtable AS ven ON ven.AccountNum = prod.SAG_NGS1VendorID
  AND ven.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.dirpartytable ON dirpartytable.RECID = ven.Party
  AND dirpartytable.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON inv.InvoiceAccount = rg.ResellerID
  and rg.InfinigateCompany = 'Nuvias'
  AND upper(trans.DataAreaId) = rg.Entity
  LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_omlegalstaging AS entmap ON rg.Entity = entmap.LEGALENTITYID
  and entmap.Sys_Silver_IsCurrent = 1
  LEFT JOIN (
    SELECT
      DISTINCT prod.SAG_NGS1VendorID AS VendorCode,
      dirpartytable.name AS VendorName,CASE
        WHEN prod.SAG_NGS1VendorID = 'VAC000036_NGS1' THEN 'riverbed'
        WHEN prod.SAG_NGS1VendorID = 'VAC000001_NGS1' THEN 'Arbor Networks'
        WHEN prod.SAG_NGS1VendorID = 'VAC000082_NGS1' THEN 'fireeye'
        WHEN prod.SAG_NGS1VendorID = 'VAC000016_NGS1' THEN 'juniper'
        WHEN prod.SAG_NGS1VendorID = 'VAC000035_NGS1' THEN 'PULSE SECURE'
        WHEN prod.SAG_NGS1VendorID = 'VAC000010_NGS1' THEN 'CheckPoint'
        WHEN prod.SAG_NGS1VendorID = 'VAC000084_NGS1' THEN 'rapid7'
        WHEN prod.SAG_NGS1VendorID = 'VAC000052_NGS1' THEN 'hid'
        WHEN prod.SAG_NGS1VendorID = 'VAC000009_NGS1' THEN 'brocade'
        WHEN prod.SAG_NGS1VendorID = 'VAC000042_NGS1' THEN 'tenable'
        WHEN prod.SAG_NGS1VendorID = 'VAC000030_NGS1' THEN 'prolabs'
        WHEN prod.SAG_NGS1VendorID = 'VAC000028_NGS1' THEN 'fortinet'
        WHEN prod.SAG_NGS1VendorID = 'VAC000006_NGS1' THEN 'barracuda'
        WHEN prod.SAG_NGS1VendorID = 'VAC000017_NGS1' THEN 'kaspersky'
        WHEN prod.SAG_NGS1VendorID = 'VAC000018_NGS1' THEN 'progress'
        WHEN prod.SAG_NGS1VendorID = 'VAC000056_NGS1' THEN 'thales'
        WHEN prod.SAG_NGS1VendorID = 'VAC000015_NGS1' THEN 'exagrid'
        WHEN prod.SAG_NGS1VendorID = 'VAC000004_NGS1' THEN 'BeyondTrust'
        WHEN prod.SAG_NGS1VendorID = 'VAC000781_NGS1' THEN 'cloudbees'
        WHEN prod.SAG_NGS1VendorID = 'VAC000023_NGS1' THEN 'malwarebytes'
        WHEN prod.SAG_NGS1VendorID = 'VAC000083_NGS1' THEN 'forcepoint'
        WHEN prod.SAG_NGS1VendorID = 'VAC000049_NGS1' THEN 'watchguard'
        WHEN prod.SAG_NGS1VendorID = 'VAC000057_NGS1' THEN 'barracuda'
        WHEN prod.SAG_NGS1VendorID = 'VAC000003_NGS1' THEN 'allot'
        WHEN prod.SAG_NGS1VendorID = 'VAC000066_NGS1' THEN 'mimecast'
        WHEN prod.SAG_NGS1VendorID = 'VAC000046_NGS1' THEN 'onespan'
        WHEN prod.SAG_NGS1VendorID = 'VAC000012_NGS1' THEN 'corero'
        WHEN prod.SAG_NGS1VendorID = 'VAC000048_NGS1' THEN 'threattrack'
        WHEN prod.SAG_NGS1VendorID = 'VAC000002_NGS1' THEN 'A10Networks'
        WHEN prod.SAG_NGS1VendorID = 'VAC000041_NGS1' THEN 'talari'
        WHEN prod.SAG_NGS1VendorID = 'VAC000022_NGS1' THEN 'macmon'
        WHEN prod.SAG_NGS1VendorID = 'VAC000085_NGS1' THEN 'digicert'
        WHEN prod.SAG_NGS1VendorID = 'VAC000019_NGS1' THEN 'knowbe4'
        WHEN prod.SAG_NGS1VendorID = 'VAC000061_NGS1' THEN 'github'
        WHEN prod.SAG_NGS1VendorID = 'VAC000053_NGS1' THEN 'PALO ALTO'
        WHEN prod.SAG_NGS1VendorID = 'VAC000026_NGS1' THEN 'hid'
        ELSE 'Other Vendor'
      END AS IG_Vendor
    from
      silver_{ENVIRONMENT}.nuvias_operations.ecoresproduct AS prod
      LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.vendtable AS ven ON ven.AccountNum = prod.SAG_NGS1VendorID
      AND ven.Sys_Silver_IsCurrent = 1
      LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.dirpartytable ON dirpartytable.RECID = ven.Party
      AND dirpartytable.Sys_Silver_IsCurrent = 1
    where
      prod.Sys_Silver_IsCurrent = 1
      and prod.SAG_NGS1VendorID <> 'NaN'
  ) AS vm ON prod.SAG_NGS1VendorID = vm.VendorCode
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.sku AS skumaster ON upper(skumaster.Vendor) = upper(vm.IG_Vendor)
  and skumaster.SKU = it.Description
  AND skumaster.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custtable AS cust
  ON inv.InvoiceAccount = cust.AccountNum
  AND inv.DataAreaId = cust.DataAreaId
  AND cust.Sys_Silver_IsCurrent = 1
WHERE
  trans.Sys_Silver_IsCurrent = 1
  and right(trans.InvoiceId, 4) not in ('NGS1', 'NNL2')"""
)

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")

df_nuvias = spark.read.table(f"gold_{ENVIRONMENT}.obt.nuvias_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col


target_columns = df_obt.columns

source_columns = df_nuvias.columns

intersection_columns = [column for column in target_columns if column in source_columns]

selection_columns = [
    col(column) for column in intersection_columns if column not in ["SID"]
]

# COMMAND ----------

df_selection = df_nuvias.select(selection_columns)
df_selection.write.mode("overwrite").partitionBy("GroupEntityCode").saveAsTable(
    "globaltransactions"
)
