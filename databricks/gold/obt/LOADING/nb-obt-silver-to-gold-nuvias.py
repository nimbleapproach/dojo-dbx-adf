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

with cte as (
 SELECT
    'NU' AS GroupEntityCode,
    UPPER(trans.DataAreaId) AS EntityCode,
    trans.SID,
    to_date(trans.InvoiceDate) AS TransactionDate,
    to_date(salestrans.CREATEDDATETIME) as SalesOrderDate,
    trans.SalesId AS SalesOrderID,
    COALESCE(so_it.Description, 'NaN') AS SalesOrderItemID,
    COALESCE(it.Description, 'NaN') AS SKUInternal,
    COALESCE(datanowarr.SKU, 'NaN')  AS SKUMaster,
    COALESCE(itdesc.Description,'NaN') AS Description,
    COALESCE(invitgroup.Name,'NaN') AS ProductTypeInternal,
    COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
    coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    coalesce(prod.SAG_NGS1VendorID,'NaN') AS VendorCode,
    coalesce(dirpartytable.name   ,'NaN') AS VendorNameInternal,
    coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
    '' AS VendorGeography,
    to_date('1900-01-01') AS VendorStartDate,
    inv.InvoiceAccount AS ResellerCode,
    inv.InvoicingName AS ResellerNameInternal,
    UPPER(trans.DataAreaId) AS ResellerGeographyInternal,
    COALESCE(
      to_date(cust.CREATEDDATETIME),
      to_date('1900-01-01')
    ) AS ResellerStartDate,
    coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
    coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
    to_date('1900-01-01') AS ResellerGroupStartDate,
    trans.CurrencyCode,
    CAST(trans.LineAmountMST AS DECIMAL(10,2)) AS RevenueAmount
  FROM
    silver_{ENVIRONMENT}.nuvias_operations.custinvoicetrans AS trans
    LEFT JOIN (
      SELECT distinct
      InvoiceId,
      DataAreaId,
      InvoiceAccount,
      MAX(InvoicingName) AS InvoicingName
      FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicejour
      WHERE Sys_Silver_IsCurrent = 1
      group by
      InvoiceId,
      DataAreaId,
      InvoiceAccount) AS inv ON trans.InvoiceId = inv.InvoiceId
    and trans.DataAreaId = inv.DataAreaId

    LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem AS it ON trans.ItemId = it.ItemId
    AND trans.DataAreaId = it.DataAreaId
    and it.Sys_Silver_IsCurrent = 1

    left join silver_{ENVIRONMENT}.nuvias_operations.ecoresproducttranslation AS itdesc ON it.Description = itdesc.Name
    AND itdesc.Sys_Silver_IsCurrent = 1

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
    AND rg.Sys_Silver_IsCurrent = 1

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
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.datanowarr AS datanowarr ON upper(datanowarr.Vendor_Name) = upper(vm.IG_Vendor)
    and datanowarr.SKU = it.Description
    AND datanowarr.Sys_Silver_IsCurrent = 1
    LEFT JOIN (
      select
        distinct AccountNum,
        CREATEDDATETIME,
        DataAreaId
      from
        silver_{ENVIRONMENT}.nuvias_operations.custtable
      where
        Sys_Silver_IsCurrent = 1
    ) AS cust ON inv.InvoiceAccount = cust.AccountNum
    AND inv.DataAreaId = cust.DataAreaId

    LEFT JOIN  silver_{ENVIRONMENT}.nuvias_operations.salesline AS salestrans ON trans.SalesId = salestrans.Salesid
    and salestrans.Sys_Silver_IsCurrent = 1
    AND salestrans.SalesStatus <> '4' --This is removed as it identifies cancelled lines on the sales order
    AND salestrans.itemid NOT IN ('Delivery_Out', '6550896')-- This removes the Delivery_Out and Swedish Chemical Tax lines that are on some orders which is never included in the Revenue Number
    AND trans.DataAreaId = salestrans.DataAreaId
    AND trans.ItemId = salestrans.ItemId
    and trans.InventTransId = salestrans.InventTransId
     
    LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem as so_it ON salestrans.ItemId = so_it.ItemId
    and so_it.DataAreaId = salestrans.DataAreaId
    and so_it.Sys_Silver_IsCurrent = 1
  WHERE
    trans.Sys_Silver_IsCurrent = 1
    AND UPPER(trans.DataAreaId) NOT IN ('NGS1', 'NNL2')
    AND UPPER(LEFT(trans.InvoiceId, 2)) IN ('IN', 'CR'))


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
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
  case
    when (
      ResellerGroupName = 'No Group'
      or ResellerGroupName is null
    ) then (
      case
        when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
        else ResellerStartDate
      end
    )
    else (
      case
        when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerGroupName)
        else min(ResellerStartDate) OVER(PARTITION BY EntityCode, ResellerGroupName)
      end
    )
  end AS ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount
from
  cte"""
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
df_selection = df_selection.fillna(value= 'NaN').replace('', 'NaN')
df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NU'").saveAsTable("globaltransactions")
