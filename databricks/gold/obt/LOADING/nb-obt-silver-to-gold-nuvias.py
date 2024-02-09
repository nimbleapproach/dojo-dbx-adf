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
CREATE OR REPLACE view gold_{ENVIRONMENT}.obt.nuvias_globaltransactions AS


with cte as (
  SELECT
    'NU' AS GroupEntityCode,
    UPPER(entity.TagetikEntityCode) AS EntityCode,
    trans.DataAreaId,
    trans.ItemId,
    trans.InvoiceId,
    to_date(trans.InvoiceDate) AS TransactionDate,
    to_date(salestrans.CREATEDDATETIME) as SalesOrderDate,
    trans.SalesId AS SalesOrderID,
    COALESCE(so_it.Description, 'NaN') AS SalesOrderItemID,
    COALESCE(it.Description, 'NaN') AS SKUInternal,
    COALESCE(datanowarr.SKU, 'NaN') AS SKUMaster,
    COALESCE(itdesc.Description, 'NaN') AS Description,
    COALESCE(invitgroup.Name, 'NaN') AS ProductTypeInternal,
    COALESCE(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
    coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    /*
        coalesce(prod.SAG_NGS1VendorID,'NaN') AS VendorCode,
        coalesce(dirpartytable.name   ,'NaN') AS VendorNameInternal,
        */
    coalesce(disit.PrimaryVendorID, 'NaN') AS VendorCode,
    coalesce(disit.PrimaryVendorName, 'NaN') AS VendorNameInternal,
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
    coalesce(rg.ResellerGroupCode, 'NaN') AS ResellerGroupCode,
    coalesce(rg.ResellerGroupName, 'NaN') AS ResellerGroupName,
    to_date('1900-01-01') AS ResellerGroupStartDate,
    trans.CurrencyCode,
    CAST(trans.LineAmountMST AS DECIMAL(10, 2)) AS RevenueAmount
  FROM
    silver_{ENVIRONMENT}.nuvias_operations.custinvoicetrans AS trans
    LEFT JOIN (
      SELECT
        distinct InvoiceId,
        DataAreaId,
        InvoiceAccount,
        MAX(InvoicingName) AS InvoicingName
      FROM
        silver_{ENVIRONMENT}.nuvias_operations.custinvoicejour
      WHERE
        Sys_Silver_IsCurrent = 1
      group by
        InvoiceId,
        DataAreaId,
        InvoiceAccount
    ) AS inv ON trans.InvoiceId = inv.InvoiceId
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
    /*
        LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.vendtable AS ven ON ven.AccountNum = prod.SAG_NGS1VendorID
        AND ven.Sys_Silver_IsCurrent = 1
        LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.dirpartytable ON dirpartytable.RECID = ven.Party
        AND dirpartytable.Sys_Silver_IsCurrent = 1
        */
    LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems AS disit ON trans.ItemId = disit.ItemId
    AND CASE
      WHEN UPPER(trans.DataAreaId) = 'NUK1' then 'NGS1'
      WHEN UPPER(trans.DataAreaId) IN (
        'NPO1',
        'NDK1',
        'NNO1',
        'NAU1',
        'NCH1',
        'NSW1',
        'NFR1',
        'NNL1',
        'NES1',
        'NDE1',
        'NFI1'
      ) then 'NNL2'
      ELSE UPPER(trans.DataAreaId)
    END = CASE
      WHEN UPPER(disit.CompanyID) = 'NUK1' then 'NGS1'
      WHEN UPPER(disit.CompanyID) IN (
        'NPO1',
        'NDK1',
        'NNO1',
        'NAU1',
        'NCH1',
        'NSW1',
        'NFR1',
        'NNL1',
        'NES1',
        'NDE1',
        'NFI1'
      ) then 'NNL2'
      ELSE UPPER(disit.CompanyID)
    END
    AND disit.Sys_Silver_IsCurrent = true
    /*
        LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON inv.InvoiceAccount = rg.ResellerID
        and rg.InfinigateCompany = 'Nuvias'
        AND upper(trans.DataAreaId) = rg.Entity
        AND rg.Sys_Silver_IsCurrent = 1
        */
    LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = it.Description
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
    LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.salesline AS salestrans ON trans.SalesId = salestrans.Salesid
    and salestrans.Sys_Silver_IsCurrent = 1
    AND salestrans.SalesStatus <> '4' --This is removed as it identifies cancelled lines on the sales order
    AND salestrans.itemid NOT IN ('Delivery_Out', '6550896') -- This removes the Delivery_Out and Swedish Chemical Tax lines that are on some orders which is never included in the Revenue Number
    AND trans.DataAreaId = salestrans.DataAreaId
    AND trans.ItemId = salestrans.ItemId
    and trans.InventTransId = salestrans.InventTransId
    LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem as so_it ON salestrans.ItemId = so_it.ItemId
    and so_it.DataAreaId = salestrans.DataAreaId
    and so_it.Sys_Silver_IsCurrent = 1
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON upper(trans.DataAreaId) = entity.SourceEntityCode --Comment by MS (30/01/2024) - Start
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON inv.InvoiceAccount = rg.ResellerID
    and rg.InfinigateCompany = 'Nuvias'
    AND UPPER(entity.TagetikEntityCode) = rg.Entity
    AND rg.Sys_Silver_IsCurrent = true --Comment by MS (30/01/2024) - End
  WHERE
    trans.Sys_Silver_IsCurrent = 1
    AND UPPER(trans.DataAreaId) NOT IN ('NGS1', 'NNL2')
    AND UPPER(LEFT(trans.InvoiceId, 2)) IN ('IN', 'CR')
),
/*[yz] 09.02.2024 custinvoicetrans is on line level whereas inventtrans for cost is on item level so needs to group first the revenue to item level before brining in the cost*/ 
invoice as(
  select
    GroupEntityCode,
    EntityCode,
    DataAreaId,
    InvoiceId,
    ItemId,
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
    VendorGeography,
    VendorStartDate,
    ResellerCode,
    ResellerNameInternal,
    ResellerGeographyInternal,
    ResellerStartDate,
    ResellerGroupCode,
    ResellerGroupName,
    ResellerGroupStartDate,
    CurrencyCode,
    sum(RevenueAmount) RevenueAmount
  from
    cte
  group by
    all
)

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
  case
    when length(EntityCode) > 3 then 'USD'
    ELSE CurrencyCode
  END AS CurrencyCode,
  CAST(RevenueAmount AS DECIMAL(10,2)),
  cast(inventtrans.CostAmountPosted as decimal(10, 2)) as CostAmount,
  cast(RevenueAmount+ inventtrans.CostAmountPosted + inventtrans.CostAmountAdjustment as decimal(10, 2)) as GP1

from
  invoice
  LEFT JOIN (
    SELECT
      InvoiceId,
      ItemId,
      DataAreaId,
      SUM(CostAmountPosted) as CostAmountPosted,
      SUM(CostAmountAdjustment) as CostAmountAdjustment
    FROM
      silver_{ENVIRONMENT}.nuvias_operations.inventtrans
    WHERE
      DataAreaId NOT IN ('NNL2', 'NGS1')
      and Sys_Silver_IsCurrent = 1
    GROUP BY
      all
  ) inventtrans ON invoice.InvoiceId = inventtrans.InvoiceId
  and invoice.ItemId = inventtrans.ItemId
  AND invoice.DataAreaId = inventtrans.DataAreaId
"""
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
df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NU' AND EntityCode NOT IN ('BE3', 'RO2', 'HR2', 'SI1', 'BG1')").saveAsTable("globaltransactions")
