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
spark.sql(
    f"""

CREATE OR Replace VIEW infinigate_globaltransactions AS

with cte as (
  --- sales invoice
  SELECT
    sil.SID AS SID,
    'IG' AS GroupEntityCode,
    entity.TagetikEntityCode AS EntityCode,
    sil.DocumentNo_ AS DocumentNo,
    sil.LineNo_ AS LineNo,
    to_date(sih.PostingDate) AS TransactionDate,
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) as SalesOrderDate,
    coalesce(so.SalesOrderID, 'NaN') AS SalesOrderID,
    coalesce(so.SalesOrderItemID, 'NaN') AS SalesOrderItemID,
    coalesce(it.No_, 'NaN') AS SKUInternal,
    coalesce(datanowarr.SKU, 'NaN') AS SKUMaster,
    trim(
      (concat(
        regexp_replace(it.Description, 'NaN', ''),
        regexp_replace(it.Description2, 'NaN', ''),
        regexp_replace(it.Description3, 'NaN', ''),
        regexp_replace(it.Description4, 'NaN', '')
      ))
    ) AS Description,
    coalesce(it.ProductType, 'NaN') AS ProductTypeInternal,
    coalesce(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
    coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    coalesce(ven.Code, 'NaN') AS VendorCode,
    coalesce(ven.Name, 'NaN') AS VendorNameInternal,
    coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
    '' AS VendorGeography,
    to_date('1900-01-01', 'yyyy-MM-dd') AS VendorStartDate,
    coalesce(cu.No_, 'NaN') AS ResellerCode,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS ResellerNameInternal,
    cu.Country_RegionCode AS ResellerGeographyInternal,
    to_date(cu.Createdon) AS ResellerStartDate,
    rg.ResellerGroupCode AS ResellerGroupCode,
    rg.ResellerGroupName AS ResellerGroupName,
    to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate,
     Case
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'CH' THEN 'CHF'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'UK' THEN 'GBP'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'SE' THEN 'SEK'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'NO' THEN 'NOK'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'DK' THEN 'DKK'
      ELSE sih.CurrencyCode
    END AS CurrencyCode,
    CAST(case when sih.CurrencyFactor>0 then Amount/sih.CurrencyFactor else Amount end  AS DECIMAL(10, 2)) AS RevenueAmount,
    CAST(case when sil.Quantity>0 then  sil.UnitCostLCY*sil.Quantity*(-1) else 0 end AS DECIMAL(10, 2)) as CostAmount
  FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
    INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
    AND sil.Sys_DatabaseName = it.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    LEFT JOIN (
      SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_{ENVIRONMENT}.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven ON it.GlobalDimension1Code = ven.Code
    AND it.Sys_DatabaseName = ven.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON concat(
      RIGHT(sih.Sys_DatabaseName, 2),
      sih.`Sell-toCustomerNo_`
    ) = concat(rg.Entity, rg.ResellerID)
    AND rg.Sys_Silver_IsCurrent = TRUE
    LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr ON it.No_ = datanowarr.sku
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    LEFT JOIN (
      select
        sla.Sys_DatabaseName,
        sla.DocumentNo_ As SalesOrderID,
        sha.DocumentDate AS SalesOrderDate,
        sla.LineNo_ AS SalesOrderLineNo,
        sla.No_ as SalesOrderItemID
      from
        silver_{ENVIRONMENT}.igsql03.sales_line_archive as sla
        inner join (
          select
            No_,
            Sys_DatabaseName,
            max(DocumentDate) DocumentDate,
            max(VersionNo_) VersionNo_
          from
            silver_{ENVIRONMENT}.igsql03.sales_header_archive
          where
            Sys_Silver_IsCurrent = 1
          group by
            No_,
            Sys_DatabaseName
        ) as sha on sla.DocumentNo_ = sha.No_
        and sla.DocumentType = 1
        and sla.Doc_No_Occurrence = 1
        and sla.VersionNo_ = sha.VersionNo_
        and sla.Sys_DatabaseName = sha.Sys_DatabaseName
        and sla.Sys_Silver_IsCurrent = 1
    ) AS so on sil.OrderNo_ = so.SalesOrderID
    and sil.OrderLineNo_ = so.SalesOrderLineNo
    and sil.Sys_DatabaseName = so.Sys_DatabaseName

    WHERE 
        (UPPER(sil.No_)NOT LIKE 'PORTO'
      AND UPPER(sil.No_)NOT LIKE 'VERSAND%'
      AND UPPER(sil.No_)NOT LIKE 'SHIP%'
      AND UPPER(sil.No_)NOT LIKE 'TRANS%'
      AND UPPER(sil.No_)NOT LIKE 'POST%'
      AND UPPER(sil.No_)NOT LIKE 'FREI%'
      AND UPPER(sil.No_)NOT LIKE 'FRACHT%'
      AND UPPER(sil.No_)NOT LIKE 'EXP%')
      AND sil.Gen_Bus_PostingGroup not like 'IC%'

  UNION all
    --- SALES CR MEMO
  SELECT
    sil.SID AS SID,
    'IG' AS GroupEntityCode,
    entity.TagetikEntityCode AS EntityCode,
    sil.DocumentNo_ AS DocumentNo,
    sil.LineNo_ AS LineNo,
    to_date(sih.PostingDate) AS TransactionDate,
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) as SalesOrderDate,
    coalesce(so.SalesOrderID, 'NaN') AS SalesOrderID,
    coalesce(so.SalesOrderItemID, 'NaN') AS SalesOrderItemID,
    coalesce(it.No_, 'NaN') AS SKUInternal,
    coalesce(it.No_,sil.No_, 'NaN')  AS SKUMaster,
    trim(
      concat(
        regexp_replace(it.Description, 'NaN', ''),
        regexp_replace(it.Description2, 'NaN', ''),
        regexp_replace(it.Description3, 'NaN', ''),
        regexp_replace(it.Description4, 'NaN', '')
      )
    ) AS Description,
    coalesce(it.ProductType, 'NaN') AS ProductTypeInternal,
    coalesce(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
    coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    coalesce(ven.Code, 'NaN') AS VendorCode,
    coalesce(ven.Name, 'NaN') AS VendorNameInternal,
    coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
    '' AS VendorGeography,
    to_date('1900-01-01', 'yyyy-MM-dd') AS VendorStartDate,
    coalesce(cu.No_, 'NaN') AS ResellerCode,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS ResellerNameInternal,
    cu.Country_RegionCode AS ResellerGeographyInternal,
    to_date(cu.Createdon) AS ResellerStartDate,
    rg.ResellerGroupCode AS ResellerGroupCode,
    rg.ResellerGroupName AS ResellerGroupName,
    to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate,
     Case
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'CH' THEN 'CHF'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'UK' THEN 'GBP'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'SE' THEN 'SEK'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'NO' THEN 'NOK'
      WHEN sih.CurrencyCode = 'NaN' AND left(entity.TagetikEntityCode, 2) = 'DK' THEN 'DKK'
      ELSE sih.CurrencyCode
    END AS CurrencyCode,
    CAST((case when sih.CurrencyFactor>0 then Amount/sih.CurrencyFactor else Amount end) *(-1) AS DECIMAL(10, 2)) AS RevenueAmount,
    CAST(case when sil.Quantity>0 then  sil.UnitCostLCY*sil.Quantity else 0 end AS DECIMAL(10, 2)) as CostAmount
  FROM
    silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
    INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
    AND sil.Sys_DatabaseName = it.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    LEFT JOIN (
      SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_{ENVIRONMENT}.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven ON it.GlobalDimension1Code = ven.Code
    AND it.Sys_DatabaseName = ven.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON concat(
      RIGHT(sih.Sys_DatabaseName, 2),
      sih.`Sell-toCustomerNo_`
    ) = concat(rg.Entity, rg.ResellerID)
    AND rg.Sys_Silver_IsCurrent = TRUE
    LEFT JOIN gold_dev.obt.datanowarr ON it.No_ = datanowarr.sku
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    LEFT JOIN (
      select
        sla.Sys_DatabaseName,
        sla.DocumentNo_ As SalesOrderID,
        sha.DocumentDate AS SalesOrderDate,
        sla.LineNo_ AS SalesOrderLineNo,
        sla.No_ as SalesOrderItemID
      from
        silver_{ENVIRONMENT}.igsql03.sales_line_archive as sla
        inner join (
          select
            No_,
            Sys_DatabaseName,
            max(DocumentDate) DocumentDate,
            max(VersionNo_) VersionNo_
          from
            silver_{ENVIRONMENT}.igsql03.sales_header_archive
          where
            Sys_Silver_IsCurrent = 1
          group by
            No_,
            Sys_DatabaseName
        ) as sha on sla.DocumentNo_ = sha.No_
        and sla.DocumentType = 1
        and sla.Doc_No_Occurrence = 1
        and sla.VersionNo_ = sha.VersionNo_
        and sla.Sys_DatabaseName = sha.Sys_DatabaseName
        and sla.Sys_Silver_IsCurrent = 1
    ) AS so on sil.OrderNo_ = so.SalesOrderID
    and sil.OrderLineNo_ = so.SalesOrderLineNo
    and sil.Sys_DatabaseName = so.Sys_DatabaseName
    WHERE 
        (UPPER(sil.No_)NOT LIKE 'PORTO'
      AND UPPER(sil.No_)NOT LIKE 'VERSAND%'
      AND UPPER(sil.No_)NOT LIKE 'SHIP%'
      AND UPPER(sil.No_)NOT LIKE 'TRANS%'
      AND UPPER(sil.No_)NOT LIKE 'POST%'
      AND UPPER(sil.No_)NOT LIKE 'FREI%'
      AND UPPER(sil.No_)NOT LIKE 'FRACHT%'
      AND UPPER(sil.No_)NOT LIKE 'EXP%')
      AND sil.Gen_Bus_PostingGroup not like 'IC%'
)
select
  GroupEntityCode,
  EntityCode,
  DocumentNo,
  LineNo,
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
  RevenueAmount,
  CostAmount,
  cast(RevenueAmount + CostAmount as decimal(10,2)) as GP1
from
  cte
"""
)

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
df_selection = df_selection.fillna(value= 'NaN').replace('', 'NaN')

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'IG'  AND EntityCode NOT IN ('FR2')").saveAsTable("globaltransactions")
