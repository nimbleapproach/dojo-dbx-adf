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

with msp_usage as (
  SELECT
    concat(right(msp_h.Sys_DatabaseName, 2), '1') as EntityCode,
    msp_h.BizTalkGuid,
    cast(msp_h.DocumentDate as date) as DocumentDate,
    case
      when msp_h.CreditMemo = '1' THEN msp_h.SalesCreditMemoNo_
      else msp_h.SalesInvoiceNo_
    end as DocumentNo,
    case
      when msp_h.CreditMemo = '1' THEN msp_l.SalesCreditMemoLineNo_
      else msp_l.SalesInvoiceLineNo_
    end as DocumentLineNo,
    Case
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
      ELSE msp_l.PurchaseCurrencyCode
    END AS CurrencyCode,
    /*[yz] 2024-03-22 add local currency filed to convert purchase currency back from EUR to LCY*/
    Case
      WHEN  right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
        END AS LocalCurrencyCode,
    msp_h.VENDORDimensionValue,
    msp_l.LineNo_ as MSPLineNo,
    msp_l.ItemNo_,
    msp_l.Quantity,
    case
      when msp_h.CreditMemo = '1' THEN msp_l.TotalPrice *(-1)
      else msp_l.TotalPrice
    end as TotalPrice,

    cast (
      case
        when msp_l.PurchaseCurrencyCode = 'NaN' then msp_l.TotalCostPCY
        else msp_l.TotalCostPCY / fx.Period_FX_rate * fx2.Period_FX_rate
      end *(-1) as decimal(10, 2)
    ) as TotalCostLCY
  FROM
    silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header as msp_h
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.inf_msp_usage_line AS msp_l on msp_h.BizTalkGuid = msp_l.BizTalkGuid
    and msp_h.Sys_DatabaseName = msp_l.Sys_DatabaseName
    and msp_h.Sys_Silver_IsCurrent = 1
    and msp_l.Sys_Silver_IsCurrent = 1
    left join (
      SELECT
        DISTINCT Calendar_Year,
        Month,
        Currency,
        Period_FX_rate
      FROM
        gold_{ENVIRONMENT}.obt.exchange_rate
      WHERE
        ScenarioGroup = 'Actual'
    ) fx on msp_l.PurchaseCurrencyCode = fx.Currency
    and year(msp_h.DocumentDate) = fx.Calendar_Year
    and month(msp_h.DocumentDate) = fx.Month
    left join (
      SELECT
        DISTINCT Calendar_Year,
        Month,
        Currency,
        Period_FX_rate
      FROM
        gold_{ENVIRONMENT}.obt.exchange_rate
      WHERE
        ScenarioGroup = 'Actual'
    ) fx2 on Case
      WHEN  right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
        END = fx2.Currency
    and year(msp_h.DocumentDate) = fx2.Calendar_Year
    and month(msp_h.DocumentDate) = fx2.Month
),
cte as (
  --- sales invoice
  SELECT
    sil.SID AS SID,
    'IG' AS GroupEntityCode,
  --- [yz]15.03.2024 split AT out from DE and BE from NL

  -- case when cu.Country_RegionCode = 'BE' AND entity.TagetikEntityCode = 'NL1' THEN 'BE1'
  --     when cu.Country_RegionCode = 'AT' AND entity.TagetikEntityCode = 'DE1' THEN 'AT1'
  --     ELSE entity.TagetikEntityCode END AS EntityCode,
   entity.TagetikEntityCode  AS EntityCode,
    cu.Country_RegionCode as Reseller_Country_RegionCode,
    sil.DocumentNo_ AS DocumentNo,
    SIL.Gen_Bus_PostingGroup,
    sil.LineNo_ AS LineNo,
    -- Comment by YZ (26/01/2023)-Start
    -- add MSPBizTalkGuid as key to join on MSP usage header and line to resolve MSP cost issue
    sih.MSPUsageHeaderBizTalkGuid,
    sil.Type AS Type,
    sil.Description AS SalesInvoiceDescription,
    to_date(sih.PostingDate) AS TransactionDate,
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) as SalesOrderDate,
    --Comment by MS (15/12/2023) - Start
    --Bring sales order no and sales order item no from main sales tables if it can't find then bring from archive tables
    --coalesce(so.SalesOrderID, 'NaN') AS SalesOrderID,
    --coalesce(so.SalesOrderItemID, 'NaN') AS SalesOrderItemID,
    --Comment by MS (15/12/2023) - End
    coalesce(sih.OrderNo_, so.SalesOrderID, 'NaN') AS SalesOrderID,
    coalesce(sil.No_, so.SalesOrderItemID, 'NaN') AS SalesOrderItemID,
    coalesce(it.No_, sil.No_, 'NaN') AS SKUInternal,
    sil.Gen_Prod_PostingGroup,
    -- coalesce(datanowarr.SKU, 'NaN') AS SKUMaster,
    trim(
      (
        concat(
          regexp_replace(it.Description, 'NaN', ''),
          regexp_replace(it.Description2, 'NaN', ''),
          regexp_replace(it.Description3, 'NaN', ''),
          regexp_replace(it.Description4, 'NaN', '')
        )
      )
    ) AS Description,
   coalesce(it.ProductType, 'NaN') AS ProductTypeInternal,
   /*coalesce(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
    coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    coalesce(ven.Code, 'NaN') AS VendorCode,
    coalesce(ven.Name, 'NaN') AS VendorNameInternal,*/
    --first go through item and dimension value
    --second go straight to dimension value
    coalesce(ven1.Code, ven.Code, ven2.Code, 'NaN') AS VendorCode,
    coalesce(ven1.Name, ven.Name, ven2.Name, 'NaN') AS VendorNameInternal,
    -- coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
    '' AS VendorGeography,
    to_date('1900-01-01', 'yyyy-MM-dd') AS VendorStartDate,
    coalesce(cu.No_, 'NaN') AS ResellerCode,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS ResellerNameInternal,
    cu.Country_RegionCode AS ResellerGeographyInternal,
    to_date(cu.Createdon) AS ResellerStartDate,
    coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
    coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
    to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate,
    Case
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'CH' THEN 'CHF'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'UK' THEN 'GBP'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'SE' THEN 'SEK'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'NO' THEN 'NOK'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'DK' THEN 'DKK'
      ELSE sih.CurrencyCode
    END AS CurrencyCode,
    sih.CurrencyFactor,
    CAST(
      case
        when sih.CurrencyFactor > 0 then Amount / sih.CurrencyFactor
        else Amount
      end AS DECIMAL(10, 2)
    ) AS RevenueAmount,
    CAST(
      case
        when sil.Quantity > 0 then sil.UnitCostLCY * sil.Quantity *(-1)
        else 0
      end AS DECIMAL(10, 2)
    ) as CostAmount
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
    /*
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON concat(
      RIGHT(sih.Sys_DatabaseName, 2),
      sih.`Sell-toCustomerNo_`
    ) = concat(rg.Entity, rg.ResellerID)
    AND rg.Sys_Silver_IsCurrent = true -- LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr ON it.No_ = datanowarr.sku
    */
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    --Comment by MS (30/01/2024) - Start
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND rg.Entity = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
    --Comment by MS (30/01/2024) - End
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
    ) ven1 ON sil.ShortcutDimension1Code = ven1.Code
    AND sil.Sys_DatabaseName = ven1.Sys_DatabaseName
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.resource res ON sil.No_ = res.No_
    AND sil.Sys_DatabaseName = res.Sys_DatabaseName
    AND res.Sys_Silver_IsCurrent = true
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
    ) ven2 ON res.GlobalDimension1Code = ven2.Code
    AND res.Sys_DatabaseName = ven2.Sys_DatabaseName
  WHERE
    (
      UPPER(sil.No_) NOT LIKE 'PORTO'
      AND UPPER(sil.No_) NOT LIKE 'VERSAND%'
      AND UPPER(sil.No_) NOT LIKE 'SHIP%'
      AND UPPER(sil.No_) NOT LIKE 'TRANS%'
      AND UPPER(sil.No_) NOT LIKE 'POST%'
      AND UPPER(sil.No_) NOT LIKE 'FREI%'
      AND UPPER(sil.No_) NOT LIKE 'FRACHT%'
      AND UPPER(sil.No_) NOT LIKE 'EXP%'
    )
    AND sil.Gen_Bus_PostingGroup not like 'IC%'
    and sil.Type <> 0 -- filter out type 0 because it is only placeholder lines (yzc)
  UNION all
    --- SALES CR MEMO
  SELECT
    sil.SID AS SID,
    'IG' AS GroupEntityCode,
  -- case when cu.Country_RegionCode = 'BE' AND entity.TagetikEntityCode = 'NL1' THEN 'BE1'
  --     when cu.Country_RegionCode = 'AT' AND entity.TagetikEntityCode = 'DE1' THEN 'AT1'
  --     ELSE entity.TagetikEntityCode END AS EntityCode,
   entity.TagetikEntityCode  AS EntityCode,
    cu.Country_RegionCode as Reseller_Country_RegionCode,
    sil.DocumentNo_ AS DocumentNo,
    SIL.Gen_Bus_PostingGroup,
    sil.LineNo_ AS LineNo,
    sih.MSPUsageHeaderBizTalkGuid,
    sil.Type AS Type,
    sil.Description AS SalesInvoiceDescription,
    to_date(sih.PostingDate) AS TransactionDate,
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) as SalesOrderDate,
    coalesce(so.SalesOrderID, 'NaN') AS SalesOrderID,
    --coalesce(so.SalesOrderItemID, 'NaN') AS SalesOrderItemID,
    coalesce(sil.No_, so.SalesOrderItemID, 'NaN') AS SalesOrderItemID,
    coalesce(it.No_, sil.No_, 'NaN') AS SKUInternal,
       sil.Gen_Prod_PostingGroup,
    -- coalesce(datanowarr.SKU, 'NaN')  AS SKUMaster,
    trim(
      concat(
        regexp_replace(it.Description, 'NaN', ''),
        regexp_replace(it.Description2, 'NaN', ''),
        regexp_replace(it.Description3, 'NaN', ''),
        regexp_replace(it.Description4, 'NaN', '')
      )
    ) AS Description,
    coalesce(it.ProductType, 'NaN') AS ProductTypeInternal,
    -- coalesce(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
    -- coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    -- coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    -- coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    -- coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    coalesce(ven1.Code, ven.Code, ven2.Code, 'NaN') AS VendorCode,
    coalesce(ven1.Name, ven.Name, ven2.Name, 'NaN') AS VendorNameInternal,
    -- coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
    '' AS VendorGeography,
    to_date('1900-01-01', 'yyyy-MM-dd') AS VendorStartDate,
    coalesce(cu.No_, 'NaN') AS ResellerCode,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS ResellerNameInternal,
    cu.Country_RegionCode AS ResellerGeographyInternal,
    to_date(cu.Createdon) AS ResellerStartDate,
    coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
    coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
    to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate,
    Case
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'CH' THEN 'CHF'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'UK' THEN 'GBP'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'SE' THEN 'SEK'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'NO' THEN 'NOK'
      WHEN sih.CurrencyCode = 'NaN'
      AND left(entity.TagetikEntityCode, 2) = 'DK' THEN 'DKK'
      ELSE sih.CurrencyCode
    END AS CurrencyCode,
    sih.CurrencyFactor,
    CAST(
      (
        case
          when sih.CurrencyFactor > 0 then Amount / sih.CurrencyFactor
          else Amount
        end
      ) *(-1) AS DECIMAL(10, 2)
    ) AS RevenueAmount,
    CAST(
      case
        when sil.Quantity > 0 then sil.UnitCostLCY * sil.Quantity
        else 0
      end AS DECIMAL(10, 2)
    ) as CostAmount
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
    /*
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON concat(
      RIGHT(sih.Sys_DatabaseName, 2),
      sih.`Sell-toCustomerNo_`
    ) = concat(rg.Entity, rg.ResellerID)
    AND rg.Sys_Silver_IsCurrent = TRUE -- LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr ON it.No_ = datanowarr.sku
    */
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    --Comment by MS (30/01/2024) - Start
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND rg.Entity = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
    --Comment by MS (30/01/2024) - End
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
    ) ven1 ON sil.ShortcutDimension1Code = ven1.Code
    AND sil.Sys_DatabaseName = ven1.Sys_DatabaseName
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.resource res ON sil.No_ = res.No_
    AND sil.Sys_DatabaseName = res.Sys_DatabaseName
    AND res.Sys_Silver_IsCurrent = true
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
    ) ven2 ON res.GlobalDimension1Code = ven2.Code
    AND res.Sys_DatabaseName = ven2.Sys_DatabaseName
  WHERE
    (
      UPPER(sil.No_) NOT LIKE 'PORTO'
      AND UPPER(sil.No_) NOT LIKE 'VERSAND%'
      AND UPPER(sil.No_) NOT LIKE 'SHIP%'
      AND UPPER(sil.No_) NOT LIKE 'TRANS%'
      AND UPPER(sil.No_) NOT LIKE 'POST%'
      AND UPPER(sil.No_) NOT LIKE 'FREI%'
      AND UPPER(sil.No_) NOT LIKE 'FRACHT%'
      AND UPPER(sil.No_) NOT LIKE 'EXP%'
    )
  AND sil.Gen_Bus_PostingGroup not like 'IC%'
    and sil.Type <> 0 -- filter out type 0 because it is only placeholder lines (yzc)

) 

select
  cte.GroupEntityCode,
    --- [yz]22.03.2024 Add country split here after the msp usage join
 case when cte.Reseller_Country_RegionCode = 'BE' AND cte.EntityCode = 'NL1' THEN 'BE1'
     when cte.Reseller_Country_RegionCode = 'AT' AND cte.VendorCode NOT LIKE '%SOW%' AND cte.EntityCode = 'DE1' THEN 'AT1'     --- [yz]08.04.2024 n-able should be excluded from AT/DE split logic
     ELSE cte.EntityCode END AS EntityCode,
  cte.DocumentNo,
  cte.LineNo,
  cte.Gen_Bus_PostingGroup,
  cte.MSPUsageHeaderBizTalkGuid,
  cte.Type,
  cte.SalesInvoiceDescription,
  cte.TransactionDate,
  cte.SalesOrderDate,
  cte.SalesOrderID,
  cte.SalesOrderItemID,
  case
    when msp_usage.ItemNo_ is null then cte.SKUInternal
    else msp_usage.ItemNo_
  end as SKUInternal,
 Gen_Prod_PostingGroup,
  coalesce(datanowarr.SKU, 'NaN') AS SKUMaster,
  cte.Description,
  cte.ProductTypeInternal,
  coalesce(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
  coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
  coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
  coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
  coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
  cte.VendorCode,
  cte.VendorNameInternal,
  coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
  cte.EntityCode as VendorGeography,
  case
    when VendorStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, cte.VendorCode)
    else VendorStartDate
  end as VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, ResellerCode)
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
        when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, ResellerCode)
        else ResellerStartDate
      end
    )
    else (
      case
        when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, ResellerGroupName)
        else min(ResellerStartDate) OVER(PARTITION BY cte.EntityCode, ResellerGroupName)
      end
    )
  end AS ResellerGroupStartDate,
  cte.CurrencyCode,
  -- cte.CurrencyFactor,
  cast (
    case WHEN msp_usage.TotalPrice is null THEN RevenueAmount
          when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor > 0 then msp_usage.TotalPrice / cte.CurrencyFactor
          when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor = 0  then msp_usage.TotalPrice
      else RevenueAmount
    end as decimal(10, 2)
  ) as RevenueAmount,
  cast(case
    when msp_usage.BizTalkGuid is null then CostAmount
    else msp_usage.TotalCostLCY
  end as decimal(10, 2)) as CostAmount,
 
  cast((case WHEN msp_usage.TotalPrice is null THEN RevenueAmount
          when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor > 0 then msp_usage.TotalPrice / cte.CurrencyFactor
          when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor = 0  then msp_usage.TotalPrice
      else RevenueAmount
    end +
  case
    when msp_usage.BizTalkGuid is null then CostAmount
    else msp_usage.TotalCostLCY
  end) as decimal(10, 2))as GP1
from
  cte
  left join msp_usage on cte.EntityCode = msp_usage.EntityCode
  and cte.MSPUsageHeaderBizTalkGuid = msp_usage.BizTalkGuid
  and cte.DocumentNo = msp_usage.DocumentNo
  and cte.LineNo = msp_usage.DocumentLineNo
  LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr ON 
  case
    when msp_usage.ItemNo_ is null then cte.SKUInternal
    else msp_usage.ItemNo_
  end = datanowarr.sku
  WHERE   CASE WHEN cte.EntityCode = 'CH1' AND cte.Gen_Bus_PostingGroup  LIKE '%IC%' THEN 0
            WHEN cte.EntityCode ='FR1' AND cte.Gen_Bus_PostingGroup  LIKE '%-IC%' THEN 0
            ELSE 1
            END =1


"""
)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")
df_infinigate = spark.read.table(
    f"gold_{ENVIRONMENT}.obt.infinigate_globaltransactions"
)

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_infinigate.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [
    col(column) for column in intersection_columns if column not in ["SID"]
]

# COMMAND ----------

df_selection = df_infinigate.select(selection_columns)
df_selection = df_selection.fillna(value="NaN").replace("", "NaN")

# COMMAND ----------

df_selection.write.mode("overwrite").option(
    "replaceWhere", "GroupEntityCode = 'IG'  AND EntityCode NOT IN ('FR2')"
).saveAsTable("globaltransactions")
