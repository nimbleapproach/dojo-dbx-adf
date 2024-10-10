# Databricks notebook source
# Importing Libraries
import os

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------


spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_trannsaction_ig_staging as (
  
  --- sales invoice
  SELECT
    -- local_code
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) sales_order_date,
    to_date(sih.PostingDate) AS posting_date,
    
    coalesce(sih.OrderNo_, so.SalesOrderID, 'NaN')
    sih.MSPUsageHeaderBizTalkGuid as biztalkguid,
    '' as cogs_account_no,
    trim(
      (
        concat(
          regexp_replace(it.Description, 'NaN', ''),
          regexp_replace(it.Description2, 'NaN', ''),
          regexp_replace(it.Description3, 'NaN', ''),
          regexp_replace(it.Description4, 'NaN', '')
        )
      )
    ) AS description,
    sil.DocumentNo_ AS doc_no,
    sla.Doc_No_Occurrence as doc_no_occurrence,
    sil.Gen_Bus_PostingGroup as gen_bus_postinggroup,
    sil.LineNo_ AS line_no,
    '' as order_line_no,
    '' as sales_credit_memo_line_no           ,
    '' as sales_invoice_line_no               ,
    '' as sales_account_no              ,
    '' as shortcut_dimension1_code            ,
    '' as shortcut_dimension2_code            ,
    sil.SID as sid                                 ,
    sih.Sys_DatabaseName as sys_databasename              ,
    '' as sys_rownumber         ,
    sil.Type as type                                ,
    '' as vatprod_postinggroup          ,
    '' as amount_local_currency               ,
    '' as amount_EUR               ,
    '' as amount_including_vat_local_currency ,
    '' as amount_including_vat_EUR            ,
    '' as cost_amount_local_currency          ,
    '' as cost_amount_EUR              ,
    '' as quantity             ,
    '' as total_cost_purchase_currency        ,
    '' as total_cost_EUR               ,
    '' as unit_cost_local_currency            ,
    '' as unit_cost_purchase_currency         ,
    '' as unit_cost_EUR                ,
    '' as unit_price                         
  FROM
  silver_dev.igsql03.sales_invoice_header sih
  LEFT JOIN silver_dev.igsql03.sales_invoice_line sil ON sih.No_ = sil.DocumentNo_
  AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
  AND sih.Sys_Silver_IsCurrent = true
  AND sil.Sys_Silver_IsCurrent = true
  LEFT JOIN silver_dev.igsql03.item it ON sil.No_ = it.No_
    AND sil.Sys_DatabaseName = it.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
  LEFT JOIN (
      SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_dev.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven ON it.GlobalDimension1Code = ven.Code
    AND it.Sys_DatabaseName = ven.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_dev.igsql03.dimension_set_entry dim ON sih.DimensionSetID = dim.DimensionSetID
    and dim.DimensionCode = 'RPTREGION'
    AND sih.Sys_DatabaseName = dim.Sys_DatabaseName
    AND dim.Sys_Silver_IsCurrent = true

    LEFT JOIN silver_dev.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
    LEFT JOIN gold_dev.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    LEFT JOIN silver_dev.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND rg.Entity = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
    LEFT JOIN (
      select
        sla.Sys_DatabaseName,
        sla.DocumentNo_ As SalesOrderID,
        sha.DocumentDate AS SalesOrderDate,
        sla.LineNo_ AS SalesOrderLineNo,
        sla.No_ as SalesOrderItemID
      from
        silver_dev.igsql03.sales_line_archive as sla
        inner join (
          select
            No_,
            Sys_DatabaseName,
            max(DocumentDate) DocumentDate,
            max(VersionNo_) VersionNo_
          from
            silver_dev.igsql03.sales_header_archive
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
        silver_dev.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven1 ON sil.ShortcutDimension1Code = ven1.Code
    AND sil.Sys_DatabaseName = ven1.Sys_DatabaseName
    LEFT JOIN silver_dev.igsql03.resource res ON sil.No_ = res.No_
    AND sil.Sys_DatabaseName = res.Sys_DatabaseName
    AND res.Sys_Silver_IsCurrent = true
    LEFT JOIN (
      SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_dev.igsql03.dimension_value
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
      AND UPPER(sil.No_) NOT LIKE '%MARKETING%'
    )
    and sil.Type <> 0
  UNION all
    --- SALES CR MEMO
  SELECT
    sil.SID AS SID,
    'IG' AS GroupEntityCode,
   entity.TagetikEntityCode  AS EntityCode,
     sil.Sys_DatabaseName,
    dim.DimensionValueCode as Reseller_Country_RegionCode,
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
      AND left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI', 'AT')  THEN 'EUR'
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
    silver_dev.igsql03.sales_cr_memo_header sih
    INNER JOIN silver_dev.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_dev.igsql03.item it ON sil.No_ = it.No_
    AND sil.Sys_DatabaseName = it.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    LEFT JOIN (
      SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_dev.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven ON it.GlobalDimension1Code = ven.Code
    AND it.Sys_DatabaseName = ven.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
    
    LEFT JOIN silver_dev.igsql03.dimension_set_entry dim ON sih.DimensionSetID = dim.DimensionSetID
    and dim.DimensionCode = 'RPTREGION'
    AND sih.Sys_DatabaseName = dim.Sys_DatabaseName
    AND dim.Sys_Silver_IsCurrent = true

    LEFT JOIN silver_dev.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
    
    LEFT JOIN gold_dev.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    --Comment by MS (30/01/2024) - Start
    LEFT JOIN silver_dev.masterdata.resellergroups AS rg
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
        silver_dev.igsql03.sales_line_archive as sla
        inner join (
          select
            No_,
            Sys_DatabaseName,
            max(DocumentDate) DocumentDate,
            max(VersionNo_) VersionNo_
          from
            silver_dev.igsql03.sales_header_archive
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
        silver_dev.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven1 ON sil.ShortcutDimension1Code = ven1.Code
    AND sil.Sys_DatabaseName = ven1.Sys_DatabaseName
    LEFT JOIN silver_dev.igsql03.resource res ON sil.No_ = res.No_
    AND sil.Sys_DatabaseName = res.Sys_DatabaseName
    AND res.Sys_Silver_IsCurrent = true
    LEFT JOIN (
      SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_dev.igsql03.dimension_value
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
      AND UPPER(sil.No_) NOT LIKE '%MARKETING%'
    )
    and sil.Type <> 0 

""")
