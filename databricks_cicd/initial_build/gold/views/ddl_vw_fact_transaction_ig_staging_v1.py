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

# REMOVE ONCE SOLUTION IS LIVE
if ENVIRONMENT == 'dev':
    spark.sql(f"""
              DROP VIEW IF {catalog}.{schema}.vw_fact_sales_transaction_ig_staging
              """)

# COMMAND ----------

spark.sql(
    f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_transaction_ig_staging as (
  
  --- sales invoice
  SELECT
    ss.source_system_pk as source_system_fk,
    -- local_code
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) sales_order_date,
    to_date(sih.PostingDate) AS posting_date,
    sih.MSPUsageHeaderBizTalkGuid as biztalk_guid,
    --START add these to table
    coalesce(sih.OrderNo_, so.SalesOrderID, 'NaN') AS sales_order_Id,
    coalesce(sil.No_, so.SalesOrderItemID, 'NaN') AS sales_order_item_Id,
    coalesce(it.No_, sil.No_, 'NaN') AS SKU_internal_Id,
   coalesce(it.ProductType, 'NaN') AS SKU_type_internal,
    coalesce(ven1.Code, ven.Code, ven2.Code, 'NaN') AS vendor_code,
    coalesce(ven1.Name, ven.Name, ven2.Name, 'NaN') AS vendor_name_internal,
    coalesce(sih.`Sell-toCustomerNo_`, 'NaN') AS reseller_code_internal_Id,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS reseller_name_internal,    
    dim.DimensionValueCode as reseller_country_region_code,
    --END add these to table
    
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
    -- so.Doc_No_Occurrence as doc_no_occurrence,
    sil.Gen_Bus_PostingGroup as gen_bus_postinggroup,
    sil.LineNo_ AS line_no,
    '' as order_line_no,
    '' as sales_credit_memo_line_no           ,
    '' as sales_invoice_line_no               ,
    '' as sales_account_no              ,
    it.GlobalDimension1Code as item_global_dimension1_code            ,
    res.GlobalDimension1Code as resource_global_dimension1_code            ,
    sil.ShortcutDimension1Code as item_dimension1_code            ,
    sil.SID as sid                                 ,
    sih.Sys_DatabaseName as sys_databasename              ,
    '' as sys_rownumber         ,
    sil.Type as type                                ,
    SIL.Gen_Bus_PostingGroup as vatprod_postinggroup          ,
    CAST(
      case
        when sih.CurrencyFactor > 0 then Amount / sih.CurrencyFactor
        else Amount
      end AS DECIMAL(10, 2)
    ) as amount_local_currency               ,
    '' as amount_EUR               ,
    '' as amount_including_vat_local_currency ,
    '' as amount_including_vat_EUR            ,
    CAST(
      case
        when sil.Quantity > 0 then sil.UnitCostLCY * sil.Quantity *(-1)
        else 0
      end AS DECIMAL(10, 2)
    ) as cost_amount_local_currency          ,
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
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sih.Sys_DatabaseName, 2)
  --igsql03.sales_invoice_line should repoint to orion.line_item
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
    ss.source_system_pk as source_system_fk,
    -- local_code
    to_date(coalesce(so.SalesOrderDate, '1900-01-01')) sales_order_date,
    to_date(sih.PostingDate) AS posting_date,
    sih.MSPUsageHeaderBizTalkGuid as biztalk_guid,
    --START add these to table
    coalesce(sil.OrderNo_, so.SalesOrderID, 'NaN') AS SalesOrderID,
    coalesce(sil.No_, so.SalesOrderItemID, 'NaN') AS sales_order_item_Id,
    coalesce(it.No_, sil.No_, 'NaN') AS SKU_internal_Id,
   coalesce(it.ProductType, 'NaN') AS SKU_type_internal,
    coalesce(ven1.Code, ven.Code, ven2.Code, 'NaN') AS vendor_code,
    coalesce(ven1.Name, ven.Name, ven2.Name, 'NaN') AS vendor_name_internal,
    coalesce(sih.`Sell-toCustomerNo_`, 'NaN') AS reseller_code_internal_Id,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS reseller_name_internal,
    dim.DimensionValueCode as reseller_country_region_code,
    --END add these to table
    
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
    -- so.Doc_No_Occurrence as doc_no_occurrence,
    sil.Gen_Bus_PostingGroup as gen_bus_postinggroup,
    sil.LineNo_ AS line_no,
    '' as order_line_no,
    '' as sales_credit_memo_line_no           ,
    '' as sales_invoice_line_no               ,
    '' as sales_account_no              ,
    it.GlobalDimension1Code as item_global_dimension1_code            ,
    res.GlobalDimension1Code as resource_global_dimension1_code            ,
    sil.ShortcutDimension1Code as item_dimension1_code            ,
    sil.SID as sid                                 ,
    sih.Sys_DatabaseName as sys_databasename              ,
    '' as sys_rownumber         ,
    sil.Type as type                                ,
    
    SIL.Gen_Bus_PostingGroup as vatprod_postinggroup          ,
    CAST(
      case
        when sih.CurrencyFactor > 0 then Amount / sih.CurrencyFactor
        else Amount
      end AS DECIMAL(10, 2)
    ) as amount_local_currency        ,
    '' as amount_EUR               ,
    '' as amount_including_vat_local_currency ,
    '' as amount_including_vat_EUR            ,
    CAST(
      case
        when sil.Quantity > 0 then sil.UnitCostLCY * sil.Quantity *(-1)
        else 0
      end AS DECIMAL(10, 2)
    ) as cost_amount_local_currency          ,
    '' as cost_amount_EUR              ,
    '' as quantity             ,
    '' as total_cost_purchase_currency        ,
    '' as total_cost_EUR               ,
    '' as unit_cost_local_currency            ,
    '' as unit_cost_purchase_currency         ,
    '' as unit_cost_EUR                ,
    '' as unit_price                      
  FROM
    silver_dev.igsql03.sales_cr_memo_header sih
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sih.Sys_DatabaseName, 2)
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
)
"""
)
