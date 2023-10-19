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
  sil.SID AS SID,
  'NAV' AS SourceSystemName,
  'NAV'AS GroupEntityCode,
  -- entity.TagetikEntityCode AS GroupEntityCode,
  'Invoice' AS DocumentType,
  to_date(sih.PostingDate) AS TransactionDate,
  so.SalesOrderID,
  so.SalesOrderDate,
  so.SalesOrderItemID,
  CAST(sil.Amount AS DECIMAL)  AS RevenueAmount,
  Case
    sih.CurrencyCode = 'NaN'
    WHEN left(entity.TagetikEntityCode, 2) = 'CH' THEN 'CHF'
    WHEN left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
    WHEN left(entity.TagetikEntityCode, 2) = 'UK' THEN 'GBP'
    WHEN left(entity.TagetikEntityCode, 2) = 'SE' THEN 'SEK'
    WHEN left(entity.TagetikEntityCode, 2) = 'NO' THEN 'NOK'
    WHEN left(entity.TagetikEntityCode, 2) = 'DK' THEN 'DKK'
    ELSE sih.CurrencyCode
  END AS CurrencyCode,
  it.No_ AS SKU,
  concat_ws(
    ' ',
    it.Description,
    it.Description2,
    it.Description3,
    it.Description4
  ) AS Description,
  it.ProductType AS ProductTypeInternal,
  datanowarr.Product_Type AS ProductTypeMaster,
  '' AS ProductSubtype,
  datanowarr.Commitment_Duration2 AS CommitmentDuration,
  datanowarr.Billing_Frequency AS BillingFrequency,
  datanowarr.Consumption_Model AS ConsumptionModel,
  ven.Code AS VendorCode,
  ven.Name AS VendorName,
  '' AS VendorGeography,
  to_date('1900-01-01', 'yyyy-MM-dd') AS VendorStartDate,
  cu.No_ AS ResellerCode,
  concat_ws(' ', cu.Name, cu.Name2) AS ResellerNameInternal,
  to_date(cu.Createdon) AS ResellerStartDate,
  rg.ResellerGroupCode AS ResellerGroupCode,
  rg.ResellerGroupName AS ResellerGroupName,
  cu.Country_RegionCode AS ResellerGeographyInternal,
  to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate,
  rg.ResellerName AS ResellerNameMaster,
  rg.Entity AS IGEntityOfReseller,
  '' AS ResellerGeographyMaster,
  rg.ResellerID AS ResellerID
FROM
  silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
  INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil ON sih.No_ = sil.DocumentNo_
  AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
  AND sih.Sys_Silver_IsCurrent = true
  AND sil.Sys_Silver_IsCurrent = true
  INNER JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
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
  ) ven ON sil.ShortcutDimension1Code = ven.Code
  AND sil.Sys_DatabaseName = ven.Sys_DatabaseName
  AND sil.Sys_Silver_IsCurrent = true
  LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
  AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
  AND cu.Sys_Silver_IsCurrent = true
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON concat(
    RIGHT(sih.Sys_DatabaseName, 2),
    sih.`Sell-toCustomerNo_`
  ) = concat(rg.Entity, rg.ResellerID)
  AND rg.Sys_Silver_IsCurrent = TRUE
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.datanowarr ON it.No_ = datanowarr.sku
  and lower(ven.Name) = lower(datanowarr.Vendor_Name)
  AND datanowarr.Sys_Silver_IsCurrent = true
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
UNION All
  --- Sales credit memo
SELECT
  sil.SID AS SID,
  'NAV' AS SourceSystemName,
  'NAV'AS GroupEntityCode,
  -- entity.TagetikEntityCode AS GroupEntityCode,
  'Sales Credit Memo' AS DocumentType,
  to_date(sih.PostingDate) AS TransactionDate,
  so.SalesOrderID,
  so.SalesOrderDate,
  so.SalesOrderItemID,
  CAST(sil.Amount * (-1)AS DECIMAL) AS RevenueAmount,
  Case
    sih.CurrencyCode = 'NaN'
    WHEN left(entity.TagetikEntityCode, 2) = 'CH' THEN 'CHF'
    WHEN left(entity.TagetikEntityCode, 2) IN('DE', 'FR', 'NL', 'FI') THEN 'EUR'
    WHEN left(entity.TagetikEntityCode, 2) = 'UK' THEN 'GBP'
    WHEN left(entity.TagetikEntityCode, 2) = 'SE' THEN 'SEK'
    WHEN left(entity.TagetikEntityCode, 2) = 'NO' THEN 'NOK'
    WHEN left(entity.TagetikEntityCode, 2) = 'DK' THEN 'DKK'
    ELSE sih.CurrencyCode
  END AS CurrencyCode,
  it.No_ AS SKU,
  concat_ws(
    ' ',
    it.Description,
    it.Description2,
    it.Description3,
    it.Description4
  ) AS Description,
  it.ProductType AS ProductTypeInternal,
  datanowarr.Product_Type AS ProductTypeMaster,
  '' AS ProductSubtype,
  datanowarr.Commitment_Duration2 AS CommitmentDuration,
  datanowarr.Billing_Frequency AS BillingFrequency,
  datanowarr.Consumption_Model AS ConsumptionModel,
  ven.Code AS VendorCode,
  ven.Name AS VendorName,
  '' AS VendorGeography,
  to_date('1900-01-01', 'yyyy-MM-dd') AS VendorStartDate,
  cu.No_ AS ResellerCode,
  concat_ws(' ', cu.Name, cu.Name2) AS ResellerNameInternal,
  to_date(cu.Createdon) AS ResellerStartDate,
  rg.ResellerGroupCode AS ResellerGroupCode,
  rg.ResellerGroupName AS ResellerGroupName,
  cu.Country_RegionCode AS ResellerGeographyInternal,
  to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate,
  rg.ResellerName AS ResellerNameMaster,
  rg.Entity AS IGEntityOfReseller,
  '' AS ResellerGeographyMaster,
  rg.ResellerID AS ResellerID
FROM
  silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
  INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
  AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
  AND sih.Sys_Silver_IsCurrent = true
  AND sil.Sys_Silver_IsCurrent = true
  INNER JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
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
  ) ven ON sil.ShortcutDimension1Code = ven.Code
  AND sil.Sys_DatabaseName = ven.Sys_DatabaseName
  AND sil.Sys_Silver_IsCurrent = true
  LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
  AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
  AND cu.Sys_Silver_IsCurrent = true
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg ON concat(
    RIGHT(sih.Sys_DatabaseName, 2),
    sih.`Sell-toCustomerNo_`
  ) = concat(rg.Entity, rg.ResellerID)
  AND rg.Sys_Silver_IsCurrent = TRUE
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.datanowarr ON it.No_ = datanowarr.sku
  and lower(ven.Name) = lower(datanowarr.Vendor_Name)
  AND datanowarr.Sys_Silver_IsCurrent = true
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

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NAV'").saveAsTable("globaltransactions")
