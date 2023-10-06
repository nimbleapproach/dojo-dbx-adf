# Databricks notebook source
# DBTITLE 1,Silver to Gold Navision
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace VIEW gold_dev.obt.infinigate AS
# MAGIC
# MAGIC --Sales Invoice Header/Line
# MAGIC SELECT
# MAGIC sil.SID AS SID
# MAGIC ,'IG' AS GroupEntityCode
# MAGIC ,'Invoice' AS DocumentType
# MAGIC ,sih.PostingDate AS TransactionDate
# MAGIC ,RIGHT(sih.Sys_DatabaseName,2) AS InfinigateEntity
# MAGIC ,sil.Amount AS RevenueAmount
# MAGIC ,sih.CurrencyCode
# MAGIC ,it.No_ AS SKU
# MAGIC ,concat_ws(' ',it.Description,it.Description2,it.Description3,it.Description4) AS Description
# MAGIC ,it.ProductType AS ProductTypeInternal
# MAGIC ,sk.Product_Type AS ProductTypeMaster
# MAGIC ,'' AS ProductSubtype
# MAGIC ,sk.Commitment_Duration AS CommitmentDuration
# MAGIC ,sk.Billing_Frequency AS BillingFrequency
# MAGIC ,sk.Consumption_Model AS ConsumptionModel
# MAGIC ,ven.Code AS VendorCode
# MAGIC ,ven.Name AS VendorName
# MAGIC ,'' AS VendorGeography
# MAGIC ,'' AS VendorStartDate
# MAGIC ,cu.No_ AS ResellerCode
# MAGIC ,concat_ws(' ',cu.Name,cu.Name2) AS ResellerNameInternal
# MAGIC ,cu.Createdon AS ResellerStartDate
# MAGIC ,rg.ResellerGroupCode AS ResellerGroupCode
# MAGIC ,rg.ResellerGroupName AS ResellerGroupName
# MAGIC ,cu.Country_RegionCode  AS ResellerGeographyInternal
# MAGIC ,'' AS ResellerGroupStartDate
# MAGIC ,rg.ResellerName AS ResellerNameMaster
# MAGIC ,rg.Entity AS IGEntityOfReseller
# MAGIC ,'' AS ResellerGeographyMaster
# MAGIC ,rg.ResellerID AS ResellerID
# MAGIC FROM 
# MAGIC   silver_dev.igsql03.sales_invoice_header sih
# MAGIC INNER JOIN 
# MAGIC   silver_dev.igsql03.sales_invoice_line sil
# MAGIC ON
# MAGIC   sih.No_ = sil.DocumentNo_
# MAGIC AND
# MAGIC   sih.Sys_DatabaseName = sil.Sys_DatabaseName
# MAGIC AND
# MAGIC   sih.Sys_Silver_IsCurrent = true
# MAGIC AND
# MAGIC   sil.Sys_Silver_IsCurrent = true
# MAGIC INNER JOIN 
# MAGIC   silver_dev.igsql03.item it
# MAGIC ON 
# MAGIC   sil.No_ = it.No_
# MAGIC AND
# MAGIC   sil.Sys_DatabaseName = it.Sys_DatabaseName
# MAGIC AND
# MAGIC   it.Sys_Silver_IsCurrent = true
# MAGIC LEFT JOIN
# MAGIC   (
# MAGIC   SELECT
# MAGIC   Code,
# MAGIC   Name,
# MAGIC   Sys_DatabaseName
# MAGIC   FROM silver_dev.igsql03.dimension_value
# MAGIC   WHERE DimensionCode = 'VENDOR'
# MAGIC   AND Sys_Silver_IsCurrent = true
# MAGIC   ) ven
# MAGIC ON 
# MAGIC   sil.ShortcutDimension1Code = ven.Code
# MAGIC AND
# MAGIC   sil.Sys_DatabaseName = ven.Sys_DatabaseName
# MAGIC AND
# MAGIC   sil.Sys_Silver_IsCurrent = true  
# MAGIC LEFT JOIN 
# MAGIC   silver_dev.igsql03.customer cu
# MAGIC ON 
# MAGIC   sih.`Sell-toCustomerNo_` = cu.No_
# MAGIC AND
# MAGIC   sih.Sys_DatabaseName = cu.Sys_DatabaseName
# MAGIC AND
# MAGIC   cu.Sys_Silver_IsCurrent = true
# MAGIC LEFT JOIN 
# MAGIC (select distinct  
# MAGIC InfinigateCompany,
# MAGIC ResellerID,
# MAGIC ResellerGroupCode,
# MAGIC ResellerGroupName,
# MAGIC ResellerName,
# MAGIC Entity
# MAGIC FROM silver_dev.masterdata.reseller_groups) rg
# MAGIC ON 
# MAGIC   concat_ws(RIGHT(sih.Sys_DatabaseName,2), sih.`Sell-toCustomerNo_`) = concat_ws(rg.Entity, rg.ResellerID)
# MAGIC LEFT JOIN
# MAGIC (select SKU, Commitment_Duration, Billing_Frequency, Consumption_Model, Product_Type
# MAGIC from silver_dev.masterdata.sku) sk
# MAGIC ON 
# MAGIC   it.No_ = sk.sku
# MAGIC AND
# MAGIC   it.Sys_Silver_IsCurrent = true
# MAGIC
# MAGIC UNION All
# MAGIC
# MAGIC --Sales Credit Memo Header/Line  
# MAGIC SELECT
# MAGIC sil.Sys_Silver_HashKey AS HashKey
# MAGIC ,'IG' AS GroupEntityCode
# MAGIC ,'Credit' AS DocumentType
# MAGIC ,sih.PostingDate AS TransactionDate
# MAGIC ,RIGHT(sih.Sys_DatabaseName,2) AS InfinigateEntity
# MAGIC ,sil.Amount AS RevenueAmount
# MAGIC ,sih.CurrencyCode
# MAGIC ,it.No_ AS SKU
# MAGIC ,concat_ws(' ',it.Description,it.Description2,it.Description3,it.Description4) AS Description
# MAGIC ,'' AS ProductTypeInternal
# MAGIC ,sk.Product_Type AS ProductTypeMaster
# MAGIC ,'' AS ProductSubtype
# MAGIC ,sk.Commitment_Duration AS CommitmentDuration
# MAGIC ,sk.Billing_Frequency AS BillingFrequency
# MAGIC ,sk.Consumption_Model AS ConsumptionModel
# MAGIC ,ven.Code AS VendorCode
# MAGIC ,ven.Name AS VendorName
# MAGIC ,'' AS VendorGeography
# MAGIC ,'' AS VendorStartDate
# MAGIC ,cu.No_ AS ResellerCode
# MAGIC ,concat_ws(' ',cu.Name,cu.Name2) AS ResellerNameInternal
# MAGIC ,cu.Createdon AS ResellerStartDate
# MAGIC ,rg.ResellerGroupCode AS ResellerGroupCode
# MAGIC ,rg.ResellerGroupName AS ResellerGroupName
# MAGIC ,cu.Country_RegionCode AS ResellerGeographyInternal
# MAGIC ,'' AS ResellerGroupStartDate
# MAGIC ,rg.ResellerName AS ResellerNameMaster
# MAGIC ,rg.Entity AS IGEntityOfReseller
# MAGIC ,'' AS ResellerGeographyMaster
# MAGIC ,rg.ResellerID AS ResellerID
# MAGIC From 
# MAGIC   silver_dev.igsql03.sales_cr_memo_header sih
# MAGIC Inner Join 
# MAGIC   silver_dev.igsql03.sales_cr_memo_line sil
# MAGIC ON
# MAGIC   sih.No_ = sil.DocumentNo_
# MAGIC AND
# MAGIC   sih.Sys_DatabaseName = sil.Sys_DatabaseName
# MAGIC AND
# MAGIC   sih.Sys_Silver_IsCurrent = true
# MAGIC AND
# MAGIC   sil.Sys_Silver_IsCurrent = true
# MAGIC INNER JOIN 
# MAGIC   silver_dev.igsql03.item it
# MAGIC ON 
# MAGIC   sil.No_ = it.No_
# MAGIC AND
# MAGIC   sil.Sys_DatabaseName = it.Sys_DatabaseName
# MAGIC AND
# MAGIC   it.Sys_Silver_IsCurrent = true
# MAGIC LEFT JOIN
# MAGIC   (
# MAGIC   SELECT
# MAGIC   Code,
# MAGIC   Name,
# MAGIC   Sys_DatabaseName
# MAGIC   FROM silver_dev.igsql03.dimension_value
# MAGIC   WHERE DimensionCode = 'VENDOR'
# MAGIC   AND Sys_Silver_IsCurrent = true
# MAGIC   ) ven
# MAGIC ON 
# MAGIC   sil.ShortcutDimension1Code = ven.Code
# MAGIC AND
# MAGIC   sil.Sys_DatabaseName = ven.Sys_DatabaseName
# MAGIC AND
# MAGIC   sil.Sys_Silver_IsCurrent = true  
# MAGIC LEFT JOIN 
# MAGIC   silver_dev.igsql03.customer cu
# MAGIC ON 
# MAGIC   sih.`Sell-toCustomerNo_` = cu.No_
# MAGIC AND
# MAGIC   sih.Sys_DatabaseName = cu.Sys_DatabaseName  
# MAGIC AND
# MAGIC   cu.Sys_Silver_IsCurrent = true
# MAGIC LEFT JOIN 
# MAGIC (select distinct  
# MAGIC InfinigateCompany,
# MAGIC ResellerID,
# MAGIC ResellerGroupCode,
# MAGIC ResellerGroupName,
# MAGIC ResellerName,
# MAGIC Entity
# MAGIC FROM silver_dev.masterdata.reseller_groups) rg
# MAGIC ON 
# MAGIC   concat_ws(RIGHT(sih.Sys_DatabaseName,2), sih.`Sell-toCustomerNo_`) = concat_ws(rg.Entity, rg.ResellerID)
# MAGIC LEFT JOIN
# MAGIC (select SKU, Commitment_Duration, Billing_Frequency, Consumption_Model, Product_Type
# MAGIC from silver_dev.masterdata.sku) sk
# MAGIC ON 
# MAGIC   it.No_ = sk.sku
# MAGIC AND
# MAGIC   it.Sys_Silver_IsCurrent = true

# COMMAND ----------


