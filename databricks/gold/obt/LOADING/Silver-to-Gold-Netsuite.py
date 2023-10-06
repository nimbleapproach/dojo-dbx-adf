# Databricks notebook source
# DBTITLE 1,Silver to Gold Netsuite
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC 'SL' AS GroupEntityCode
# MAGIC ,si.SID
# MAGIC ,si.Date  AS TransactionDate
# MAGIC -- ,RIGHT(sih.Sys_DatabaseName,2) AS InfinigateEntity
# MAGIC ,si.Revenue_USD AS RevenueAmountUSD
# MAGIC ,si.Deal_Currency
# MAGIC ,it.SKU_ID AS SKU
# MAGIC ,it.Description AS Description
# MAGIC ,it.Item_Category AS ProductTypeInternal
# MAGIC ,'' AS ProductTypeMaster
# MAGIC ,'' AS ProductSubtype
# MAGIC ,ven.Vendor_ID AS VendorCode
# MAGIC ,ven.Vendor_Name AS VendorName
# MAGIC ,'' AS VendorGeography
# MAGIC ,ven.Contract_Start_Date AS VendorStartDate
# MAGIC ,cu.Customer_Name AS ResellerCode
# MAGIC ,'' AS ResellerNameInternal
# MAGIC ,cu.Date_Created AS ResellerStartDate
# MAGIC ,'' AS ResellerGroupCode
# MAGIC ,'' AS ResellerGroupName
# MAGIC ,'' AS ResellerGeographyInternal
# MAGIC ,'' AS ResellerGroupStartDate
# MAGIC ,'' AS ResellerNameMaster
# MAGIC ,'' AS IGEntityOfReseller
# MAGIC ,'' AS ResellerGeographyMaster
# MAGIC ,'' AS ResellerID
# MAGIC FROM 
# MAGIC   silver_dev.netsuite.InvoiceReportsInfinigate AS si
# MAGIC
# MAGIC left join silver_dev.netsuite.masterdatasku AS it
# MAGIC ON si.SKU_ID = it.SKU_ID
# MAGIC
# MAGIC LEFT JOIN silver_dev.netsuite.masterdatavendor AS ven
# MAGIC ON si.Vendor_Name = ven.Vendor_Name
# MAGIC
# MAGIC LEFT JOIN silver_dev.netsuite.masterdatacustomer AS cu
# MAGIC ON si.Customer_Name = cu.Customer_Name
# MAGIC
# MAGIC
# MAGIC
# MAGIC
