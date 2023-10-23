# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Starlink
spark.sql(
    f"""         
CREATE OR REPLACE VIEW starlink_globaltransactions AS

with cte as 
(SELECT
  'SL' AS GroupEntityCode,
  'AE1' AS EntityCode,
  si.SID,
  to_date(si.Date) AS TransactionDate,
  TO_DATE(si.Sales_Order_Date) AS SalesOrderDate,
  RIGHT(si.Sales_Order_Number,9) AS SalesOrderID,
  si.SKU_ID AS SalesOrderItemID,
  COALESCE(it.SKU_ID, "NaN") AS SKUInternal,
  COALESCE(datanowarr.SKU, 'NaN')  AS SKUMaster,
  COALESCE(it.Description,'NaN') AS Description,
  COALESCE(it.Item_Category,'NaN') AS ProductTypeInternal,
  COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
  coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
  coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
  coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
  coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
  coalesce(ven.Vendor_ID,   'NaN') AS VendorCode,
  coalesce(ven.Vendor_Name, 'NaN') AS VendorNameInternal,
  coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
  'AE1' AS VendorGeography,
  to_date('1900-01-01') AS VendorStartDate,
  coalesce(cu.Customer_Name,'NaN') AS ResellerCode,
  coalesce(cu.Customer_Name,'NaN') AS ResellerNameInternal,
  'AE1' AS ResellerGeographyInternal,
  to_date(coalesce(cu.Date_Created, '1900-01-01' )) AS ResellerStartDate,
  'NaN' AS ResellerGroupCode,
  'NaN' AS ResellerGroupName,
  to_date(coalesce(cu.Date_Created, '1900-01-01' )) AS ResellerGroupStartDate,
  si.Deal_Currency AS CurrencyCode,
  cast(si.Revenue_USD as DECIMAL(10, 2)) AS RevenueAmount

FROM
  silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
  left join silver_{ENVIRONMENT}.netsuite.masterdatasku AS it ON si.SKU_ID = it.SKU_ID
  and it.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatavendor AS ven ON si.Vendor_Name = ven.Vendor_Name
  and ven.Sys_Silver_IsCurrent is not false
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatacustomer AS cu ON si.Customer_Name = cu.Customer_Name
  and cu.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.datanowarr AS datanowarr ON case
    when lower(regexp_replace(
        substring_index(ven.Vendor_Name, '-', 1),
        ' ',
        ''
      )) NOT IN ( select distinct lower(Vendor_Name) from silver_{ENVIRONMENT}.netsuite.masterdatavendor )
    then 'other vendors'
    else lower(
      regexp_replace(
        substring_index(ven.Vendor_Name, '-', 1),
        ' ',
        ''
      )
    ) end = lower(datanowarr.Vendor_Name)
  AND datanowarr.SKU = it.SKU_ID
  AND datanowarr.Sys_Silver_IsCurrent = 1
where
  si.Sys_Silver_IsCurrent = 1)

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
  substring_index(ResellerCode, ' ', 1)ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount
from
  cte""")

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")

df_sl = spark.read.table(f"gold_{ENVIRONMENT}.obt.starlink_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col


target_columns = df_obt.columns

source_columns = df_sl.columns

intersection_columns = [column for column in target_columns if column in source_columns]

selection_columns = [
    col(column) for column in intersection_columns if column not in ["SID"]
]

# COMMAND ----------

df_selection = df_sl.select(selection_columns)
df_selection = df_selection.fillna(value= 'NaN').replace('', 'NaN')

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'SL'").saveAsTable("globaltransactions")
