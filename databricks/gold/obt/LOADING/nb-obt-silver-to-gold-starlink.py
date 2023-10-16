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

SELECT
  'SL' AS GroupEntityCode,
  si.SID,
  to_date(si.Date) AS TransactionDate,
  cast(si.Revenue_USD as DECIMAL(10, 0)) AS RevenueAmount,
  si.Deal_Currency AS CurrencyCode,
  COALESCE(it.SKU_ID, "NaN") AS SKU,
  it.Description AS Description,
  it.Item_Category AS ProductTypeInternal,
  datanowarr.Product_Type AS ProductTypeMaster,
  '' AS ProductSubtype,
  datanowarr.Commitment_Duration AS CommitmentDuration,
  datanowarr.Billing_Frequency AS BillingFrequency,
  datanowarr.Consumption_Model AS ConsumptionModel,
  ven.Vendor_ID AS VendorCode,
  ven.Vendor_Name AS VendorName,
  '' AS VendorGeography,
  to_date(ven.Contract_Start_Date) AS VendorStartDate,
  cu.Customer_Name AS ResellerCode,
  cu.Customer_Name AS ResellerNameInternal,
  to_date(cu.Date_Created) AS ResellerStartDate,
  '' AS ResellerGroupCode,
  '' AS ResellerGroupName,
  '' AS ResellerGeographyInternal,
  to_date(cu.Date_Created) AS ResellerGroupStartDate,
  '' AS ResellerNameMaster,
  '' AS IGEntityOfReseller,
  '' AS ResellerGeographyMaster,
  '' AS ResellerID,
  datanowarr.Vendor_Name AS VendorNameMaster
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
  si.Sys_Silver_IsCurrent = 1""")

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

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'SL'").saveAsTable("globaltransactions")
