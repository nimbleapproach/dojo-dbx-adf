# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Cloud Distribution
spark.sql(
    f"""         
CREATE OR REPLACE VIEW clouddistribution_globaltransactions AS

WITH initial_query AS (
  SELECT
    'NU' AS GroupEntityCode,
    'UK4' AS EntityCode,
    coalesce(
      to_date(
        CAST(
          UNIX_TIMESTAMP(invoice.InvoiceDate, 'dd/MM/yyyy') AS timestamp
        )
      ),
      to_date(invoice.InvoiceDate)
    ) as TransactionDate,
    coalesce(
      to_date(
        CAST(
          UNIX_TIMESTAMP(invoice.InvoiceDate, 'dd/MM/yyyy') AS timestamp
        )
      ),
      to_date(invoice.InvoiceDate)
    ) as SalesOrderDate,
    coalesce(invoice.InvoiceNumber, 'NaN') AS SalesOrderID,
    coalesce(invoice.ItemID, 'NaN') AS SalesOrderItemID,
    COALESCE(invoice.SKU, "NaN") AS SKUInternal,
    COALESCE(datanowarr.SKU, 'NaN') AS SKUMaster,
    COALESCE(invoice.SKUDescription, 'NaN') AS Description,
    COALESCE(invoice.ItemType, 'NaN') AS ProductTypeInternal,
    COALESCE(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
    coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
    coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
    coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
    coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
    coalesce(invoice.VendorID, 'NaN') AS VendorCode,
    coalesce(invoice.VendorName, 'NaN') AS VendorNameInternal,
    coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
    'NaN' AS VendorGeography,
    to_date('1900-01-01') AS VendorStartDate,
    coalesce(invoice.CustomerAccount, 'NaN') AS ResellerCode,
    coalesce(invoice.CustomerName, 'NaN') AS ResellerNameInternal,
    'NaN' AS ResellerGeographyInternal,
    to_date('1900-01-01') AS ResellerStartDate,
    'NaN' AS ResellerGroupCode,
    'NaN' AS ResellerGroupName,
    to_date('1900-01-01') AS ResellerGroupStartDate,
    Currency AS CurrencyCode,
    cast(RevenueLocal as DECIMAL(10, 2)) AS RevenueAmount,
    cast(CostLocal as DECIMAL(10, 2)) AS CostAmount,
    cast(MarginLocal as DECIMAL(10, 2)) AS GP1
  from
    silver_{ENVIRONMENT}.cloud_distribution.invoicedata as invoice
    LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = invoice.SKU
  WHERE
    invoice.Sys_Silver_IsCurrent = true
    and  invoice.MarginLocal is not null
)
SELECT
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
  substring_index(ResellerCode, ' ', 1) ResellerCode,
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
  RevenueAmount,
  CostAmount,
  GP1
from
  initial_query""")

# COMMAND ----------

df_obt = spark.read.table("globaltransactions")
df_clouddistribution = spark.read.table(f"gold_{ENVIRONMENT}.obt.clouddistribution_globaltransactions")

# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_clouddistribution.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_clouddistribution.select(selection_columns)

# COMMAND ----------

df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'NU' AND EntityCode ='UK4'").saveAsTable("globaltransactions")
