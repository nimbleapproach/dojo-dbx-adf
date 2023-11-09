# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Platinum Data Quality Observation
spark.sql(f"""

CREATE OR Replace VIEW globaltransactions_dataquality_observation AS

SELECT GroupEntityCode, EntityCode, TransactionDate,
--SalesOrderDate
SUM(CASE WHEN SalesOrderDate IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SalesOrderDate_Null,
SUM(CASE WHEN SalesOrderDate = '1900-01-01' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SalesOrderDate_DefaultDate,

--SalesOrderID
SUM(CASE WHEN SalesOrderID IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SalesOrderID_Null,
SUM(CASE WHEN SalesOrderID = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SalesOrderID_NaN,

--SalesOrderItemID
SUM(CASE WHEN SalesOrderItemID IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SalesOrderItemID_Null,
SUM(CASE WHEN SalesOrderItemID = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SalesOrderItemID_NaN,

--SKUInternal
SUM(CASE WHEN SKUInternal IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SKUInternal_Null,
SUM(CASE WHEN SKUInternal = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SKUInternal_NaN,

--SKUMaster
SUM(CASE WHEN SKUMaster IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SKUMaster_Null,
SUM(CASE WHEN SKUMaster = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS SKUMaster_NaN,

--Description
SUM(CASE WHEN Description IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS Description_Null,
SUM(CASE WHEN Description = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS Description_NaN,

--ProductTypeInternal
SUM(CASE WHEN ProductTypeInternal IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ProductTypeInternal_Null,
SUM(CASE WHEN ProductTypeInternal = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ProductTypeInternal_NaN,

--ProductTypeMaster
SUM(CASE WHEN ProductTypeMaster IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ProductTypeMaster_Null,
SUM(CASE WHEN ProductTypeMaster = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ProductTypeMaster_NaN,

--CommitmentDuration1Master
SUM(CASE WHEN CommitmentDuration1Master IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS CommitmentDuration1Master_Null,
SUM(CASE WHEN CommitmentDuration1Master = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS CommitmentDuration1Master_NaN,

--CommitmentDuration2Master
SUM(CASE WHEN CommitmentDuration2Master IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS CommitmentDuration2Master_Null,
SUM(CASE WHEN CommitmentDuration2Master = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS CommitmentDuration2Master_NaN,

--BillingFrequencyMaster
SUM(CASE WHEN BillingFrequencyMaster IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS BillingFrequencyMaster_Null,
SUM(CASE WHEN BillingFrequencyMaster = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS BillingFrequencyMaster_NaN,

--ConsumptionModelMaster
SUM(CASE WHEN ConsumptionModelMaster IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ConsumptionModelMaster_Null,
SUM(CASE WHEN ConsumptionModelMaster = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ConsumptionModelMaster_NaN,

--VendorCode
SUM(CASE WHEN VendorCode IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) VendorCode_Null,
SUM(CASE WHEN VendorCode = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS VendorCode_NaN,

--VendorNameInternal
SUM(CASE WHEN VendorNameInternal IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) VendorNameInternal_Null,
SUM(CASE WHEN VendorNameInternal = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS VendorNameInternal_NaN,

--VendorNameMaster
SUM(CASE WHEN VendorNameMaster IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) VendorNameMaster_Null,
SUM(CASE WHEN VendorNameMaster = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS VendorNameMaster_NaN,

--VendorGeography
SUM(CASE WHEN VendorGeography IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) VendorGeography_Null,
SUM(CASE WHEN VendorGeography = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS VendorGeography_NaN,

--VendorStartDate
SUM(CASE WHEN VendorStartDate IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) VendorStartDate_Null,
SUM(CASE WHEN VendorStartDate = '1900-01-01' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS VendorStartDate_DefaultDate,

--ResellerCode
SUM(CASE WHEN ResellerCode IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) ResellerCode_Null,
SUM(CASE WHEN ResellerCode = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ResellerCode_NaN,

--ResellerNameInternal
SUM(CASE WHEN ResellerNameInternal IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) ResellerNameInternal_Null,
SUM(CASE WHEN ResellerNameInternal = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ResellerNameInternal_NaN,

--ResellerGeographyInternal
SUM(CASE WHEN ResellerGeographyInternal IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) ResellerGeographyInternal_Null,
SUM(CASE WHEN ResellerGeographyInternal = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ResellerGeographyInternal_NaN,

--ResellerStartDate
SUM(CASE WHEN ResellerStartDate IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) ResellerStartDate_Null,
SUM(CASE WHEN ResellerStartDate = '1900-01-01' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ResellerStartDate_DefaultDate,

--ResellerGroupCode
SUM(CASE WHEN ResellerGroupCode IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) ResellerGroupCode_Null,
SUM(CASE WHEN ResellerGroupCode = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ResellerGroupCode_NaN,

--ResellerGroupStartDate
SUM(CASE WHEN ResellerGroupStartDate IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) ResellerGroupStartDate_Null,
SUM(CASE WHEN ResellerGroupStartDate = '1900-01-01' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS ResellerGroupStartDate_DefaultDate,

--CurrencyCode
SUM(CASE WHEN CurrencyCode IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) CurrencyCode_Null,
SUM(CASE WHEN CurrencyCode = 'NaN' THEN 1 ELSE 0 END) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS CurrencyCode_NaN,

COUNT(*) OVER (PARTITION BY GroupEntityCode, EntityCode, TransactionDate) AS Sys_Gold_NumberOfRows
FROM gold_{ENVIRONMENT}.obt.globaltransactions
""")
