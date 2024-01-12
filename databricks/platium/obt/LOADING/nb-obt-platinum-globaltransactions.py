# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Gold Transactions Platinum
spark.sql(f"""
          

CREATE OR Replace VIEW globaltransactions AS


SELECT
  g.GroupEntityCode,
  g.EntityCode,
  g.TransactionDate,
  g.SalesOrderDate,
  g.SalesOrderID,
  g.SalesOrderItemID,
  g.SKUInternal,
  g.SKUMaster,
  g.Description,
  g.ProductTypeInternal,
  g.ProductTypeMaster,
  g.CommitmentDuration1Master,
  g.CommitmentDuration2Master,
  g.BillingFrequencyMaster,
  g.ConsumptionModelMaster,
  g.VendorCode,
  g.VendorNameInternal,
  g.VendorNameMaster,
  g.VendorGeography,
  g.VendorStartDate,
  g.ResellerCode,
  g.ResellerNameInternal,
  g.ResellerGeographyInternal,
  g.ResellerStartDate,
  g.ResellerGroupCode,
  g.ResellerGroupName,  
  g.ResellerGroupStartDate,
  g.CurrencyCode,
  g.RevenueAmount,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN e1.Period_FX_rate
  ELSE e.Period_FX_rate
  END AS Period_FX_rate,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN cast(g.RevenueAmount / e1.Period_FX_rate AS DECIMAL(10,2))
  ELSE cast(g.RevenueAmount / e.Period_FX_rate AS DECIMAL(10,2))
  END AS RevenueAmount_Euro,
  g.GP1,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN cast(g.GP1 / e1.Period_FX_rate AS DECIMAL(10,2))
  ELSE cast(g.GP1 / e.Period_FX_rate AS DECIMAL(10,2))
  END AS GP1_Euro
FROM 
  gold_{ENVIRONMENT}.obt.globaltransactions g
LEFT JOIN
  gold_{ENVIRONMENT}.obt.exchange_rate e
ON
  e.Calendar_Year = cast(year(g.TransactionDate) as string)
AND
  e.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
AND
  g.EntityCode = e.COD_AZIENDA
AND
  e.ScenarioGroup = 'Actual'
--Only for VU and entitycode 'NOTINTAGETIK'
LEFT JOIN
  (SELECT DISTINCT Calendar_Year, Month, Currency, Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate WHERE ScenarioGroup = 'Actual') e1
ON
  e1.Calendar_Year = cast(year(g.TransactionDate) as string)
AND
  e1.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
AND
  g.CurrencyCode = cast(e1.Currency as string)
  """)
