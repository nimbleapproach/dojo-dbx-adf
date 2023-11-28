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
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace VIEW globaltransactions AS
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   g.GroupEntityCode,
# MAGIC   g.EntityCode,
# MAGIC   g.TransactionDate,
# MAGIC   g.SalesOrderDate,
# MAGIC   g.SalesOrderID,
# MAGIC   g.SalesOrderItemID,
# MAGIC   g.SKUInternal,
# MAGIC   g.SKUMaster,
# MAGIC   g.Description,
# MAGIC   g.ProductTypeInternal,
# MAGIC   g.ProductTypeMaster,
# MAGIC   g.CommitmentDuration1Master,
# MAGIC   g.CommitmentDuration2Master,
# MAGIC   g.BillingFrequencyMaster,
# MAGIC   g.ConsumptionModelMaster,
# MAGIC   g.VendorCode,
# MAGIC   g.VendorNameInternal,
# MAGIC   g.VendorNameMaster,
# MAGIC   g.VendorGeography,
# MAGIC   g.VendorStartDate,
# MAGIC   g.ResellerCode,
# MAGIC   g.ResellerNameInternal,
# MAGIC   g.ResellerGeographyInternal,
# MAGIC   g.ResellerStartDate,
# MAGIC   g.ResellerGroupCode,
# MAGIC   g.ResellerGroupName,  
# MAGIC   g.ResellerGroupStartDate,
# MAGIC   g.CurrencyCode,
# MAGIC   g.RevenueAmount,
# MAGIC   CASE 
# MAGIC   WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
# MAGIC   THEN e1.Period_FX_rate
# MAGIC   ELSE e.Period_FX_rate
# MAGIC   END AS Period_FX_rate,
# MAGIC   CASE 
# MAGIC   WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
# MAGIC   THEN cast(g.RevenueAmount / e1.Period_FX_rate AS DECIMAL(10,2))
# MAGIC   ELSE cast(g.RevenueAmount / e.Period_FX_rate AS DECIMAL(10,2))
# MAGIC   END AS RevenueAmount_Euro
# MAGIC FROM 
# MAGIC   gold_dev.obt.globaltransactions g
# MAGIC LEFT JOIN
# MAGIC   gold_dev.obt.exchange_rate e
# MAGIC ON
# MAGIC   e.Calendar_Year = cast(year(g.TransactionDate) as string)
# MAGIC AND
# MAGIC   e.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
# MAGIC AND
# MAGIC   g.EntityCode = e.COD_AZIENDA
# MAGIC AND
# MAGIC   e.ScenarioGroup = 'Actual'
# MAGIC --Only for VU and entitycode 'NOTINTAGETIK'
# MAGIC LEFT JOIN
# MAGIC   (SELECT DISTINCT Calendar_Year, Month, Currency, Period_FX_rate FROM gold_dev.obt.exchange_rate WHERE ScenarioGroup = 'Actual') e1
# MAGIC ON
# MAGIC   e1.Calendar_Year = cast(year(g.TransactionDate) as string)
# MAGIC AND
# MAGIC   e1.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
# MAGIC AND
# MAGIC   g.CurrencyCode = cast(e1.Currency as string)
