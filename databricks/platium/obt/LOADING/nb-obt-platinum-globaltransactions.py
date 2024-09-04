# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

spark.sql(f"""
Create or replace temporary view max_fx_rates as 
SELECT
  Currency,
  MAX(Period_FX_rate) as Period_FX_rate
FROM
  gold_{ENVIRONMENT}.obt.exchange_rate r
WHERE
  ScenarioGroup = 'Actual' 
AND Calendar_Year = (select MAX(Calendar_Year) from gold_{ENVIRONMENT}.obt.exchange_rate m WHERE ScenarioGroup = 'Actual' AND r.Currency = m.Currency)
AND Month = (select MAX(Month) from gold_{ENVIRONMENT}.obt.exchange_rate m WHERE ScenarioGroup = 'Actual' AND r.Calendar_Year = m.Calendar_Year AND r.Currency = m.Currency)
GROUP BY Currency
"""
)

# COMMAND ----------

# DBTITLE 1,Gold Transactions Platinum
spark.sql(f"""
          
CREATE OR REPLACE TABLE globaltransactions as 
 
SELECT
  g.GroupEntityCode,
  g.EntityCode,
  'NaN' AS DocumentNo,
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
  THEN ifnull(e1.Period_FX_rate, mx.Period_FX_Rate)
  ELSE ifnull(e.Period_FX_rate, mx.Period_FX_Rate)
  END AS Period_FX_rate,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN cast(g.RevenueAmount / ifnull(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
  ELSE cast(g.RevenueAmount / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
  END AS RevenueAmount_Euro,
  g.GP1,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN cast(g.GP1 / ifnull(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
  ELSE cast(g.GP1 / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
  END AS GP1_Euro,
  --Added Cost Amount
  g.CostAmount AS COGS,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN cast(g.CostAmount / ifnull(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
  ELSE cast(g.CostAmount / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
  END AS COGS_Euro,
  case when g.VendorNameInternal in('Mouse & Bear Solutions Ltd','Blackthorne International Transport Ltd','Transport','Nuvias Internal Logistics') then 'Logistics'
        when g.SKUInternal in('TRADEFAIR_A','TRAVELEXP') then 'Marketing'
        when g.SKUInternal ='BEBAT' THEN 'Others'
        when g.ProductTypeInternal ='Shipping & Delivery Income' then 'Logistics'
        when g.ProductTypeInternal in('Quarterly Rebate','Instant Rebate') then 'Rebate'
  else 'Revenue'end as GL_Group
  ,0 as TopCostFlag
FROM 
  gold_{ENVIRONMENT}.obt.globaltransactions g
LEFT JOIN
  gold_{ENVIRONMENT}.obt.exchange_rate e
ON
  e.Calendar_Year = cast(year(g.TransactionDate) as string)
AND
  e.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
AND
/*[YZ] 15.03.2024 : Add Replace BE1 with NL1 since it is not a valid entity in tagetik for fx*/
  CASE WHEN g.EntityCode = 'BE1' THEN 'NL1' ELSE  g.EntityCode  END   = e.COD_AZIENDA
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
LEFT JOIN 
  max_fx_rates mx 
ON
  g.CurrencyCode = cast(mx.Currency as string)
WHERE g.GroupEntityCode <>'IG'

--[yz] 19.04.2024: split out IG from other entities due to cost adjustments need to be added

UNION ALL

SELECT 
  GroupEntityCode,
  EntityCode,
  DocumentNo,
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
  VendorGeography,
  VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,  
  ResellerGroupStartDate,
  CurrencyCode,
  SUM(RevenueAmount) AS RevenueAmount,
  Period_FX_rate,
  SUM(RevenueAmount_Euro) AS RevenueAmount_Euro,
  SUM(GP1) AS GP1,
  SUM(GP1_Euro) AS GP1_Euro,
  SUM(COGS) AS COGS,
  SUM(COGS_Euro) AS COGS_Euro,
  GL_Group,
  TopCostFlag
  FROM (
SELECT
  g.GroupEntityCode,
  g.EntityCode,
  g.DocumentNo,
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
  g.RevenueAmount as RevenueAmount,
  e.Period_FX_rate ,
  cast(g.RevenueAmount/ e.Period_FX_rate AS DECIMAL(10,2)) as RevenueAmount_Euro,
  g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 ))  AS GP1,
 cast(( g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) )/ e.Period_FX_rate AS DECIMAL(10,2)) AS GP1_Euro,
  --Added Cost Amount
  g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 ) AS COGS,
 cast((g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) / e.Period_FX_rate AS DECIMAL(10,2)) AS COGS_Euro,
 coalesce(GL_Group, 'Others') AS GL_Group,
  0 as TopCostFlag
FROM gold_{ENVIRONMENT}.obt.infinigate_globaltransactions_cost_adjusted_gl g
LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = cast(year(g.TransactionDate) as string)
                                         AND e.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
                                         AND  CASE WHEN g.EntityCode = 'BE1' THEN 'NL1' ELSE  g.EntityCode  END   = e.COD_AZIENDA --[YZ] 15.03.2024 : Add Replace BE1 with NL1 since it is not a valid entity in tagetik for fx
                                         AND  e.ScenarioGroup = 'Actual'

WHERE g.GroupEntityCode ='IG'

UNION ALL
--Add IG top cost adjustments
SELECT
  'IG',
  tc.EntityCode,
  tc.DocumentNo_,
  tc.PostingDate,
  NULL AS SalesOrderDate,
  NULL AS SalesOrderID,
  NULL AS SalesOrderItemID,
  NULL AS SKUInternal,
  NULL AS SKUMaster,
  NULL AS Description,
  NULL AS ProductTypeInternal,
  NULL AS ProductTypeMaster,
  NULL AS CommitmentDuration1Master,
  NULL AS CommitmentDuration2Master,
  NULL AS BillingFrequencyMaster,
  NULL AS ConsumptionModelMaster,
  tc.VendorCode,
  tc.VendorName AS VendorNameInternal,
  NULL AS VendorNameMaster,
  NULL AS VendorGeography,
  NULL AS VendorStartDate,
  NULL AS ResellerCode,
  NULL AS ResellerNameInternal,
  NULL AS ResellerGeographyInternal,
  NULL AS ResellerStartDate,
  NULL AS ResellerGroupCode,
  NULL AS ResellerGroupName,  
  NULL AS ResellerGroupStartDate,
  NULL AS CurrencyCode,
  CAST(SUM(CASE WHEN GL_Group = 'Revenue' THEN ((-1) * tc.CostAmount)
      ELSE 0.00 END )AS DECIMAL(20,2)) AS RevenueAmount,
  tc.CostAmount / tc.CostAmount_EUR AS Period_FX_rate ,
  CAST(SUM(CASE WHEN GL_Group = 'Revenue' THEN ((-1) * tc.CostAmount_EUR)
      ELSE 0.00 END )AS DECIMAL(20,2)) AS RevenueAmount_Euro,
 (-1) * SUM(tc.CostAmount) AS GP1,
 (-1) * SUM(tc.CostAmount_EUR)  AS GP1_Euro,
 (-1) * SUM(tc.CostAmount)  AS COGS,
 (-1) * SUM(tc.CostAmount_EUR)  AS COGS_Euro,
 GL_Group,
 1 as TopCostFlag
FROM gold_{ENVIRONMENT}.obt.infinigate_top_cost_adjustments tc

GROUP BY ALL
  
  ) combined
GROUP BY ALL

  """)
