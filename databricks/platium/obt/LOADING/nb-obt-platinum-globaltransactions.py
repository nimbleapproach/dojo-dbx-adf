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
          

create or replace table globaltransactions as 
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
  END AS GP1_Euro,
  --Added Cost Amount
  g.CostAmount AS COGS,
  CASE 
  WHEN (g.GroupEntityCode = 'VU' OR g.EntityCode IN ('NOTINTAGETIK', 'RO2', 'HR2', 'SI1', 'BG1'))
  THEN cast(g.CostAmount / e1.Period_FX_rate AS DECIMAL(10,2))
  ELSE cast(g.CostAmount / e.Period_FX_rate AS DECIMAL(10,2))
  END AS COGS_Euro,
  case when g.VendorNameInternal in('Mouse & Bear Solutions Ltd','Blackthorne International Transport Ltd','Transport','Nuvias Internal Logistics') then 'Logistics'
        when g.SKUInternal in('TRADEFAIR_A','TRAVELEXP') then 'Marketing'
        when g.SKUInternal ='BEBAT' THEN 'Others'
        when g.ProductTypeInternal ='Shipping & Delivery Income' then 'Logistics'
        when g.ProductTypeInternal in('Quarterly Rebate','Instant Rebate') then 'Rebate'
  else 'Revenue'end as GL_Group
  ,g.TransactionDate AS GL_Doc_PostingDate
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
WHERE g.GroupEntityCode <>'IG'

--[yz] 19.04.2024: split out IG from other entities due to cost adjustments need to be added

UNION ALL
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
  -- cast(g.RevenueAmount / e.Period_FX_rate AS DECIMAL(10,2)) as RevenueAmount_Euro,
  cast(g.RevenueAmount/ e.Period_FX_rate AS DECIMAL(10,2)) as RevenueAmount_Euro,
  g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 ))  AS GP1,
 cast(( g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) )/ e.Period_FX_rate AS DECIMAL(10,2)) AS GP1_Euro,
  --Added Cost Amount
  g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 ) AS COGS,
 cast((g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) / e.Period_FX_rate AS DECIMAL(10,2)) AS COGS_Euro,
 coalesce(GL_Group, 'Others') AS GL_Group,
 GL_Doc_PostingDate
 --,cast(g.RevenueAmount/ e2.Period_FX_rate AS DECIMAL(10,2)) as RevenueAmount_Euro
 --,cast((g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) / e2.Period_FX_rate AS DECIMAL(10,2)) AS COGS_Euro,
 --,cast(( g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) )/ e2.Period_FX_rate AS DECIMAL(10,2)) AS GP1_Euro,
FROM gold_{ENVIRONMENT}.obt.infinigate_globaltransactions_cost_adjusted_gl g
LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = cast(year(g.TransactionDate) as string)
                                         AND e.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
                                         AND  CASE WHEN g.EntityCode = 'BE1' THEN 'NL1' ELSE  g.EntityCode  END   = e.COD_AZIENDA --[YZ] 15.03.2024 : Add Replace BE1 with NL1 since it is not a valid entity in tagetik for fx
                                         AND  e.ScenarioGroup = 'Actual'
/* Maybe this will be required, means we'll need new version of each '_Euro' converted column though.
LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e2 ON e.Calendar_Year = cast(year(g.GL_Doc_PostingDate) as string)
                                         AND e.Month = right(concat('0',cast(month(g.GL_Doc_PostingDate) as string)),2)
                                         AND  CASE WHEN g.EntityCode = 'BE1' THEN 'NL1' ELSE  g.EntityCode  END   = e.COD_AZIENDA --[YZ] 15.03.2024 : Add Replace BE1 with NL1 since it is not a valid entity in tagetik for fx
                                         AND  e.ScenarioGroup = 'Actual'
                                         */
WHERE g.GroupEntityCode ='IG'

  """)
