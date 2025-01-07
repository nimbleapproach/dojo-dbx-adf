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

spark.sql(f"""
Create or replace temporary view globaltransactions_arr_history as 
SELECT
SKUInternal
,SKUMaster
,VendorNameInternal
,VendorNameMaster
,(date_format(TransactionDate, 'yyyy-MM')) as TransactionDateYYYYMM
,is_matched 
,matched_type
,matched_arr_type
,ProductTypeMaster
,CommitmentDuration1Master
,CommitmentDuration2Master
,BillingFrequencyMaster
,ConsumptionModelMaster
FROM gold_{ENVIRONMENT}.orion.globaltransactions_arr r
  group by all
  """)
 


# COMMAND ----------

# IG
spark.sql(f"""SELECT 
  CASE WHEN Sys_DatabaseName ='ReportsBE' AND TransactionDate >='2024-07-01'
              THEN 'NU' 
              ELSE GroupEntityCode 
              END AS GroupEntityCode,
  CASE WHEN Sys_DatabaseName ='ReportsBE' AND TransactionDate >='2024-07-01'
              THEN 'BE4' 
              ELSE EntityCode 
              END AS EntityCode,
  DocumentNo,
  TransactionDate, 
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  SKUMaster,
  Description,
  Technology,
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
  EndCustomer,
  IndustryVertical,
  CurrencyCode,
  SUM(RevenueAmount) AS RevenueAmount,
  Period_FX_rate,
  SUM(RevenueAmount_Euro) AS RevenueAmount_Euro,
SUM(  CASE 
      WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =1 
      THEN 0 
       WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =0
      then GP1
      ELSE GP1_adj
  END )AS GP1,
SUM(  CASE 
      WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =1 
      THEN 0 
       WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =0
      then GP1_Euro
      ELSE GP1_adj_Euro
  END )AS GP1_Euro,
  -- SUM(COGS) AS COGS,
SUM(  CASE 
      WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =1 
      THEN 0 
       WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =0
      then COGS
      ELSE COGS_adj
  END )AS COGS,
SUM(  CASE 
      WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =1 
      THEN 0 
       WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) and TopCostFlag =0
      then COGS_Euro
      ELSE COGS_adj_Euro
  END )AS COGS_Euro,
  CASE /**TeamFON has only service SKU but is posted in MSP Product revenue**/
        WHEN GL_Group='Revenue' AND lower(Type) like '%service%' and VendorCode <> 'TEF' THEN 'Service' 
        WHEN GL_Group='Revenue' AND lower(SKUInternal) like 'inf-ps-%' THEN 'Service' 
        WHEN GL_Group='Revenue' AND lower(SKUInternal) like 'inf-ts-%' THEN 'Service' 
        WHEN GL_Group='Revenue' AND lower(SKUInternal) like 'inf-eps-%' THEN 'Service'
        WHEN GL_Group='Revenue' AND lower(SKUInternal) like '%prof-service%' THEN 'Service'
        WHEN GL_Group='Revenue' AND ( lower(SKUInternal) like '%tr-so-%' OR lower(SKUInternal) like '%tr-sw-%') THEN 'Service'
        WHEN GL_Group='Revenue' AND lower(SKUInternal) like '%trn-day%' THEN 'Service'
        WHEN GL_Group='Revenue' AND ( lower(SKUInternal) like 'trdx%') THEN 'Service'  
        WHEN GL_Group='Revenue' AND SKUInternal  IN ('INF-DL-SEC-SPESEN','INF-TR-MSP','INF-DS-HID-1Y','TC8ZTCCEN') THEN 'Service'          
        WHEN GL_Group='Revenue' AND VendorCode ='SO_CL' AND lower(SKUInternal) like 'trrx%' THEN   'Service' --TRRXFW00ZZPCAA Sophos ATC Training Pack
        WHEN GL_Group='Revenue' AND VendorCode ='SO_CL' AND lower(SKUInternal) like 'atca%' THEN   'Service' --ATCARE00ZZPCAA Sophos ATC Training Pack
        WHEN GL_Group='Revenue' AND SKUInternal IN ('370988') THEN 'Others'
        WHEN GL_Group='Revenue' AND SKUInternal IN ('310188','310288','310388','310488','310588','310688','310788','310888','310988','311088','312088','313088','314088')
        THEN 'Service'
    ELSE  GL_Group 
        END AS GL_Group,
  TopCostFlag
  FROM (
SELECT
  g.GroupEntityCode,
  g.EntityCode,
  g.Sys_DatabaseName,
  g.DocumentNo,
  g.TransactionDate,
  g.SalesOrderDate,
  g.SalesOrderID,
  g.SalesOrderItemID,
  CASE WHEN ga.Consol_CreditAcc_ IS NULL THEN g.SKUInternal
        ELSE ga.Consol_CreditAcc_  END AS SKUInternal,
   CASE WHEN (lower(R.Name ) like '%invoice%'  OR  r.No ='Z-SC-S' OR  r.No ='RESCHEMTAX1' ) THEN 'Revenue'
        WHEN r.No LIKE 'VAT%' THEN 'Revenue'
        WHEN r.No = '099-000002-001' THEN 'Revenue'
    else r.Type end as Type,
  g.SKUMaster,
  g.Description,
  t.Name as Technology,
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
  ec.Name as EndCustomer,
  nace.display_text as IndustryVertical,
  g.CurrencyCode,
  g.RevenueAmount as RevenueAmount,
  e.Period_FX_rate ,
  cast(g.RevenueAmount/ e.Period_FX_rate AS DECIMAL(10,2)) as RevenueAmount_Euro,

    g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry )  AS GP1,
 cast(( g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry ) )/ e.Period_FX_rate AS DECIMAL(10,2)) AS GP1_Euro,

  g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 ))  AS GP1_adj,
 cast(( g.RevenueAmount + (g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) )/ e.Period_FX_rate AS DECIMAL(10,2)) AS GP1_adj_Euro,
  --Added Cost Amount
  (g.CostAmount +g.CostAmount_ValueEntry) AS COGS,
 cast((g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) / e.Period_FX_rate AS DECIMAL(10,2)) AS COGS_Euro,

(g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) AS COGS_adj,
 cast((g.CostAmount+g.CostAmount_ValueEntry + coalesce(g.Cost_ProRata_Adj,0 )) / e.Period_FX_rate AS DECIMAL(10,2)) AS COGS_adj_Euro,

 coalesce(GL_Group, 'Others') AS GL_Group,
  0 as TopCostFlag
FROM gold_{ENVIRONMENT}.obt.infinigate_globaltransactions_cost_adjusted_gl g
LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = cast(year(g.TransactionDate) as string)
                                         AND e.Month = right(concat('0',cast(month(g.TransactionDate) as string)),2)
                                         AND  CASE WHEN g.EntityCode = 'BE1' THEN 'NL1' ELSE  g.EntityCode  END   = e.COD_AZIENDA --[YZ] 15.03.2024 : Add Replace BE1 with NL1 since it is not a valid entity in tagetik for fx
                                         AND  e.ScenarioGroup = 'Actual'
LEFT JOIN (
          select 
        No,
        max(Name)Name,
        max(Type)Type
        from silver_{ENVIRONMENT}.masterdata.resources
        where Sys_Silver_IsCurrent =1
        group by all)  r ON g.SKUInternal = r.No

LEFT JOIN silver_{ENVIRONMENT}.igsql03.g_l_account ga 
ON ga.Sys_Silver_IsCurrent =1
AND g.SKUInternal = ga.No_
and g.Sys_DatabaseName = ga.Sys_DatabaseName

LEFT JOIN (
    select distinct  Contact_No_,
    Entity,
    name,
    section
    from  gold_{ENVIRONMENT}.obt.end_customer) ec
ON g.EndCustomerInternal = ec.Contact_No_
AND RIGHT(g.Sys_DatabaseName, 2) = ec.entity

LEFT JOIN gold_{ENVIRONMENT}.obt.nace_2_codes nace
ON ec.section = nace.section
AND nace.division is NULL

LEFT JOIN gold_{ENVIRONMENT}.obt.technology t
ON g.TechnologyCode = t.Code

WHERE g.GroupEntityCode ='IG'
and right(cast(g.TransactionDate as varchar(108)),8) <>'23:59:59'
UNION ALL
--Add IG top cost adjustments
SELECT
  'IG',
  tc.EntityCode,
  tc.Sys_DatabaseName,
  tc.DocumentNo_,
  tc.PostingDate,
  NULL AS SalesOrderDate,
  NULL AS SalesOrderID,
  NULL AS SalesOrderItemID,
  NULL AS SKUInternal,
  'Revenue' as Type,
  NULL AS SKUMaster,
  NULL AS Description,
  NULL AS Technology,
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
  NULL AS EndCustomer,
  NULL AS IndustryVertical,
  NULL AS CurrencyCode,
  CAST(SUM(CASE WHEN GL_Group like  '%Revenue%' THEN ((-1) * tc.CostAmount)
      ELSE 0.00 END )AS DECIMAL(20,2)) AS RevenueAmount,
  tc.CostAmount / tc.CostAmount_EUR AS Period_FX_rate ,
  CAST(SUM(CASE WHEN GL_Group like  '%Revenue%' THEN ((-1) * tc.CostAmount_EUR)
      ELSE 0.00 END )AS DECIMAL(20,2)) AS RevenueAmount_Euro,
 0 AS GP1,
 0 AS GP1_Euro,
 (-1) * SUM(tc.CostAmount)  AS GP1_adj,
 (-1) * SUM(tc.CostAmount_EUR)  AS GP1_adj_Euro,
 0  AS COGS,
 0 AS COGS_Euro,
 (-1) * SUM(tc.CostAmount) AS COGS_adj,
 (-1) * SUM(tc.CostAmount_EUR) AS COGS_adj_Euro,
 GL_Group,
 1 as TopCostFlag
FROM gold_{ENVIRONMENT}.obt.infinigate_top_cost_adjustments tc
where  right(cast( tc.PostingDate as varchar(108)),8) <>'23:59:59'
GROUP BY ALL
  
  ) combined
GROUP BY ALL""").createOrReplaceTempView('IG')

# COMMAND ----------

# NU
spark.sql(f"""
          

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
  NULL AS Technology,
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
  NULL AS EndCustomer,
  NULL AS IndustryVertical,
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
WHERE g.GroupEntityCode NOT IN('IG','SL','VU')""").createOrReplaceTempView('NU')

# COMMAND ----------

# NU_journal
spark.sql(f"""
with cte as (

select

'NU' AS GroupEntityCode,
EntityID AS EntityCode,
nu.Voucher AS DocumentNo,
TransactionDate,
NULL AS SalesOrderDate,
SalesOrderID,
 NULL AS SalesOrderItemID,
'NaN' AS SKUInternal,
'NaN' AS SKUMaster,
'NaN' AS Description,
'NaN' AS ProductTypeInternal,
'NaN' AS ProductTypeMaster,
'NaN' AS CommitmentDuration1Master,
'NaN' AS CommitmentDuration2Master,
'NaN' AS BillingFrequencyMaster,
'NaN' AS ConsumptionModelMaster,
nu.VendorCode,
coalesce(ven.PrimaryVendorName ,ven_mast.VendorNameInternal, 'NaN')  AS  VendorNameInternal,
'NaN' AS VendorNameMaster,
'NaN' AS VendorGeography,
NULL AS VendorStartDate,
coalesce(ResellerCode , 'NaN')  ResellerCode,
coalesce(ResellerNameInternal, 'NaN') ResellerNameInternal ,
'NaN' AS ResellerGeographyInternal,
NULL AS ResellerStartDate,
'NaN' AS ResellerGroupCode,
'NaN' AS ResellerGroupName,  
NULL AS ResellerGroupStartDate,
CurrencyCode,
Sales AS RevenueAmount,
Cost,
Sales + Cost AS GP1
from
  gold_{ENVIRONMENT}.obt.nuvias_journal nu
  left join (
    select
      distinct left(PrimaryVendorID,9) AS VendorCode,
      max(PrimaryVendorName)PrimaryVendorName
    from
      silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems
    where
      Sys_Silver_IsCurrent = 1
    group by all
  ) ven on nu.VendorCode = ven.VendorCode
  LEFT JOIN (
        select
           distinct left(VendorCode,9) AS VendorCode,
        max(VendorNameInternal)VendorNameInternal

          from silver_{ENVIRONMENT}.masterdata.vendor_mapping
          GROUP BY ALL 
  )ven_mast ON nu.VendorCode = ven_mast.VendorCode
)

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
  NULL AS Technology,
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
  NULL AS EndCustomer,
  NULL AS IndustryVertical,
  g.CurrencyCode,
  g.RevenueAmount,
  e.Period_FX_rate AS Period_FX_rate,
  cast(g.RevenueAmount / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2)) AS RevenueAmount_Euro,
  g.GP1,
  cast(g.GP1 / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))  GP1_Euro,
  --Added Cost Amount
  g.Cost AS COGS,
  cast(g.Cost / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))  AS COGS_Euro,
  'Revenue_Adj_NU' as GL_Group,
  0 as TopCostFlag
  --  ,IFG_Mapping
 FROM cte g

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
 LEFT JOIN 
   max_fx_rates mx 
 ON
   g.CurrencyCode = cast(mx.Currency as string)""").createOrReplaceTempView('NU_Journal')


# COMMAND ----------

# SL

spark.sql(f"""
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
   NULL AS Technology,
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
   NULL AS EndCustomer,
   NULL AS IndustryVertical,
   g.CurrencyCode,
   g.RevenueAmount,
   coalesce(e.Period_FX_rate, mx.Period_FX_Rate) AS Period_FX_rate,
  cast(g.RevenueAmount / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2)) AS RevenueAmount_Euro,
   g.GP1,
   CASE 
       WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) 
      then cast(g.GP1 / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))
      ELSE coalesce(GP1_Trueup, cast(g.GP1 / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2)))
  END  AS GP1_Euro,
  -- coalesce(GP1_Trueup, cast(g.GP1 / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))) GP1_Euro,
   --Added Cost Amount
   g.CostAmount AS COGS,
   cast(g.CostAmount / ifnull(e.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10,2))AS COGS_Euro,
   case when g.VendorNameInternal in('Mouse & Bear Solutions Ltd','Blackthorne International Transport Ltd','Transport','Nuvias Internal Logistics') then 'Logistics'
         when g.SKUInternal in('TRADEFAIR_A','TRAVELEXP') then 'Marketing'
         when g.SKUInternal ='BEBAT' THEN 'Others'
         when g.ProductTypeInternal ='Shipping & Delivery Income' then 'Logistics'
         when g.ProductTypeInternal in('Quarterly Rebate','Instant Rebate') then 'Rebate'
         when lower(g.VendorNameInternal) like '%- starlink%' then 'Service' 
   else 'Revenue'end as GL_Group
   ,0 as TopCostFlag
  --  ,IFG_Mapping
 FROM gold_{ENVIRONMENT}.obt.globaltransactions_sl_gp1 g

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
 LEFT JOIN 
   max_fx_rates mx 
 ON
   g.CurrencyCode = cast(mx.Currency as string)
"""
).createOrReplaceTempView('SL')

# COMMAND ----------

# VU

spark.sql(f"""SELECT
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
  NULL AS Technology,
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
  to_date('1900-01-01') AS VendorStartDate,
  g.ResellerCode,
  g.ResellerNameInternal,
  g.ResellerGeographyInternal,
  to_date('1900-01-01') AS ResellerStartDate,
  g.ResellerGroupCode,
  g.ResellerGroupName,
  to_date('1900-01-01') AS ResellerGroupStartDate,
  NULL AS EndCustomer,
  NULL AS IndustryVertical,
  g.CurrencyCode,
  g.RevenueAmount,
  CASE
    WHEN (
      g.GroupEntityCode = 'VU'
      AND g.CurrencyCode = 'EUR'
    ) THEN 1
    ELSE ifnull(e1.Period_FX_rate, mx.Period_FX_Rate)
  END AS Period_FX_rate,
  cast( g.RevenueAmount / ifnull(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10, 2)) AS RevenueAmount_Euro,
 cast(g.GP1 / ifnull(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10, 2))AS GP1,
CASE
  WHEN TransactionDate BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE) AND LAST_DAY(CURRENT_DATE) THEN
    CAST(g.GP1 / IFNULL(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10, 2))
  ELSE 
    COALESCE(GP1_Trueup, CAST(g.GP1 / IFNULL(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10, 2)))
END AS GP1_Euro,
 
  --Added Cost Amount
  g.CostAmount AS COGS,
cast(g.CostAmount / ifnull(e1.Period_FX_rate, mx.Period_FX_Rate) AS DECIMAL(10, 2) ) AS COGS_Euro,
  case
    when g.VendorNameInternal in(
      'Mouse & Bear Solutions Ltd',
      'Blackthorne International Transport Ltd',
      'Transport',
      'Nuvias Internal Logistics'
    ) then 'Logistics'
    when g.SKUInternal in('TRADEFAIR_A', 'TRAVELEXP') then 'Marketing'
    when g.SKUInternal = 'BEBAT' THEN 'Others'
    when g.ProductTypeInternal = 'Shipping & Delivery Income' then 'Logistics'
    when g.ProductTypeInternal in('Quarterly Rebate', 'Instant Rebate') then 'Rebate'
    else 'Revenue'
  end as GL_Group,
  0 as TopCostFlag
FROM
  gold_{ ENVIRONMENT }.obt.globaltransactions_vu_gp1 g --Only for VU and entitycode 'NOTINTAGETIK'
  LEFT JOIN (
    SELECT
      DISTINCT Calendar_Year,
      Month,
      Currency,
      Period_FX_rate
    FROM
      gold_{ ENVIRONMENT }.obt.exchange_rate
    WHERE
      ScenarioGroup = 'Actual'
  ) e1 ON e1.Calendar_Year = cast(year(g.TransactionDate) as string)
  AND e1.Month = right(
    concat('0', cast(month(g.TransactionDate) as string)),
    2
  )
  AND g.CurrencyCode = cast(e1.Currency as string)
  LEFT JOIN max_fx_rates mx ON g.CurrencyCode = cast(mx.Currency as string)"""
).createOrReplaceTempView('VU')

# COMMAND ----------

spark.sql(f"""
          
CREATE OR REPLACE TABLE platinum_{ENVIRONMENT}.obt.globaltransactions as 
with cte as (
    SELECT * FROM IG
    UNION ALL 
    SELECT * FROM NU
    UNION ALL 
    SELECT * FROM NU_Journal
    UNION ALL 
    SELECT * FROM SL
    UNION ALL
    SELECT * FROM VU
)
select 
a.GroupEntityCode
,a.EntityCode
,a.DocumentNo
,a.TransactionDate
,a.SalesOrderDate
,a.SalesOrderID
,a.SalesOrderItemID
,a.SKUInternal
,a.SKUMaster
,a.Description
,a.Technology
,a.ProductTypeInternal
,coalesce(b.ProductTypeMaster, a.ProductTypeMaster) ProductTypeMaster
,coalesce(b.CommitmentDuration1Master, a.CommitmentDuration1Master) CommitmentDuration1Master
,coalesce(b.CommitmentDuration2Master, a.CommitmentDuration2Master) CommitmentDuration2Master
,coalesce(b.BillingFrequencyMaster, a.BillingFrequencyMaster) BillingFrequencyMaster
,coalesce(b.ConsumptionModelMaster, a.ConsumptionModelMaster) ConsumptionModelMaster
,a.VendorCode
,a.VendorNameInternal
,a.VendorNameMaster
,a.VendorGeography
,a.VendorStartDate
,a.ResellerCode
,a.ResellerNameInternal
,a.ResellerGeographyInternal
,a.ResellerStartDate
,a.ResellerGroupCode
,a.ResellerGroupName
,a.ResellerGroupStartDate
,a.EndCustomer
,a.IndustryVertical
,a.CurrencyCode
,a.RevenueAmount
,a.Period_FX_rate
,a.RevenueAmount_Euro
,a.GP1
,a.GP1_Euro
,a.COGS
,a.COGS_Euro
,a.GL_Group
,a.TopCostFlag
,b.is_matched 
,b.matched_type
,b.matched_arr_type
from cte a
left join globaltransactions_arr_history b on 
    COALESCE(a.SKUInternal, 'NA') = COALESCE(b.SKUInternal, 'NA')
    AND COALESCE(a.SKUMaster, 'NA') = COALESCE(b.SKUMaster, 'NA')
    AND COALESCE(a.VendorNameInternal, 'NA') = COALESCE(b.VendorNameInternal, 'NA')
    AND COALESCE(a.VendorNameMaster, 'NA') = COALESCE(b.VendorNameMaster, 'NA')
    AND (date_format(a.TransactionDate, 'yyyy-MM')) = b.TransactionDateYYYYMM
""")
