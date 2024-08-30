# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------


#  Original Date [19/04/2024]
#  Created BY [YZ]
#  Create table to capture the top adjustments posted in GL but not in sales documents

spark.sql(
  f"""create or replace view infinigate_top_cost_adjustments as
with gl as
(select
gle.DocumentNo_,
GLE.PostingDate,
gle.Sys_DatabaseName,
concat(right(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
region.DimensionValueCode AS RegionID,
gle.GlobalDimension1Code as VendorCode,
ven.Name as VendorName,
SUM( gle.Amount) as CostAmount,
CASE WHEN ga.Consol_CreditAcc_ IN (
--- Revenue level adj.
'309988'
,'310188'
,'310288'
,'310388'
,'310488'
,'310588'
,'310688'
,'310788'
,'310888'
,'310988'
,'311088'
,'312088'
,'313088'
,'314088'
,'320988'
,'322988'
,'370988') THEN 'Revenue' ELSE 'GP1'
END AS GL_Group

 from silver_{ENVIRONMENT}.igsql03.g_l_entry gle
 left join silver_{ENVIRONMENT}.igsql03.g_l_account ga
 on gle.G_LAccountNo_ = ga.No_
 and gle.Sys_DatabaseName = ga.Sys_DatabaseName
 and ga.Sys_Silver_IsCurrent=1
 and gle.Sys_Silver_IsCurrent=1
 left join(
    SELECT
        Code,
        Name,
        Sys_DatabaseName
      FROM
        silver_{ENVIRONMENT}.igsql03.dimension_value
      WHERE
        DimensionCode = 'VENDOR'
        AND Sys_Silver_IsCurrent = true
    ) ven ON gle.GlobalDimension1Code = ven.Code
    and ven.Sys_DatabaseName = gle.Sys_DatabaseName
LEFT JOIN silver_{ENVIRONMENT}.igsql03.dimension_set_entry as region
on region.DimensionSetID = gle.DimensionSetID
and region.Sys_DatabaseName = gle.Sys_DatabaseName
and region.Sys_Silver_IsCurrent = 1
and region.DimensionCode = 'RPTREGION'

where gle.Sys_Silver_IsCurrent=1

and ga.Consol_CreditAcc_ in (
--- Revenue level adj.
'309988'
,'310188'
,'310288'
,'310388'
,'310488'
,'310588'
,'310688'
,'310788'
,'310888'
,'310988'
,'311088'
,'312088'
,'313088'
,'314088'
,'320988'
,'322988'
,'370988'

--- GP1 level adj.
,'350988'
,'351988'
,'391988'
,'371988'
,'400988'
,'401988'
,'402988'
,'420988'
,'421988'
,'422988'
,'440188'
,'440288'
,'440388'
,'440588'
,'440688'
,'440788'
,'440888'
,'449988'
,'450888'
,'450988'
,'451988'
,'452788'
,'452888'
,'452988'
,'468988'
,'469988'
,'499988')

group by all
having sum(Amount)<>0)

select 
gl.DocumentNo_
,gl.PostingDate
,gl.Sys_DatabaseName
,case when gl.EntityCode ='DE1' AND gl.RegionID = 'AT' and gl.VendorCode not in ('ZZ_INF','SOW',/*'DAT',*/'ITG', 'RFT') THEN 'AT1'
      when gl.EntityCode ='NL1' AND gl.RegionID = 'BE' THEN 'BE1'
      ELSE gl.EntityCode END AS EntityCode
,gl.VendorCode
,gl.VendorName
,gl.CostAmount
,coalesce(gl.CostAmount, 0)/e.Period_FX_rate as CostAmount_EUR
,GL_Group
from gl 
LEFT JOIN
  gold_{ENVIRONMENT}.obt.exchange_rate e
ON
  e.Calendar_Year = cast(year(gl.PostingDate) as string)
AND
  e.Month = right(concat('0',cast(month(gl.PostingDate) as string)),2)
AND

  CASE WHEN gl.EntityCode = 'BE1' THEN 'NL1' ELSE  gl.EntityCode  END   = e.COD_AZIENDA
AND
  e.ScenarioGroup = 'Actual'
WHERE concat(Sys_DatabaseName,DocumentNo_) NOT IN (
  SELECT DISTINCT concat(Sys_DatabaseName,No_) No_ 
  from silver_{ENVIRONMENT}.igsql03.sales_invoice_header
  where Sys_Silver_IsCurrent=1
 union all
  SELECT DISTINCT concat(Sys_DatabaseName,No_) No_
  from silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header
  where Sys_Silver_IsCurrent=1
)
/*AND  LEFT(DocumentNo_,3 ) NOT IN (
'GER',
'GEG',
'PPI',
'PPC',
'PI-',
'PC-',
'PIB',
'PCB',
'PIO',
'PIV'

)*/

""")

   
