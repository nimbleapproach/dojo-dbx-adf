# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

#  [yz] 08.04.2024 : create value entry view for cost adjustments
spark.sql(f"""
create or replace view gold_{ENVIRONMENT}.obt.value_entry_adjustments as

select 
CONCAT( RIGHT (ve.Sys_DatabaseName,2),'1')  AS EntityCode,
case when ve.Sys_DatabaseName in( 'ReportsDE','ReportsAT') AND region.DimensionValueCode = 'AT' THEN 'AT1'
    WHEN ve.Sys_DatabaseName = 'ReportsNL' AND region.DimensionValueCode = 'BE' THEN 'BE1'
    ELSE CONCAT( RIGHT (ve.Sys_DatabaseName,2),'1') END AS RegionCode,
ve.Sys_DatabaseName,
ItemNo_,
ven.DimensionValueCode as VendorCode,
ven_name.Name as VendorName,
PostingDate,
DocumentNo_,
sum(CostPostedtoG_L)as CostPostedtoG_L,
sum(cast(CostPostedtoG_L/ e.Period_FX_rate AS DECIMAL(10,2))) as CostPostedtoG_L_EUR
from silver_{ENVIRONMENT}.igsql03.value_entry ve

/*left join (
  select  * from silver_{ENVIRONMENT}.igsql03.dimension_set_entry
  where DimensionCode = 'RPTREGION'
  and Sys_Silver_IsCurrent =1
)region 
on ve.Sys_DatabaseName = region.Sys_DatabaseName
and ve.DimensionSetID=region.DimensionSetID*/

LEFT JOIN (
    select  * from silver_{ENVIRONMENT}.igsql03.dimension_set_entry
  where DimensionCode = 'VENDOR'
  and Sys_Silver_IsCurrent =1
)ven
on ve.Sys_DatabaseName = ven.Sys_DatabaseName
and ve.DimensionSetID=ven.DimensionSetID

left join(
      select  * from silver_{ENVIRONMENT}.igsql03.dimension_value
  where DimensionCode = 'VENDOR'
  and Sys_Silver_IsCurrent =1
)ven_name 
on ven.DimensionValueCode = ven_name.Code
and ven.Sys_DatabaseName = ven_name.Sys_DatabaseName

LEFT JOIN (
    select  * from silver_{ENVIRONMENT}.igsql03.dimension_set_entry
  where DimensionCode = 'RPTREGION'
  and Sys_Silver_IsCurrent =1
)region
on region.Sys_DatabaseName = ve.Sys_DatabaseName
and ve.DimensionSetID=region.DimensionSetID

LEFT JOIN
  gold_{ENVIRONMENT}.obt.exchange_rate e
ON
  e.Calendar_Year = cast(year(ve.PostingDate) as string)
AND
  e.Month = right(concat('0',cast(month(ve.PostingDate) as string)),2)
AND
/*[YZ] 15.03.2024 : Add Replace BE1 with NL1 since it is not a valid entity in tagetik for fx*/
 CONCAT( RIGHT (ve.Sys_DatabaseName,2),'1')      = e.COD_AZIENDA
AND
  e.ScenarioGroup = 'Actual'
where Adjustment = 1
and ve.Sys_Silver_IsCurrent =1
and DocumentType in (2,4)

group by all
""")
