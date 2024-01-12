# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Create exchange rate view in gold
spark.sql(f"""
          
          

CREATE OR Replace VIEW exchange_rate AS
with FX AS
(SELECT
  distinct 
  case when COD_SCENARIO like '%ACT%' THEN 'Actual'
        when COD_SCENARIO like '%FC%' THEN 'Forecast'
        when COD_SCENARIO like '%BUD%' THEN 'Plan' end as ScenarioGroup, 
  COD_SCENARIO AS Scenario,
  COD_PERIODO Period,case
    when COD_PERIODO between '10'
    and '12' then LEFT(COD_SCENARIO, 4)
    else cast(LEFT(COD_SCENARIO, 4) as int) -1
  end as Calendar_Year,
  case
    when COD_PERIODO between '01' and '09' 
    then RIGHT(CONCAT('0' ,cast(CAST((COD_PERIODO +3)AS INT) as STRING)), 2)
    when COD_PERIODO between '10' and '12' 
    then RIGHT(CONCAT('0' ,cast(CAST((COD_PERIODO -9)AS INT) as STRING)), 2)
  end as Month,
  dati_cambio.COD_VALUTA as Currency,
  azienda.COD_AZIENDA,
  cast(CAMBIO_PERIODO as decimal(18, 4)) as Period_FX_rate
FROM
  silver_{ENVIRONMENT}.tag02.dati_cambio
left join  silver_{ENVIRONMENT}.tag02.azienda ON dati_cambio.COD_VALUTA = azienda.COD_VALUTA
where
  LEFT(COD_SCENARIO, 4) rlike '[0-9]'
  and 
    COD_SCENARIO LIKE '%ACT%'
  and COD_SCENARIO like '%PFA-04'
  AND COD_SCENARIO not like '%AUD%'
  AND COD_SCENARIO not like '%OB%'
  and dati_cambio.Sys_Silver_IsCurrent = 1
  and azienda.Sys_Silver_IsCurrent =1
)

select * from FX
where concat(Calendar_Year, '-',Month,'-01' ) < concat(year(now()), '-', right(concat('0',cast(month(now()) as string)),2),'-01' )

union all

select distinct 
ScenarioGroup,
Scenario,
Period,
year(now()) as Calendar_Year,
right(concat('0',cast(month(now()) as string)),2) as Month,
Currency,
COD_AZIENDA,
Period_FX_rate
from FX
where Calendar_Year = case when month(now()) = 1 then year(now())-1 else year(now()) end
and Month = case when month(now()) = 1 then '12' else right(concat('0',cast(month(now())-1 as string)),2) end
""")
