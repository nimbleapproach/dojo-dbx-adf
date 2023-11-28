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
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR Replace VIEW exchange_rate AS
# MAGIC with FX AS
# MAGIC (SELECT
# MAGIC   distinct 
# MAGIC   case when COD_SCENARIO like '%ACT%' THEN 'Actual'
# MAGIC         when COD_SCENARIO like '%FC%' THEN 'Forecast'
# MAGIC         when COD_SCENARIO like '%BUD%' THEN 'Plan' end as ScenarioGroup, 
# MAGIC   COD_SCENARIO AS Scenario,
# MAGIC   COD_PERIODO Period,case
# MAGIC     when COD_PERIODO between '10'
# MAGIC     and '12' then LEFT(COD_SCENARIO, 4)
# MAGIC     else cast(LEFT(COD_SCENARIO, 4) as int) -1
# MAGIC   end as Calendar_Year,
# MAGIC   case
# MAGIC     when COD_PERIODO between '01' and '09' 
# MAGIC     then RIGHT(CONCAT('0' ,cast(CAST((COD_PERIODO +3)AS INT) as STRING)), 2)
# MAGIC     when COD_PERIODO between '10' and '12' 
# MAGIC     then RIGHT(CONCAT('0' ,cast(CAST((COD_PERIODO -9)AS INT) as STRING)), 2)
# MAGIC   end as Month,
# MAGIC   dati_cambio.COD_VALUTA as Currency,
# MAGIC   azienda.COD_AZIENDA,
# MAGIC   cast(CAMBIO_PERIODO as decimal(18, 4)) as Period_FX_rate
# MAGIC FROM
# MAGIC   silver_dev.tag02.dati_cambio
# MAGIC left join  silver_dev.tag02.azienda ON dati_cambio.COD_VALUTA = azienda.COD_VALUTA
# MAGIC where
# MAGIC   LEFT(COD_SCENARIO, 4) rlike '[0-9]'
# MAGIC   and 
# MAGIC     COD_SCENARIO LIKE '%ACT%'
# MAGIC   and COD_SCENARIO like '%PFA-04'
# MAGIC   AND COD_SCENARIO not like '%AUD%'
# MAGIC   AND COD_SCENARIO not like '%OB%'
# MAGIC   and dati_cambio.Sys_Silver_IsCurrent = 1
# MAGIC   and azienda.Sys_Silver_IsCurrent =1

# MAGIC )
# MAGIC
# MAGIC select * from FX
# MAGIC where concat(Calendar_Year, '-',Month,'-01' )< concat(year(now()), '-',month(now())-1,'-01' )
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select distinct 
# MAGIC ScenarioGroup,
# MAGIC Scenario,
# MAGIC Period,
# MAGIC year(now()) as Calendar_Year,
# MAGIC month(now()) as Month,
# MAGIC Currency,
# MAGIC COD_AZIENDA,
# MAGIC Period_FX_rate
# MAGIC  from FX
# MAGIC where Calendar_Year = year(now())
# MAGIC and Month = month(now())-1
# MAGIC

# MAGIC )
# MAGIC
# MAGIC select * from FX
# MAGIC where concat(Calendar_Year, '-',Month,'-01' )< concat(year(now()), '-',month(now()),'-01' )
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select distinct 
# MAGIC ScenarioGroup,
# MAGIC Scenario,
# MAGIC Period,
# MAGIC year(now()) as Calendar_Year,
# MAGIC month(now()) as Month,
# MAGIC Currency,
# MAGIC COD_AZIENDA,
# MAGIC Period_FX_rate
# MAGIC  from FX
# MAGIC where Calendar_Year = year(now())
# MAGIC and Month = month(now())-1
# MAGIC

# MAGIC
# MAGIC
