# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

try:
    spark.sql('DROP VIEW IF EXISTS exchange_rate')
except:
    None

# COMMAND ----------

spark.sql(f"""CREATE  TABLE if not exists gold_{ENVIRONMENT}.obt.exchange_rate
  ( ScenarioGroup   STRING
      COMMENT 'scenario'
    ,Scenario   STRING
      COMMENT 'scenario'
    ,Period  STRING
      COMMENT 'period '
    ,Calendar_Year STRING
      COMMENT 'year'
    ,Month STRING
      COMMENT 'month'
    ,Currency STRING
    ,COD_AZIENDA STRING
      COMMENT 'entity'
    ,Period_FX_rate DECIMAL(18,4)
      COMMENT 'period average rate'
    ,DateID date
      COMMENT 'date key'
  )
COMMENT 'This is table for FX rate'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")

# COMMAND ----------

spark.sql(
    f"""
        
with FX AS (
  SELECT
    distinct case
      when COD_SCENARIO like '%ACT%' THEN 'Actual'
      when COD_SCENARIO like '%FC%' THEN 'Forecast'
      when COD_SCENARIO like '%BUD%' THEN 'Plan'
    end as ScenarioGroup,
    COD_SCENARIO AS Scenario,
    COD_PERIODO Period,
    case
      when COD_PERIODO between '10'
      and '12' then LEFT(COD_SCENARIO, 4)
      else cast(LEFT(COD_SCENARIO, 4) as int) -1
    end as Calendar_Year,
    case
      when COD_PERIODO between '01'
      and '09' then RIGHT(
        CONCAT(
          '0',
          cast(CAST((COD_PERIODO + 3) AS INT) as STRING)
        ),
        2
      )
      when COD_PERIODO between '10'
      and '12' then RIGHT(
        CONCAT(
          '0',
          cast(CAST((COD_PERIODO -9) AS INT) as STRING)
        ),
        2
      )
    end as Month,
    dati_cambio.COD_VALUTA as Currency,
    azienda.COD_AZIENDA,
    cast(CAMBIO_PERIODO as decimal(18, 4)) as Period_FX_rate,
    concat_ws(
      '-',
      case
        when COD_PERIODO between '10'
        and '12' then LEFT(COD_SCENARIO, 4)
        else cast(LEFT(COD_SCENARIO, 4) as int) -1
      end,case
        when COD_PERIODO between '01'
        and '09' then RIGHT(
          CONCAT(
            '0',
            cast(CAST((COD_PERIODO + 3) AS INT) as STRING)
          ),
          2
        )
        when COD_PERIODO between '10'
        and '12' then RIGHT(
          CONCAT(
            '0',
            cast(CAST((COD_PERIODO -9) AS INT) as STRING)
          ),
          2
        )
      end,
      '01'
    ) as DateID
  FROM
    silver_{ENVIRONMENT}.tag02.dati_cambio
    left join silver_{ENVIRONMENT}.tag02.azienda ON dati_cambio.COD_VALUTA = azienda.COD_VALUTA
  where
    LEFT(COD_SCENARIO, 4) rlike '[0-9]'
    and COD_SCENARIO LIKE '%ACT%'
    and (COD_SCENARIO like '%PFA-04' or  (COD_SCENARIO ='2025ACT-PFA-01'))
    AND COD_SCENARIO not like '%AUD%'
    AND COD_SCENARIO not like '%OB%'
    and dati_cambio.Sys_Silver_IsCurrent = 1
    and azienda.Sys_Silver_IsCurrent = 1
)
select * from FX 
 """
).createOrReplaceTempView('FX')

# COMMAND ----------

import pyspark.sql.functions as F

# create helper date column
(spark.sql(f""" select sequence(to_date(concat_ws('-',2020,'04','01')), to_date(concat_ws('-',year(now()),month(now()),'01')), interval 1 month) as DateID""")
 .withColumn("DateID", F.explode(F.col("DateID")))
 .createOrReplaceTempView('date'))


# COMMAND ----------

df = spark.sql(f"""with cte as 
(SELECT 
FX.ScenarioGroup,
fx.Scenario,
fx.Period,
fx.Calendar_Year,
fx.Month,
fx.Currency,
coalesce(fx.COD_AZIENDA,DATE.COD_AZIENDA) AS COD_AZIENDA,
FX.Period_FX_rate,
DATE.DateID
 FROM (
    select distinct 
    COD_AZIENDA,
    DATE.DateID
    from FX
    cross join
    DATE
 ) DATE
LEFT JOIN FX ON DATE.DateID = FX.DateID
AND DATE.COD_AZIENDA = FX.COD_AZIENDA)

select * from cte""")

# COMMAND ----------

from pyspark.sql import *

df = df.withColumn('Period_FX_rate',F.when(df.Period_FX_rate>0,df.Period_FX_rate).otherwise(F.last(df.Period_FX_rate,ignorenulls=True).over(Window.orderBy(["COD_AZIENDA",'DateID']))))

# COMMAND ----------

from pyspark.sql.window import Window

df = (df
 .withColumn('Calendar_Year', F.last('Calendar_Year', ignorenulls=True).over(
     Window.partitionBy(['COD_AZIENDA']).orderBy(['COD_AZIENDA','DateID']).rangeBetween(Window.unboundedPreceding, 0)))
 .withColumn('ScenarioGroup', F.last('ScenarioGroup', ignorenulls=True).over(
     Window.partitionBy(['COD_AZIENDA']).orderBy(['COD_AZIENDA','DateID']).rangeBetween(Window.unboundedPreceding, 0)))
 .withColumn('Scenario', F.last('Scenario', ignorenulls=True).over(
     Window.partitionBy(['COD_AZIENDA']).orderBy(['COD_AZIENDA','DateID']).rangeBetween(Window.unboundedPreceding, 0)))
 .withColumn('Currency', F.last('Currency', ignorenulls=True).over(
     Window.partitionBy(['COD_AZIENDA']).orderBy(['COD_AZIENDA','DateID']).rangeBetween(Window.unboundedPreceding, 0)))
 .withColumn('Month', F.coalesce(df.Month, F.date_format("DateID", "MM")))
 .withColumn('Period', F.coalesce(df.Period, F.when(F.date_format("DateID", "MM")<4 ,F.date_format("DateID", "MM")+9
                                                   ).otherwise(F.date_format("DateID", "MM")-3)))
)


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.read.table("exchange_rate")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("exchange_rate")
