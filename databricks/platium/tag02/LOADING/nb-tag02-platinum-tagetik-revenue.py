# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

spark.conf.set("tableObject.environment", ENVIRONMENT)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Use SCHEMA tag02

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW tagetik_revenue AS
# MAGIC
# MAGIC SELECT y.date,
# MAGIC        y.date_id,
# MAGIC        y.period,
# MAGIC        y.currency_code,
# MAGIC        y.account_code,
# MAGIC        y.special_deal_code,
# MAGIC        y.region_code,
# MAGIC        y.vendor_code,
# MAGIC        y.cost_centre_code,
# MAGIC        y.scenario_code,
# MAGIC        y.entity_code,
# MAGIC        y.category,
# MAGIC        y.revenue - coalesce(y.next_revenue,0) AS monthly_revenue_lcy,
# MAGIC        ROUND(CAST(((y.revenue - coalesce(y.next_revenue,0)) * 1 / y.exchange_rate) AS DECIMAL(20,2)),3) AS monthly_revenue_lcy_in_euros,
# MAGIC       (CASE WHEN y.scenario_code LIKE '%ACT%' AND ( y.scenario_code LIKE '%04%' OR y.scenario_code = '2025ACT-PFA-01') AND y. 
# MAGIC        scenario_code NOT LIKE '%OB%' AND atyp.total_revenue = 1 THEN 1 ELSE 0 END) AS total_revenue,
# MAGIC       (CASE WHEN y.scenario_code LIKE '%ACT%' AND ( y.scenario_code LIKE '%04%' OR y.scenario_code = '2025ACT-PFA-01') AND y. 
# MAGIC        scenario_code NOT LIKE '%OB%' AND atyp.total_cogs = 1 THEN 1 ELSE 0 END) AS total_cogs,
# MAGIC       (CASE WHEN y.scenario_code LIKE '%ACT%' AND ( y.scenario_code LIKE '%04%' OR y.scenario_code = '2025ACT-PFA-01') AND y. 
# MAGIC        scenario_code NOT LIKE '%OB%' AND atyp.gp1 = 1 THEN 1 ELSE 0 END) AS gp1              
# MAGIC FROM
# MAGIC (
# MAGIC SELECT x.date,
# MAGIC        x.date_id,
# MAGIC        x.period,
# MAGIC        x.currency_code,
# MAGIC        x.account_code,
# MAGIC        x.special_deal_code,
# MAGIC        x.region_code,
# MAGIC        x.vendor_code,
# MAGIC        x.cost_centre_code,
# MAGIC        x.scenario_code,
# MAGIC        x.entity_code,
# MAGIC        x.category,
# MAGIC        x.revenue,
# MAGIC        LAG (x.revenue) OVER (PARTITION BY x.currency_code, x.account_code, x.special_deal_code, x.region_code, x.vendor_code, x.cost_centre_code, x.scenario_code, x.entity_code, x.category ORDER BY x.date_id, x.period ) AS next_revenue,
# MAGIC        x.exchange_rate
# MAGIC FROM 
# MAGIC (
# MAGIC SELECT dd.date,
# MAGIC        dd.date_id,
# MAGIC        ftr.period,
# MAGIC        der.currency_code,
# MAGIC        da.account_code,
# MAGIC        ftr.special_deal_code,
# MAGIC        dr.region_code,
# MAGIC        dv.vendor_code,
# MAGIC        dcc.cost_centre_code,
# MAGIC        ds.scenario_code,
# MAGIC        de.entity_code,
# MAGIC        ftr.category,
# MAGIC        SUM(ftr.revenue_lcy) AS revenue,
# MAGIC        der.exchange_rate
# MAGIC FROM gold_${tableObject.environment}.tag02.fact_tagetik_revenue ftr
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_date dd
# MAGIC     ON ftr.date_sk = dd.date_id
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_exchange_rate der
# MAGIC     ON ftr.exchange_rate_sk = der.dim_exchange_rate_pk 
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_account da
# MAGIC     ON ftr.account_sk = da.dim_account_pk
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_region dr
# MAGIC     ON ftr.region_sk = dr.dim_region_pk
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_vendor dv
# MAGIC     ON ftr.vendor_sk = dv.dim_vendor_pk
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_cost_centre dcc
# MAGIC     ON ftr.cost_centre_sk = dcc.dim_cost_centre_pk
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_scenario ds
# MAGIC     ON ftr.scenario_sk = ds.dim_scenario_pk
# MAGIC INNER JOIN gold_${tableObject.environment}.tag02.dim_entity de
# MAGIC     ON ftr.entity_sk = de.dim_entity_pk
# MAGIC WHERE ftr.Sys_Gold_is_active = 1
# MAGIC GROUP BY dd.date,
# MAGIC        dd.date_id,
# MAGIC        ftr.period,
# MAGIC        der.currency_code,
# MAGIC        da.account_code,
# MAGIC        ftr.special_deal_code,
# MAGIC        dr.region_code,
# MAGIC        dv.vendor_code,
# MAGIC        dcc.cost_centre_code,
# MAGIC        ds.scenario_code,
# MAGIC        de.entity_code,
# MAGIC        ftr.category,
# MAGIC        der.exchange_rate
# MAGIC ) x
# MAGIC ) y
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.account_type atyp
# MAGIC     ON y.account_code = atyp.account_code
# MAGIC
