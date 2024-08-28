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
# MAGIC CREATE OR REPLACE VIEW staging_tagetik_revenue_budget AS
# MAGIC SELECT ftr.date_sk AS date_id,
# MAGIC        ftr.period,
# MAGIC        der.currency_code,
# MAGIC        da.account_code,
# MAGIC        ftr.special_deal_code,
# MAGIC        dr.region_code,
# MAGIC        dv.vendor_code,
# MAGIC        dcc.cost_centre_code,
# MAGIC        ds.scenario_code,
# MAGIC       (CASE WHEN de.entity_code = 'AT1' THEN 'DE1'
# MAGIC             WHEN de.entity_code = 'BE1' THEN 'NL1'
# MAGIC             WHEN de.entity_code = 'FR2' THEN 'FR1'
# MAGIC             WHEN de.entity_code IN ('IE1', 'UK2') THEN 'VU'
# MAGIC             WHEN dr.region_code = 'HL5' THEN 'HL5'
# MAGIC             ELSE de.entity_code END) AS entity_code,
# MAGIC        ftr.category,
# MAGIC        SUM(ftr.revenue_lcy) AS revenue,
# MAGIC        der.exchange_rate
# MAGIC FROM gold_${tableObject.environment}.tag02.fact_tagetik_revenue ftr
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
# MAGIC AND ds.scenario_code IN ('2025BUD-YEAR-01', '2024BUD-YEAR-02')
# MAGIC GROUP BY ftr.date_sk,
# MAGIC        ftr.period,
# MAGIC        der.currency_code,
# MAGIC        da.account_code,
# MAGIC        ftr.special_deal_code,
# MAGIC        dr.region_code,
# MAGIC        dv.vendor_code,
# MAGIC        dcc.cost_centre_code,
# MAGIC        ds.scenario_code,
# MAGIC       (CASE WHEN de.entity_code = 'AT1' THEN 'DE1'
# MAGIC             WHEN de.entity_code = 'BE1' THEN 'NL1'
# MAGIC             WHEN de.entity_code = 'FR2' THEN 'FR1'
# MAGIC             WHEN de.entity_code IN ('IE1', 'UK2') THEN 'VU'
# MAGIC             WHEN dr.region_code = 'HL5' THEN 'HL5'
# MAGIC             ELSE de.entity_code END),
# MAGIC        ftr.category,
# MAGIC        der.exchange_rate
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW tagetik_revenue_budget AS
# MAGIC
# MAGIC SELECT y.date_id AS date_id,
# MAGIC        dd.date AS date,
# MAGIC        CONCAT(dd.short_month_name,' - ',CAST(dd.year AS STRING)) AS short_month_name_year,
# MAGIC        y.period AS period,
# MAGIC        y.currency_code AS currency_code,
# MAGIC        y.account_code AS account_code,
# MAGIC        y.special_deal_code AS special_deal_code,
# MAGIC        y.region_code AS region_code,
# MAGIC        y.vendor_code AS vendor_code,
# MAGIC        y.cost_centre_code AS cost_centre_code,
# MAGIC        y.scenario_code AS scenario_code,
# MAGIC        y.entity_code AS entity_code,
# MAGIC        y.category AS category,
# MAGIC        y.revenue - coalesce(y.prev_revenue,0) AS monthly_revenue_in_lcy,
# MAGIC        ROUND(CAST(((y.revenue - coalesce(y.prev_revenue,0)) * 1 / y.exchange_rate) AS DECIMAL(20,2)),3) AS monthly_revenue_in_euros,
# MAGIC       (CASE WHEN account_code in (309988,320988) THEN monthly_revenue_in_lcy ELSE 0 END) AS total_revenue_in_lcy,
# MAGIC       (CASE WHEN account_code in (309988,320988) THEN monthly_revenue_in_euros ELSE 0 END) AS total_revenue_in_euros,
# MAGIC       (CASE WHEN account_code in (310288,470088,470288,440188,310688,421988,401988) THEN monthly_revenue_in_lcy ELSE 0 END) AS total_cogs_in_lcy,
# MAGIC       (CASE WHEN account_code in (310288,470088,470288,440188,310688,421988,401988) THEN monthly_revenue_in_euros ELSE 0 END) AS total_cogs_in_euros,
# MAGIC       (CASE WHEN account_code in (309988,320988,310288,470088,470288,440188,310688,421988,401988) THEN monthly_revenue_in_lcy ELSE 0 END) AS total_gp1_in_lcy,
# MAGIC       (CASE WHEN account_code in (309988,320988,310288,470088,470288,440188,310688,421988,401988) THEN monthly_revenue_in_euros ELSE 0 END) AS total_gp1_in_euros
# MAGIC FROM
# MAGIC (
# MAGIC SELECT x.date_id,
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
# MAGIC        LAG (x.revenue) OVER (PARTITION BY x.currency_code, x.account_code, x.special_deal_code, x.region_code, x.vendor_code, x.cost_centre_code, x.scenario_code, x.entity_code, x.category ORDER BY x.date_id, x.period ) AS prev_revenue,
# MAGIC        x.exchange_rate
# MAGIC FROM staging_tagetik_revenue_budget x
# MAGIC ) y
# MAGIC LEFT OUTER JOIN platinum_${tableObject.environment}.tag02.date dd ON y.date_id = dd.date_id
