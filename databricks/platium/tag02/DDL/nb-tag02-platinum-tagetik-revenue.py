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
# MAGIC CREATE OR REPLACE VIEW staging_tagetik_revenue AS
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
# MAGIC CREATE OR REPLACE VIEW staging_tagetik_revenue_adjusted_periods AS
# MAGIC
# MAGIC SELECT CAST(date_format(CAST((CASE WHEN x.max_period+1 BETWEEN 1 AND 9 THEN DATEADD(YEAR,-1,(DATEADD(MONTH,3,CONCAT(LEFT(x.scenario_code,4),'-', RIGHT(CONCAT('0',CAST(x.max_period + 1 AS STRING)),2),'-01'))))
# MAGIC                                    WHEN x.max_period+1 BETWEEN 10 AND 12 THEN DATEADD(MONTH,-9,CONCAT(LEFT(x.scenario_code, 4),'-',RIGHT(CONCAT('0',CAST(x.max_period + 1 AS STRING)),2),'-01')) END) AS DATE), 'yyyyMMdd') AS INT) AS date_id,
# MAGIC        RIGHT(CONCAT('0',CAST(x.max_period + 1 AS STRING)),2) AS period,
# MAGIC        y.currency_code,
# MAGIC        y.account_code,
# MAGIC        y.special_deal_code,
# MAGIC        y.region_code,
# MAGIC        y.vendor_code,
# MAGIC        y.cost_centre_code,
# MAGIC        y.scenario_code,
# MAGIC        y.entity_code,
# MAGIC        y.category,
# MAGIC        CAST(0 AS DECIMAL(20,2)) AS revenue,
# MAGIC        der.exchange_rate
# MAGIC FROM staging_tagetik_revenue y
# MAGIC INNER JOIN (
# MAGIC SELECT CAST(MAX(Period) AS INT) AS max_period,
# MAGIC        currency_code,
# MAGIC        account_code,
# MAGIC        special_deal_code,
# MAGIC        region_code,
# MAGIC        vendor_code,
# MAGIC        cost_centre_code,
# MAGIC        scenario_code,
# MAGIC        entity_code
# MAGIC FROM staging_tagetik_revenue
# MAGIC GROUP BY ALL
# MAGIC HAVING MAX(Period) != 12
# MAGIC ) x
# MAGIC   ON y.period = x.max_period
# MAGIC  AND y.currency_code = x.currency_code
# MAGIC  AND y.account_code = x.account_code
# MAGIC  AND y.special_deal_code = x.special_deal_code
# MAGIC  AND y.region_code = x.region_code
# MAGIC  AND y.vendor_code = x.vendor_code
# MAGIC  AND y.cost_centre_code = x.cost_centre_code
# MAGIC  AND y.scenario_code = x.scenario_code
# MAGIC  AND y.entity_code = x.entity_code
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_exchange_rate der
# MAGIC   ON CONCAT(y.scenario_code,'_',RIGHT(CONCAT('0',CAST(x.max_period + 1 AS STRING)),2),'_',y.currency_code) = der.exchange_rate_code
# MAGIC  AND CAST(NOW() AS TIMESTAMP) BETWEEN der.start_datetime AND COALESCE(der.end_datetime,'9999-12-31')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW tagetik_revenue AS
# MAGIC
# MAGIC SELECT y.date_id,
# MAGIC        dd.date,
# MAGIC        CONCAT(dd.short_month_name,' - ',CAST(dd.year AS STRING)) AS short_month_name_year,
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
# MAGIC        y.revenue - coalesce(y.next_revenue,0) AS monthly_revenue_in_lcy,
# MAGIC        ROUND(CAST(((y.revenue - coalesce(y.next_revenue,0)) * 1 / y.exchange_rate) AS DECIMAL(20,2)),3) AS monthly_revenue_in_euros,
# MAGIC       (CASE WHEN atyp.total_revenue = 1 THEN monthly_revenue_in_lcy ELSE 0 END) AS total_revenue_in_lcy,
# MAGIC       (CASE WHEN atyp.total_revenue = 1 THEN monthly_revenue_in_euros ELSE 0 END) AS total_revenue_in_euros,
# MAGIC       (CASE WHEN atyp.total_cogs = 1 THEN monthly_revenue_in_lcy ELSE 0 END) AS total_cogs_in_lcy,
# MAGIC       (CASE WHEN atyp.total_cogs = 1 THEN monthly_revenue_in_euros ELSE 0 END) AS total_cogs_in_euros,
# MAGIC       (CASE WHEN atyp.gp1 = 1 THEN monthly_revenue_in_lcy ELSE 0 END) AS total_gp1_in_lcy,
# MAGIC       (CASE WHEN atyp.gp1 = 1 THEN monthly_revenue_in_euros ELSE 0 END) AS total_gp1_in_euros
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
# MAGIC        LAG (x.revenue) OVER (PARTITION BY x.currency_code, x.account_code, x.special_deal_code, x.region_code, x.vendor_code, x.cost_centre_code, x.scenario_code, x.entity_code, x.category ORDER BY x.date_id, x.period ) AS next_revenue,
# MAGIC        x.exchange_rate
# MAGIC FROM 
# MAGIC (
# MAGIC SELECT str.date_id,
# MAGIC        str.period,
# MAGIC        str.currency_code,
# MAGIC        str.account_code,
# MAGIC        str.special_deal_code,
# MAGIC        str.region_code,
# MAGIC        str.vendor_code,
# MAGIC        str.cost_centre_code,
# MAGIC        str.scenario_code,
# MAGIC        str.entity_code,
# MAGIC        str.category,
# MAGIC        str.revenue,
# MAGIC        str.exchange_rate
# MAGIC FROM staging_tagetik_revenue str
# MAGIC UNION ALL
# MAGIC SELECT strap.date_id,
# MAGIC        strap.period,
# MAGIC        strap.currency_code,
# MAGIC        strap.account_code,
# MAGIC        strap.special_deal_code,
# MAGIC        strap.region_code,
# MAGIC        strap.vendor_code,
# MAGIC        strap.cost_centre_code,
# MAGIC        strap.scenario_code,
# MAGIC        strap.entity_code,
# MAGIC        strap.category,
# MAGIC        strap.revenue,
# MAGIC        strap.exchange_rate
# MAGIC FROM staging_tagetik_revenue_adjusted_periods strap
# MAGIC ) x
# MAGIC ) y
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.lup_account_type atyp
# MAGIC     ON y.account_code = atyp.account_code
# MAGIC LEFT OUTER JOIN platinum_${tableObject.environment}.tag02.date dd
# MAGIC     ON y.date_id = dd.date_id
# MAGIC WHERE y.scenario_code LIKE '%ACT%'
# MAGIC   AND (y.scenario_code LIKE '%04' OR y.scenario_code ='2025ACT-PFA-01' )
# MAGIC