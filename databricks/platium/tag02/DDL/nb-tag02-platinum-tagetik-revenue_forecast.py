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
# MAGIC CREATE OR REPLACE VIEW tagetik_revenue_forecast AS
# MAGIC SELECT
# MAGIC   y.date_id AS date_id,
# MAGIC   dd.date AS date,
# MAGIC   CONCAT(
# MAGIC     dd.short_month_name,
# MAGIC     ' - ',
# MAGIC     CAST(dd.year AS STRING)
# MAGIC   ) AS short_month_name_year,
# MAGIC   y.period AS period,
# MAGIC   y.currency_code AS currency_code,
# MAGIC   y.account_code AS account_code,
# MAGIC   y.special_deal_code AS special_deal_code,
# MAGIC   y.region_code AS region_code,
# MAGIC   coalesce(atyp.account_vendor_override, y.vendor_code) AS vendor_code,
# MAGIC   y.cost_centre_code AS cost_centre_code,
# MAGIC   y.scenario_code AS scenario_code,
# MAGIC   y.entity_code AS entity_code,
# MAGIC   y.category AS category,
# MAGIC   y.revenue - coalesce(y.prev_revenue, 0) AS monthly_revenue_in_lcy,
# MAGIC   ROUND(
# MAGIC     CAST(
# MAGIC       (
# MAGIC         (y.revenue - coalesce(y.prev_revenue, 0)) * 1 / y.exchange_rate
# MAGIC       ) AS DECIMAL(20, 2)
# MAGIC     ),
# MAGIC     3
# MAGIC   ) AS monthly_revenue_in_euros,
# MAGIC   (CASE WHEN atyp.total_revenue = 1 THEN monthly_revenue_in_lcy ELSE 0 END) AS total_revenue_in_lcy,
# MAGIC   (CASE WHEN atyp.total_revenue = 1 THEN monthly_revenue_in_euros ELSE 0 END) AS total_revenue_in_euros,
# MAGIC   (CASE WHEN atyp.total_cogs = 1 THEN monthly_revenue_in_lcy ELSE 0 END) AS total_cogs_in_lcy,
# MAGIC   (CASE WHEN atyp.total_cogs = 1 THEN monthly_revenue_in_euros ELSE 0 END) AS total_cogs_in_euros,
# MAGIC   (CASE WHEN atyp.gp1 = 1 THEN monthly_revenue_in_lcy ELSE 0 END) AS total_gp1_in_lcy,
# MAGIC   (CASE WHEN atyp.gp1 = 1 THEN monthly_revenue_in_euros ELSE 0 END) AS total_gp1_in_euros
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       x.date_id,
# MAGIC       x.period,
# MAGIC       x.currency_code,
# MAGIC       x.account_code,
# MAGIC       x.special_deal_code,
# MAGIC       x.region_code,
# MAGIC       x.vendor_code,
# MAGIC       x.cost_centre_code,
# MAGIC       x.scenario_code,
# MAGIC       x.entity_code,
# MAGIC       x.category,
# MAGIC       x.revenue,
# MAGIC       LAG (x.revenue) OVER (
# MAGIC         PARTITION BY x.currency_code,
# MAGIC         x.account_code,
# MAGIC         x.special_deal_code,
# MAGIC         x.region_code,
# MAGIC         x.vendor_code,
# MAGIC         x.cost_centre_code,
# MAGIC         x.scenario_code,
# MAGIC         x.entity_code,
# MAGIC         x.category
# MAGIC         ORDER BY
# MAGIC           x.date_id,
# MAGIC           x.period
# MAGIC       ) AS prev_revenue,
# MAGIC       x.exchange_rate
# MAGIC     FROM
# MAGIC       platinum_dev.tag02.staging_tagetik_revenue x
# MAGIC   ) y
# MAGIC   LEFT OUTER JOIN platinum_dev.tag02.date dd ON y.date_id = dd.date_id
# MAGIC   LEFT OUTER JOIN gold_${tableObject.environment}.tag02.lup_account_type atyp
# MAGIC     ON y.account_code = atyp.account_code
# MAGIC   WHERE scenario_code like "%FC%"
