-- Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

spark.conf.set("tableObject.environment", ENVIRONMENT)

-- COMMAND ----------


Use SCHEMA tag02

-- COMMAND ----------


CREATE OR REPLACE VIEW tagetik_revenue_forecast AS
SELECT
  y.date_id AS date_id,
  dd.date AS date,
  CONCAT(
    dd.short_month_name,
    ' - ',
    CAST(dd.year AS STRING)
  ) AS short_month_name_year,
  y.period AS period,
  y.currency_code AS currency_code,
  y.account_code AS account_code,
  y.special_deal_code AS special_deal_code,
  y.region_code AS region_code,
  y.vendor_code AS vendor_code,
  y.cost_centre_code AS cost_centre_code,
  y.scenario_code AS scenario_code,
  y.entity_code AS entity_code,
  y.category AS category,
  y.revenue - coalesce(y.prev_revenue, 0) AS monthly_revenue_in_lcy,
  ROUND(
    CAST(
      (
        (y.revenue - coalesce(y.prev_revenue, 0)) * 1 / y.exchange_rate
      ) AS DECIMAL(20, 2)
    ),
    3
  ) AS monthly_revenue_in_euros,
  (
    CASE
      WHEN y.account_code IN (310288, 310388, 310188, 310688, 309988, 320988) THEN monthly_revenue_in_lcy
      ELSE 0
    END
  ) AS total_revenue_in_lcy,
  (
    CASE
      WHEN y.account_code IN (310288, 310388, 310188, 310688, 309988, 320988) THEN monthly_revenue_in_euros
      ELSE 0
    END
  ) AS total_revenue_in_euros,
  (
    CASE
      WHEN account_code in (310288, 470088, 470288, 440188, 310688, 421988, 401988) THEN monthly_revenue_in_lcy
      ELSE 0
    END
  ) AS total_cogs_in_lcy,
  (
    CASE
      WHEN account_code in (310288, 470088, 470288, 440188, 310688, 421988, 401988) THEN monthly_revenue_in_euros
      ELSE 0
    END
  ) AS total_cogs_in_euros,
  (
    CASE
      WHEN account_code in (
        309988,
        320988,
        310288,
        470088,
        470288,
        440188,
        310688,
        421988,
        401988
      ) THEN monthly_revenue_in_lcy
      ELSE 0
    END
  ) AS total_gp1_in_lcy,
  (
    CASE
      WHEN account_code in (
        309988,
        320988,
        310288,
        470088,
        470288,
        440188,
        310688,
        421988,
        401988
      ) THEN monthly_revenue_in_euros
      ELSE 0
    END
  ) AS total_gp1_in_euros
FROM
  (
    SELECT
      x.date_id,
      x.period,
      x.currency_code,
      x.account_code,
      x.special_deal_code,
      x.region_code,
      x.vendor_code,
      x.cost_centre_code,
      x.scenario_code,
      x.entity_code,
      x.category,
      x.revenue,
      LAG (x.revenue) OVER (
        PARTITION BY x.currency_code,
        x.account_code,
        x.special_deal_code,
        x.region_code,
        x.vendor_code,
        x.cost_centre_code,
        x.scenario_code,
        x.entity_code,
        x.category
        ORDER BY
          x.date_id,
          x.period
      ) AS prev_revenue,
      x.exchange_rate
    FROM
      platinum_dev.tag02.staging_tagetik_revenue x
  ) y
  LEFT OUTER JOIN platinum_dev.tag02.date dd ON y.date_id = dd.date_id
  WHERE scenario_code like "%FC%"
