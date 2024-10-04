# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA tag02;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW qbr_profit_loss AS
# MAGIC SELECT CAST(1 AS INT) AS order_id, c.Country, b.Date, 'Gross Revenue' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_5 = 'Total Revenue'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(2 AS INT) AS order_id, c.Country, b.Date, 'Direct Profit (GP1)' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_4 = 'GP1'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(3 AS INT) AS order_id, c.Country, b.Date, 'Net Distribution Profit (GP2)' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_3 = 'GP2'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(4 AS INT) AS order_id, c.Country, b.Date, 'Gross Profit (GP3)' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_2 = 'GP3'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(5 AS INT) AS order_id, c.Country, b.Date, 'Personnel Expenses' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.profit_loss_level_3 = 'Personnel Expenses'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(6 AS INT) AS order_id, c.Country, b.Date, 'Non-FTE OPEX' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.profit_loss_level_3 = 'Non-FTE OPEX'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(7 AS INT) AS order_id, c.Country, b.Date, 'Total OPEX' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.profit_loss_level_2 = 'Total OPEX'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(8 AS INT) AS order_id, c.Country, b.Date, 'Adj. EBITDA' AS description, SUM(monthly_revenue_in_euros) AS revenue, 0 AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_actuals b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_1 = 'EBITDA (Adjusted)'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(1 AS INT) AS order_id, c.Country, b.Date, 'Gross Revenue' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_5 = 'Total Revenue'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(2 AS INT) AS order_id, c.Country, b.Date, 'Direct Profit (GP1)' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_4 = 'GP1'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(3 AS INT) AS order_id, c.Country, b.Date, 'Net Distribution Profit (GP2)' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_3 = 'GP2'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(4 AS INT) AS order_id, c.Country, b.Date, 'Gross Profit (GP3)' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_2 = 'GP3'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(5 AS INT) AS order_id, c.Country, b.Date, 'Personnel Expenses' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.profit_loss_level_3 = 'Personnel Expenses'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(6 AS INT) AS order_id, c.Country, b.Date, 'Non-FTE OPEX' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.profit_loss_level_3 = 'Non-FTE OPEX'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(7 AS INT) AS order_id, c.Country, b.Date, 'Total OPEX' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.profit_loss_level_2 = 'Total OPEX'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC UNION
# MAGIC SELECT CAST(8 AS INT) AS order_id, c.Country, b.Date, 'Adj. EBITDA' AS description, 0 AS revenue, SUM(monthly_revenue_in_euros) AS budget
# MAGIC FROM platinum_dev.tag02.qbr_account_description_hierarchy a
# MAGIC INNER JOIN platinum_dev.tag02.tagetik_revenue_budget b
# MAGIC   ON a.account_id = b.account_code
# MAGIC INNER JOIN platinum_dev.tag02.region c
# MAGIC   ON b.region_code = c.region_code
# MAGIC WHERE a.level_1 = 'EBITDA (Adjusted)'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE 'E%'
# MAGIC   AND b.Cost_Centre_Code NOT LIKE '%C%'
# MAGIC GROUP BY c.Country, b.Date
# MAGIC
