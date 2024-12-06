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
# MAGIC CREATE OR REPLACE VIEW tagetik_revenue_actuals_intra_month AS
# MAGIC
# MAGIC SELECT date_id,
# MAGIC        date,
# MAGIC        short_month_name_year,
# MAGIC        period,
# MAGIC        currency_code,
# MAGIC        account_code,
# MAGIC        special_deal_code,
# MAGIC        region_code,
# MAGIC        vendor_code,
# MAGIC        cost_centre_code,
# MAGIC        scenario_code,
# MAGIC        entity_code,
# MAGIC        category,
# MAGIC        monthly_revenue_in_lcy,
# MAGIC        monthly_revenue_in_euros,
# MAGIC        total_revenue_in_lcy,
# MAGIC        total_revenue_in_euros,
# MAGIC        total_cogs_in_lcy,
# MAGIC        total_cogs_in_euros,
# MAGIC        total_gp1_in_lcy,
# MAGIC        total_gp1_in_euros,
# MAGIC        CAST(NULL AS STRING) AS GL_Group,
# MAGIC        CAST(NULL AS STRING) ResellerCode,
# MAGIC        CAST(NULL AS STRING) ResellerNameInternal,
# MAGIC        CAST(NULL AS STRING) ResellerGroupCode,
# MAGIC        CAST(NULL AS STRING) ResellerGroupName,
# MAGIC        CAST(NULL AS STRING) Technology,
# MAGIC        CAST(NULL AS STRING) EndCustomer,
# MAGIC        CAST(NULL AS STRING) IndustryVertical,
# MAGIC        CAST('Tagetik' AS STRING) AS data_source,
# MAGIC        CAST(NULL AS STRING) GroupEntityCode
# MAGIC FROM platinum_${tableObject.environment}.tag02.tagetik_revenue_actuals
# MAGIC WHERE date <= (SELECT tagetik_data_cut_off_date FROM platinum_${tableObject.environment}.tag02.finance_tagetik_cut_over_date)
# MAGIC UNION ALL
# MAGIC SELECT CONCAT(YEAR(TransactionDate),RIGHT(CONCAT('0',MONTH(TransactionDate)),2),RIGHT(CONCAT('0',DAY(TransactionDate)),2)) AS date_id,
# MAGIC        TransactionDate AS date,
# MAGIC        CONCAT(dd.short_month_name,' - ',CAST(dd.year AS STRING)) AS short_month_name_year,
# MAGIC        -1 AS period,
# MAGIC        CurrencyCode AS currency_code,
# MAGIC        'N/A' AS account_code,
# MAGIC        'N/A' AS special_deal_code,
# MAGIC        EntityCode AS region_code,
# MAGIC        VendorCode AS vendor_code,
# MAGIC        'N/A' AS cost_centre_code,
# MAGIC        'N/A' AS scenario_code,
# MAGIC        EntityCode AS entity_code,
# MAGIC        'N/A' AS category,
# MAGIC        RevenueAmount AS monthly_revenue_in_lcy,
# MAGIC        RevenueAmount_Euro AS monthly_revenue_in_euros,
# MAGIC        RevenueAmount AS total_revenue_in_lcy,
# MAGIC        RevenueAmount_Euro AS total_revenue_in_euros,
# MAGIC        COGS AS total_cogs_in_lcy,
# MAGIC        COGS_Euro AS total_cogs_in_euros,
# MAGIC        GP1 AS total_gp1_in_lcy,
# MAGIC        GP1_Euro AS total_gp1_in_euros,
# MAGIC        GL_Group,
# MAGIC        ResellerCode,
# MAGIC        ResellerNameInternal,
# MAGIC        ResellerGroupCode,
# MAGIC        ResellerGroupName,
# MAGIC        Technology,
# MAGIC        EndCustomer,
# MAGIC        IndustryVertical,
# MAGIC        CAST('OBT' AS STRING) AS data_source,
# MAGIC        GroupEntityCode
# MAGIC FROM platinum_${tableObject.environment}.obt.globaltransactions a
# MAGIC LEFT OUTER JOIN platinum_${tableObject.environment}.tag02.date dd
# MAGIC     ON CONCAT(YEAR(TransactionDate),RIGHT(CONCAT('0',MONTH(TransactionDate)),2),RIGHT(CONCAT('0',DAY(TransactionDate)),2)) = dd.date_id
# MAGIC WHERE TransactionDate > (SELECT tagetik_data_cut_off_date FROM platinum_${tableObject.environment}.tag02.finance_tagetik_cut_over_date)
# MAGIC   AND TransactionDate <= NOW()
# MAGIC
