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
# MAGIC CREATE OR REPLACE VIEW vendor AS
# MAGIC
# MAGIC SELECT vendor_code,
# MAGIC        vendor_name
# MAGIC FROM gold_${tableObject.environment}.tag02.dim_vendor
# MAGIC WHERE is_current = 1
# MAGIC UNION
# MAGIC SELECT DISTINCT a.account_vendor_override AS vendor_code,
# MAGIC                 CONCAT('Account Vendor Override - ',a.account_vendor_override) AS vendor_name
# MAGIC FROM gold_${tableObject.environment}.tag02.lup_account_type a
# MAGIC LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_vendor b
# MAGIC   ON a.account_vendor_override = b.vendor_code
# MAGIC WHERE a.account_vendor_override IS NOT NULL
# MAGIC   AND b.vendor_code IS NULL
# MAGIC
