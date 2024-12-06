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
# MAGIC WITH cte_tagetik_vendors AS (   -- This pulls together all Tagetik vendors plus all Vendor overrides
# MAGIC SELECT
# MAGIC   vendor_code,
# MAGIC   vendor_name
# MAGIC FROM
# MAGIC   gold_${tableObject.environment}.tag02.dim_vendor
# MAGIC WHERE
# MAGIC   is_current = 1
# MAGIC UNION
# MAGIC SELECT
# MAGIC   DISTINCT a.account_vendor_override AS vendor_code,
# MAGIC   CONCAT(
# MAGIC     'Account Vendor Override - ',
# MAGIC     a.account_vendor_override
# MAGIC   ) AS vendor_name
# MAGIC FROM
# MAGIC   gold_${tableObject.environment}.tag02.lup_account_type a
# MAGIC   LEFT OUTER JOIN gold_${tableObject.environment}.tag02.dim_vendor b ON a.account_vendor_override = b.vendor_code
# MAGIC WHERE
# MAGIC   a.account_vendor_override IS NOT NULL
# MAGIC   AND b.vendor_code IS NULL
# MAGIC )
# MAGIC SELECT vendor_code,
# MAGIC        vendor_name
# MAGIC FROM cte_tagetik_vendors
# MAGIC UNION
# MAGIC SELECT y.vendor_code,
# MAGIC        y.vendor_name
# MAGIC FROM ( SELECT x.vendor_code,
# MAGIC               x.vendor_name,
# MAGIC               ROW_NUMBER() OVER(PARTITION BY LOWER(x.vendor_code) ORDER BY LEN(x.vendor_name) DESC) AS ranked_length
# MAGIC         FROM ( SELECT DISTINCT a.VendorCode AS vendor_code,
# MAGIC                                a.VendorNameInternal AS vendor_name
# MAGIC                FROM platinum_${tableObject.environment}.obt.globaltransactions a
# MAGIC                LEFT OUTER JOIN cte_tagetik_vendors b
# MAGIC                  ON LOWER(a.VendorCode) = LOWER(b.vendor_code)
# MAGIC                WHERE a.VendorCode IS NOT NULL
# MAGIC                 AND b.vendor_code IS NULL) x ) y    -- Ensure we only insert vendors that we don't already have from Tagetik
# MAGIC WHERE y.ranked_length = 1  -- Remove duplicates by only take the longest name for a single vendor code
# MAGIC
