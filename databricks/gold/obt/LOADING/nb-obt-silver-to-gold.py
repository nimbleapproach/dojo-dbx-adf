# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA OBT

# COMMAND ----------

# MAGIC %md
# MAGIC Create the view with infinigate data as we need it for our OBT.

# COMMAND ----------

spark.sql(
    f"""
Create or replace view infinigate_globaltransactions
as Select 'IG' as GroupEntityCode,No_ as SKU, concat_ws(' ',Description,Description2,Description3,Description4) as Description from silver_{ENVIRONMENT}.igsql03.item
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC   *
# MAGIC from
# MAGIC   infinigate_globaltransactions

# COMMAND ----------

# MAGIC %md
# MAGIC Create the view with starlink data as we need it for our OBT.

# COMMAND ----------

spark.sql(
    f"""
Create or replace view starlink_globaltransactions
as Select 'SL' as GroupEntityCode,SKU_ID as SKU, Description from silver_{ENVIRONMENT}.netsuite.masterdatasku
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC   *
# MAGIC from
# MAGIC   starlink_globaltransactions

# COMMAND ----------

# MAGIC %md
# MAGIC Union ALL together

# COMMAND ----------

# MAGIC %sql
# MAGIC Create
# MAGIC or replace temporary view union_globaltransactions as
# MAGIC Select
# MAGIC   *
# MAGIC from
# MAGIC   starlink_globaltransactions
# MAGIC UNION ALL
# MAGIC Select
# MAGIC   *
# MAGIC from
# MAGIC   infinigate_globaltransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC   *
# MAGIC FROm
# MAGIC   union_globaltransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT
# MAGIC   OVERWRITE globaltransactions
# MAGIC Select
# MAGIC   *
# MAGIC from
# MAGIC   union_globaltransactions
