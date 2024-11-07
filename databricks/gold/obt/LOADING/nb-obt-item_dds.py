# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA obt;

# COMMAND ----------

spark.sql( f"""
create or replace view item_dds as
select Source_Entity, Manufacturer_Item_No_, Vendor_DIM_Code, Description, Technology_Code
from (select Source_Entity, Manufacturer_Item_No_, Vendor_DIM_Code, Description, Technology_Code,
            row_number() over (
                  partition by Source_Entity, Manufacturer_Item_No_, Vendor_DIM_Code
                  order by `timestamp` desc) as row_
      from silver_{ENVIRONMENT}.igsql03.item_dds
      where Sys_Silver_IsCurrent)
where row_ = 1
""")
