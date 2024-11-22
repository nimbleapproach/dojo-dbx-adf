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
create or replace view technology as
select t.Code, t.Name, t.Technology_Group_Code AS Group_Code, g.Name as Group_Name
from silver_{ENVIRONMENT}.igsql03.technology t
join silver_{ENVIRONMENT}.igsql03.technology_group g
  on t.Technology_Group_Code = g.Code
where t.Sys_Silver_IsCurrent and g.Sys_Silver_IsCurrent
""")
