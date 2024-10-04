# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialization Notebook
# MAGIC This notebook is responsible for initializing task values from a JSON widget input.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
catalog = dbutils.widgets.get("catalog")

if catalog == "":
    raise ValueError("error: catalog not specified")

dbutils.jobs.taskValues.set(key='catalog', value=catalog)

dbutils.widgets.text("FULL_RELOAD", "NO")
FULL_RELOAD = dbutils.widgets.get("FULL_RELOAD")

dbutils.jobs.taskValues.set(key='FULL_RELOAD', value=FULL_RELOAD)