# Databricks notebook source
# Importing Libraries
from databricks.sdk import WorkspaceClient
import re
import io
import base64
import os

# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT
# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ---------- 

catalog = spark.catalog.currentCatalog()
schema = 'bida_metadata'
# COMMAND ----------

from dummy_model import main
main.get_taxis(spark).show(10)