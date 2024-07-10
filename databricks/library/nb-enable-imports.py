# Databricks notebook source
import sys, os
databricks_path = os.path.abspath('..')
databricks_path

# COMMAND ----------

if databricks_path not in sys.path:
    sys.path.append(databricks_path)
