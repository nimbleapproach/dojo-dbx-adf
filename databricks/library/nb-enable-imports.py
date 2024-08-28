# Databricks notebook source
import sys, os
from os.path import dirname, abspath

# COMMAND ----------

DATABRICKS_PART = "inf-edw/databricks"

def is_databricks_path(abs_path: str) -> bool:
    return abs_path.endswith(DATABRICKS_PART)

def get_databricks_path(abs_path: str) -> str:
    """Finds databricks parent folder within path.

    Args:
        abs_path (str): Subfolder absolute path.

    Raises:
        ValueError: when path is not a subfolder of searched path.

    Returns:
        str: Absolute path of the databricks folder
    """
    if DATABRICKS_PART not in abs_path:
        raise ValueError("The path is not subfolder of the databricks folder.")

    result_path = abs_path
    while not is_databricks_path(result_path):
        result_path = dirname(result_path)
    
    return result_path

# COMMAND ----------

# when this notebook is run via %run magic, the relative path is of the calling notebook
# that must reside within databricks folder structure
start_path = os.path.abspath('..')
databricks_path = get_databricks_path(start_path)
databricks_path

# COMMAND ----------

if databricks_path not in sys.path:
    sys.path.append(databricks_path)
