# Databricks notebook source
# MAGIC %run  ../../library/nb-data_quality-library

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

try:
    TABLE_NAME = dbutils.widgets.get("wg_tableName")
except:
    dbutils.widgets.text(name = "wg_tableName", defaultValue = 'customer')
    TABLE_NAME = dbutils.widgets.get("wg_tableName")

# COMMAND ----------

WATERMARK_COLUMN = 'Sys_RowNumber'

# COMMAND ----------

df_test = spark.read.table(f'silver_{ENVIRONMENT}.igsql03.{TABLE_NAME}')
df_test_bronze = spark.read.table(f'bronze_{ENVIRONMENT}.igsql03.{TABLE_NAME}')

# COMMAND ----------

SILVER_PRIMARY_KEYS = [key['column_name'] for key in spark.sql(f"""
SELECT a.column_name FROM information_schema.constraint_column_usage a
join information_schema.table_constraints b
on a.constraint_name = b.constraint_name
where a.table_schema = 'igsql03'
and a.table_name = '{TABLE_NAME}'
and b.constraint_type = 'PRIMARY KEY'
""").collect()]

# COMMAND ----------

BUSINESS_KEYS = SILVER_PRIMARY_KEYS.copy()
try :
    BUSINESS_KEYS.remove(WATERMARK_COLUMN)
except:
    BUSINESS_KEYS = SILVER_PRIMARY_KEYS.copy()

# COMMAND ----------

SILVER_PRIMARY_KEYS

# COMMAND ----------

BUSINESS_KEYS

# COMMAND ----------

import unittest

class TestTable(unittest.TestCase):

    def test_table_all_bronze_keys(self):
        """
        Test that the table contains only valid values.
        """
        result = silver_table_has_bronze_keys(df_test,df_test_bronze, BUSINESS_KEYS)
        self.assertEqual(result, 'OK')

    def test_table_valid_values(self):
        """
        Test that the table contains active rows for all keys.
        """
        result = silver_table_has_active_keys(df_test,BUSINESS_KEYS)
        self.assertEqual(result, 'OK')

    def test_table_valid_databasename(self):
        """
        Test that Sys_DatabaseName contains only valid values.
        """
        result = column_contains_only_valid_values(df_test, 'Sys_DatabaseName',['ReportsDE','ReportsCH','ReportsDK','ReportsSE','ReportsNL','ReportsFI','ReportsNO','ReportsUK','ReportsFR','ReportsAT'])
        self.assertEqual(result, 'OK')
    

# COMMAND ----------

test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
  raise Exception(
    f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed."
    )
