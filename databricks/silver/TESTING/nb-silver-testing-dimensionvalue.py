# Databricks notebook source
# MAGIC %run  ../../library/nb-data_quality-library

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

df_test = spark.read.table(f'silver_{ENVIRONMENT}.igsql03.dimension_value')
df_test_bronze = spark.read.table(f'bronze_{ENVIRONMENT}.igsql03.dimension_value')

# COMMAND ----------

import unittest

class TestTable(unittest.TestCase):
    def test_dimensioncode_valid_values(self):
        """
        Test that the DimensionCode contains only valid values.
        """
        result = column_contains_only_valid_values(df_test, 'DimensionCode',['CUSTOMER','SALESPERSON','KST','VENDOR','DEAL','INTERCOMPANY','PROJECT','RPTREGION','BUSINESSTYPE','COSTCENTRE'])
        self.assertEqual(result, 'OK')

    def test_dimensioncode_all_bronze_keys(self):
        """
        Test that the DimensionCode contains only valid values.
        """
        result = silver_table_has_bronze_keys(df_test,df_test_bronze, ['Code','Sys_DatabaseName'])
        self.assertEqual(result, 'OK')

    def test_dimensioncode_valid_values(self):
        """
        Test that the DimensionCode contains active rows for all keys.
        """
        result = silver_table_has_active_keys(df_test,['Code','Sys_DatabaseName'])
        self.assertEqual(result, 'OK')

    def test_dimensioncode_valid_databasename(self):
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
