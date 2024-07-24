# Databricks notebook source
# MAGIC %run  ../../library/nb-data_quality-library

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

df_test = spark.read.table(f'silver_{ENVIRONMENT}.igsql03.item')
df_test_bronze = spark.read.table(f'bronze_{ENVIRONMENT}.igsql03.item')

# COMMAND ----------

import unittest

class TestTable(unittest.TestCase):
    def test_item_valid_values(self):
        """
        Test that the item contains only valid values.
        """
        result = column_contains_only_valid_values(df_test, 'PhysicalGoods',[0, 1])
        self.assertEqual(result, 'OK')

    def test_item_all_bronze_keys(self):
        """
        Test that the item contains only valid values.
        """
        result = silver_table_has_bronze_keys(df_test,df_test_bronze, ['No_','Sys_DatabaseName'])
        self.assertEqual(result, 'OK')

    def test_item_valid_values(self):
        """
        Test that the item contains active rows for all keys.
        """
        result = silver_table_has_active_keys(df_test,['No_','Sys_DatabaseName'])
        self.assertEqual(result, 'OK')

    def test_item_valid_databasename(self):
        """
        Test that Sys_DatabaseName contains only valid values.
        """
        result = column_contains_only_valid_values(df_test, 'Sys_DatabaseName',['ReportsDE','ReportsCH','ReportsDK','ReportsSE','ReportsNL','ReportsFI','ReportsNO','ReportsUK','ReportsFR','ReportsAT','ReportsBE'])
        self.assertEqual(result, 'OK')
    

# COMMAND ----------

test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
  raise Exception(
    f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed."
    )
