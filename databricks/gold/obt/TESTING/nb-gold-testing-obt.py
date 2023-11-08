# Databricks notebook source
# MAGIC %run  ../../../library/nb-data_quality-library

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

df_test = spark.read.table(f'gold_{ENVIRONMENT}.obt.globaltransactions')

# COMMAND ----------

import unittest

class TestTable(unittest.TestCase):
    def test_obt_groupentitycode_valid_values(self):
        """
        Test that the GroupEntityCode contains only valid values.
        """
        result = column_contains_only_valid_values(df_test, 'GroupEntityCode',['IG','VU','SL','NU'])
        self.assertEqual(result, 'OK')

    def test_obt_all_bronze_keys(self):
    

# COMMAND ----------

test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
  raise Exception(
    f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed."
    )
