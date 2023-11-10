# Databricks notebook source
# MAGIC %run  ../../library/nb-data_quality-library

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

try:
    TABLE_NAME = dbutils.widgets.get("wg_tableName")
except:
    dbutils.widgets.text(name = "wg_tableName", defaultValue = 'sales_invoice_line')
    TABLE_NAME = dbutils.widgets.get("wg_tableName")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

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

df_test = spark.read.table(f'silver_{ENVIRONMENT}.igsql03.{TABLE_NAME}')
df_test_bronze = spark.read.table(f'bronze_{ENVIRONMENT}.igsql03.{TABLE_NAME}')

# COMMAND ----------

WATERMARK_COLUMN = 'Sys_RowNumber'
BUSINESS_KEYS = SILVER_PRIMARY_KEYS.copy()
try :
    BUSINESS_KEYS.remove(WATERMARK_COLUMN)
except:
    BUSINESS_KEYS = SILVER_PRIMARY_KEYS.copy()

# COMMAND ----------

def column_contains_NaN(df: pyspark.sql.DataFrame, columnName : list) -> str:
    col_list = columnName
    count_list = [df.filter(col(c).isin("NaN")).count() for c in col_list]
    unvalidCount = 0
    column_list = []
    for i in range(len(count_list)):
        unvalidCount = unvalidCount  + count_list[i]
        if count_list[i] != 0:
            column_list.append(col_list[i])

    if unvalidCount != 0:
        return (f'ERROR, Total {unvalidCount} NaN count in following columns {column_list}".')

    return 'OK'

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
        result = column_contains_only_valid_values(df_test, 'Sys_DatabaseName',['ReportsDE','ReportsCH','ReportsDK','ReportsSE','ReportsNL','ReportsFI','ReportsNO','ReportsUK','ReportsFR',])
        self.assertEqual(result, 'OK')

    def test_table_valid_values(self):
        """
        Test that the given table contains only valid values.
        """
        result = column_contains_NaN(df_test, ['DocumentNo_','LineNo_'])
        self.assertEqual(result, 'OK')
    

# COMMAND ----------

test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
  raise Exception(
    f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed."
    )
