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
    # ensure that the default value won't work when run as a job
    # for manual testing, enter the value in the widget and re-run this cell
    dbutils.widgets.text(name = "wg_tableName", defaultValue = 'enter table name')
    TABLE_NAME = dbutils.widgets.get("wg_tableName")

TABLE_NAME

# COMMAND ----------

WATERMARK_COLUMN = 'ModifiedOn'
SCHEMA_NAME = 'igsql01'

# COMMAND ----------

df_test = spark.read.table(f'silver_{ENVIRONMENT}.{SCHEMA_NAME}.{TABLE_NAME}')
df_test_bronze = spark.read.table(f'bronze_{ENVIRONMENT}.{SCHEMA_NAME}.{TABLE_NAME}')

# COMMAND ----------

SILVER_PRIMARY_KEYS = [key['column_name'] for key in spark.sql(f"""
SELECT a.column_name FROM information_schema.constraint_column_usage a
join information_schema.table_constraints b
on a.constraint_name = b.constraint_name
where a.table_schema = '{SCHEMA_NAME}'
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
        result = silver_table_has_bronze_keys(df_test.where(col('Sys_Silver_IsCurrent')),
                                               df_test_bronze, BUSINESS_KEYS)
        self.assertEqual(result, 'OK')

    def test_table_valid_databasename(self):
        """
        Test that Sys_DatabaseName contains only valid values.
        """
        valid_dbs = [
            	'infinigateAll',
                'infinigateCH',
                'infinigateDE',
                'infinigateDK',
                'infinigateFI',
                'infinigateFR',
                'infinigateNL',
                'infinigateNO',
                'infinigateSE',
                'infinigateUK',
        ]
        result = column_contains_only_valid_values(df_test, 'Sys_DatabaseName', valid_dbs)
        self.assertEqual(result, 'OK')
    

# COMMAND ----------

test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
  raise Exception(
    f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed."
    )
