# Databricks notebook source
# MAGIC %md
# MAGIC ##### The POC Notebook to produce some DQ metrics over igsql01 Customer

# COMMAND ----------

# MAGIC %run ../library/nb-enable-imports

# COMMAND ----------

import os
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

TABLE_SCHEMA = 'igsql01'
TABLE_NAME = 'bi_interestlink_appointments'
df = spark.table(f'{TABLE_SCHEMA}.{TABLE_NAME}')

# COMMAND ----------

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

from library import dq_counts

# COMMAND ----------

ID_COL = 'CustomerNumber'
NAME_COL = 'CustomerName'

basic_counts = df.select(F.count(col('*')).alias('rows'),
                         F.count(ID_COL).alias('non_null_ids'),
                         (col('rows')-col('non_null_ids')).alias('null_ids'),
                         F.count_distinct(ID_COL).alias('distinct_ids'),
                )

# COMMAND ----------

missing_names_count = dq_counts.count_missing_string_values(df, NAME_COL)
missing_names_count

# COMMAND ----------

multiple_name_count = (
    df.groupBy(ID_COL)
      .agg(
          F.count_distinct(NAME_COL).alias('names_count')
      )
      .where(col('names_count') > lit(1))
      .count()
)
multiple_name_count

# COMMAND ----------


entry = (
    basic_counts.select(lit(TABLE_NAME).alias('table'),
                        lit('bronze').alias('env'),
                        F.current_timestamp().alias('as_of'),
                        col('*'),
                        lit(missing_names_count).alias('missing_names'),
                        lit(multiple_name_count).alias('multiple_names')
                )
)
entry.display()

# COMMAND ----------

TARGET_TABLE_NAME = TABLE_SCHEMA + '_customer_dq'
TARGET_TABLE_SCHEMA = 'dq'
TARGET = f'silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}'
TARGET

# COMMAND ----------

(
    entry.write
         .mode('append')
         .option("mergeSchema", "true")
         .saveAsTable(TARGET)
 )

# COMMAND ----------

# # uncomment for cleanup
# spark.sql(f'DROP TABLE IF EXISTS {TARGET}')
