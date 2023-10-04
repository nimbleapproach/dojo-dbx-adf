# Databricks notebook source
# MAGIC %md This Notebook serves as a general loading solution for ALL tables.
# MAGIC
# MAGIC This ideal is the following:
# MAGIC
# MAGIC ![Medallion Architecture](documentation/assets/images/medallion.png "Medallion Architecture")

# COMMAND ----------

# MAGIC %run  ../../library/nb-silver-library

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

try:
    TABLE_NAME = dbutils.widgets.get("wg_tableName")
except:
    dbutils.widgets.text(name = "wg_tableName", defaultValue = 'AZIENDA')
    TABLE_NAME = dbutils.widgets.get("wg_tableName")

# COMMAND ----------

try:
    TABLE_SCHEMA = dbutils.widgets.get("wg_tableSchema")
except:
    dbutils.widgets.text(name = "wg_tableSchema", defaultValue = 'tag02')
    TABLE_SCHEMA = dbutils.widgets.get("wg_tableSchema")

# COMMAND ----------

try:
    WATERMARK_COLUMN = dbutils.widgets.get("wg_watermarkColumn")
except:
    dbutils.widgets.text(name = "wg_watermarkColumn", defaultValue = 'DATEUPD')
    WATERMARK_COLUMN = dbutils.widgets.get("wg_watermarkColumn")

# COMMAND ----------

try:
    FULL_LOAD = bool(dbutils.widgets.get("wg_fullload") == 'true' )
except:
    dbutils.widgets.dropdown(name = "wg_fullload", defaultValue = 'false', choices =  ['false','true'])
    FULL_LOAD = bool(dbutils.widgets.get("wg_fullload")== 'true')

# COMMAND ----------

try:
    TRUNCATE = bool(dbutils.widgets.get("wg_truncate") == 'true')
except:
    dbutils.widgets.dropdown(name = "wg_truncate", defaultValue = 'false', choices =   ['false','true'])
    TRUNCATE = bool(dbutils.widgets.get("wg_truncate") == 'true')

# COMMAND ----------

TABLE_NAME = TABLE_NAME.lower()
TABLE_SCHEMA = TABLE_SCHEMA.lower()

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

spark.sql(f"""
          USE SCHEMA {TABLE_SCHEMA}
          """)


# COMMAND ----------

if TRUNCATE :
    print(f'Truncating {TABLE_NAME}...')
    spark.sql(f"""
              TRUNCATE TABLE {TABLE_NAME}
              """)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

target_df = spark.read.table(f'silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}')

# COMMAND ----------

# MAGIC %md Here we are using the unity catalog inbuilt system tables to get the information which columns in the silver layer forming together a primary key. In this way we dont need to tell this script manually which columns to use.
# MAGIC
# MAGIC The business keys are then those but without the watermark column.
# MAGIC

# COMMAND ----------

SILVER_PRIMARY_KEYS = [key['column_name'] for key in spark.sql(f"""
SELECT a.column_name FROM information_schema.constraint_column_usage a
join information_schema.table_constraints b
on a.constraint_name = b.constraint_name
where a.table_schema = '{TABLE_SCHEMA}'
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

# MAGIC %md Calculating the current watermark to only load newly arrived data at bronze.
# MAGIC

# COMMAND ----------

from datetime import datetime

if FULL_LOAD:
    currentWatermark = datetime.strptime('01-01-1900', '%m-%d-%Y').date()
else:
    currentWatermark = (
                        target_df
                        .agg(
                            coalesce(
                                max(col('Sys_Bronze_InsertDateTime_UTC').cast('TIMESTAMP')),
                                lit('1900-01-01').cast('TIMESTAMP')
                                )
                            .alias('current_watermark'))
                        .collect()[0]['current_watermark']
                        )

# COMMAND ----------

# MAGIC %md For our column selection we get the target columns to do a select on.
# MAGIC

# COMMAND ----------

target_columns = target_df.columns
selection_column = [col(column) for column in target_columns if column not in ['SID']]
hash_columns = [col(column) for column in target_columns if not column in ['SID' ,'Sys_Bronze_InsertDateTime_UTC','Sys_Silver_InsertDateTime_UTC','Sys_Silver_ModifedDateTime_UTC','Sys_Silver_HashKey','Sys_Silver_IsCurrent',f'{WATERMARK_COLUMN}']]

# COMMAND ----------

# MAGIC %md Read new bronze data in and enrich with a few sys columns.
# MAGIC
# MAGIC Since we are selecting columns we need to drop duplicates.
# MAGIC

# COMMAND ----------

source_df = (
            spark
            .read
            .table(f'bronze_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}')
            .withColumn('Sys_Silver_InsertDateTime_UTC', current_timestamp())
            .withColumn('Sys_Silver_ModifedDateTime_UTC', current_timestamp())
            .withColumn('Sys_Silver_HashKey', hash(*hash_columns))
            .withColumn('Sys_Silver_IsCurrent', lit(True))
            .select(selection_column)
            .where(col('Sys_Bronze_InsertDateTime_UTC') > currentWatermark)
            .dropDuplicates(['Sys_Silver_HashKey'])
            )

# COMMAND ----------

source_df.count()

# COMMAND ----------

# MAGIC %md Based now on the business keys as well as the watermark column we are again dropping duplicates.

# COMMAND ----------

deduped_df = fillnas(source_df.dropDuplicates(SILVER_PRIMARY_KEYS))

# COMMAND ----------

deduped_df.count()

# COMMAND ----------

target_insert_columns = [f'`{column}`' for column in target_columns if column not in ['SID']]
source_insert_columns = [f's.{column}' for column in target_insert_columns]
insertDict = dict(zip(target_insert_columns,source_insert_columns))

# COMMAND ----------

target_update_columns = [f'`{column}`' for column in target_columns if column not in ['SID' , 'Sys_Silver_InsertDateTime_UTC']]
source_update_columns = [f's.{column}' for column in target_update_columns]
updateDict = dict(zip(target_update_columns,source_update_columns))

# COMMAND ----------

# MAGIC %md Merge on business keys and watermark column to preserve history.
# MAGIC

# COMMAND ----------

SILVER_PRIMARY_KEYS

# COMMAND ----------

BUSINESS_KEYS

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark,tableOrViewName=f"silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}")

# COMMAND ----------

conditionBK = " AND ".join([f's.{BUSINESS_KEYS[i]} = t.{BUSINESS_KEYS[i]}' for i in range(len(BUSINESS_KEYS))])

dedupedBK_df = deduped_df.dropDuplicates(BUSINESS_KEYS)
(deltaTable.alias("t").merge(
dedupedBK_df.alias("s"),
conditionBK)
.whenMatchedUpdate(set = {'t.Sys_Silver_IsCurrent' : lit(None)})
.execute()
)

# COMMAND ----------

condition = " AND ".join([f's.{SILVER_PRIMARY_KEYS[i]} = t.{SILVER_PRIMARY_KEYS[i]}' for i in range(len(SILVER_PRIMARY_KEYS))])

(deltaTable.alias("t").merge(
deduped_df.alias("s"),
condition)
.whenMatchedUpdate('s.Sys_Silver_HashKey <> t.Sys_Silver_HashKey or t.Sys_Silver_HashKey is Null',set = updateDict)
.whenNotMatchedInsert(values  = insertDict)
.execute()
)

# COMMAND ----------

# MAGIC %md Vacuum to clean up older data.

# COMMAND ----------

spark.sql(f"""
          VACUUM {TABLE_NAME}
          """)

# COMMAND ----------

import random

chanceForOptimizing = random.random()

# COMMAND ----------

# MAGIC %md By chance loadings we want to use liquid clustering to optimzie our silver table.

# COMMAND ----------

print(f'The chance is: {chanceForOptimizing}')
if chanceForOptimizing >= 0.75:
    print(f'We are optimizing the table by using liquid clustering on {BUSINESS_KEYS}')
    spark.sql(
        f"""
        OPTIMIZE {TABLE_NAME}
        """
    )
