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
import pyspark.sql.functions as F

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
    DELTA_LOAD = (dbutils.widgets.get("wg_DeltaLoadTable"))
except:
    dbutils.widgets.dropdown(name = "wg_DeltaLoadTable", defaultValue = 'delta', choices =  ['delta','full'])
    DELTA_LOAD = (dbutils.widgets.get("wg_DeltaLoadTable"))

# COMMAND ----------

# NOTE: (DP 23/10/2024 T22814) (https://dev.azure.com/InfinigateHolding/Group%20IT%20Program/_workitems/edit/22814)
#       the setting has no bearing but is used by databricks jobs,
#       it should be removed from jobs first, then from here
try:
    FULL_LOAD = bool(dbutils.widgets.get("wg_fullload") == 'true' )
except:
    dbutils.widgets.dropdown(name = "wg_fullload", defaultValue = 'false', choices =  ['false','true'])
    FULL_LOAD = bool(dbutils.widgets.get("wg_fullload") == 'true')

# COMMAND ----------

try:
    TRUNCATE = bool(dbutils.widgets.get("wg_truncate") == 'true')
except:
    dbutils.widgets.dropdown(name = "wg_truncate", defaultValue = 'false', choices =   ['false','true'])
    TRUNCATE = bool(dbutils.widgets.get("wg_truncate") == 'true')

# COMMAND ----------

try:
    SOFT_DELETE = bool(dbutils.widgets.get("wg_softdelete") == 'true')
except:
    dbutils.widgets.dropdown(name = "wg_softdelete", defaultValue = 'false', choices =   ['false','true'])
    SOFT_DELETE = bool(dbutils.widgets.get("wg_softdelete") == 'true')

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
target_datatypes = target_df.dtypes

INIT_LOAD = (target_df.count() == 0)

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

BUSINESS_KEYS = list(set([x for x in BUSINESS_KEYS if x != WATERMARK_COLUMN]))

# COMMAND ----------

SILVER_PRIMARY_KEYS

# COMMAND ----------

BUSINESS_KEYS

# COMMAND ----------

# MAGIC %md For our column selection we get the target columns to do a select on.
# MAGIC

# COMMAND ----------

target_columns = target_df.columns
selection_column = [col(column) for column in target_columns if column not in ['SID','Sys_Silver_IsDeleted']]
hash_columns = [col(column) for column in target_columns if not column in ['SID' ,'Sys_Bronze_InsertDateTime_UTC','Sys_Silver_InsertDateTime_UTC','Sys_Silver_ModifedDateTime_UTC','Sys_Silver_HashKey','Sys_Silver_IsDeleted',f'{WATERMARK_COLUMN}']]

# COMMAND ----------

# MAGIC %md Read new bronze data in and enrich with a few sys columns.
# MAGIC
# MAGIC Since we are selecting columns we need to drop duplicates.
# MAGIC

# COMMAND ----------

if DELTA_LOAD == 'delta':
  print('Delta Loading')
  source_df = spark.sql(f"""
                      Select *,
                      max({WATERMARK_COLUMN})  OVER (PARTITION BY {','.join(BUSINESS_KEYS)}) AS Current_Version,
                      {WATERMARK_COLUMN} = Current_Version as Sys_Silver_IsCurrent
                      from bronze_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}""")
else:
  print('Full Loading')
  source_df = spark.sql(f"""
                      Select *, true as Sys_Silver_IsCurrent
                      from bronze_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}""")
if SILVER_PRIMARY_KEYS:
  source_df = (
      source_df.withColumn('Sys_Silver_InsertDateTime_UTC', current_timestamp())
              .withColumn('Sys_Silver_ModifedDateTime_UTC', current_timestamp())
              .withColumn('Sys_Silver_HashKey', xxhash64(*hash_columns))
              .select(selection_column)
              .dropDuplicates(SILVER_PRIMARY_KEYS)
              .dropDuplicates(['Sys_Silver_HashKey'])
      )
  print("has silver key")
else:
  source_df = (
      source_df.withColumn('Sys_Silver_InsertDateTime_UTC', current_timestamp())
              .withColumn('Sys_Silver_ModifedDateTime_UTC', current_timestamp())
              .withColumn('Sys_Silver_HashKey', xxhash64(*hash_columns))
              .select(selection_column)
      )
  print("no silver key")

# COMMAND ----------

# MAGIC %md Based now on the business keys as well as the watermark column we are again dropping duplicates.

# COMMAND ----------

deduped_df = fillnas(source_df)

if BUSINESS_KEYS:
  deduped_df = deduped_df.na.drop(subset= BUSINESS_KEYS)

# COMMAND ----------

target_insert_columns = [f'`{column}`' for column in target_columns if column not in ['SID','Sys_Silver_IsDeleted']]
source_insert_columns = [f's.{column}' for column in target_insert_columns]
insertDict = dict(zip(target_insert_columns,source_insert_columns))

# COMMAND ----------

target_update_columns = [f'`{column}`' for column in target_columns if column not in ['SID' , 'Sys_Silver_IsDeleted','Sys_Silver_InsertDateTime_UTC']]
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

## Cast source data to desired types

source_columns = deduped_df.columns
for column in source_columns:
        target_datatype = [pair[1] for pair in target_datatypes if pair[0] == column][0]
        deduped_df = deduped_df.withColumn(column, col(column).cast(target_datatype))

# COMMAND ----------

if INIT_LOAD:
    print('We are initial loading...')
    deduped_df.writeTo(f'silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}').append()
else:
    print('We are merging...')
    condition = " AND ".join([f's.{SILVER_PRIMARY_KEYS[i]} = t.{SILVER_PRIMARY_KEYS[i]}' for i in range(len(SILVER_PRIMARY_KEYS))])

    (deltaTable.alias("t").merge(
    deduped_df.alias("s"),
    condition)
    .whenMatchedUpdate('t.Sys_Silver_HashKey <> s.Sys_Silver_HashKey',set = updateDict)
    .whenMatchedUpdate('t.Sys_Silver_IsCurrent != s.Sys_Silver_IsCurrent', set = {'t.Sys_Silver_IsCurrent' : 's.Sys_Silver_IsCurrent'})
    .whenNotMatchedBySourceUpdate(set = {'t.Sys_Silver_IsCurrent' : lit(False)})
    .whenNotMatchedInsert(values  = insertDict)
    .execute()
    )

# COMMAND ----------

if SOFT_DELETE :
    print(f'Soft deleting {TABLE_NAME}...')
    condition = " AND ".join([f'k.{BUSINESS_KEYS[i]} = t.{BUSINESS_KEYS[i]}' for i in range(len(BUSINESS_KEYS))])
    spark.sql(f"""
              UPDATE {TABLE_NAME} t
                SET  Sys_Silver_IsDeleted = True
                where NOT EXISTS (
                    Select *
                    from keys_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME} k 
                    where {condition}
                )
              """)
    spark.sql(f"""
              UPDATE {TABLE_NAME} t
                SET  Sys_Silver_IsDeleted = False
                where EXISTS (
                    Select *
                    from keys_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME} k 
                    where {condition}
                )
              """)

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

# MAGIC %md By chance loadings we want to use liquid clustering to optimize our silver table.

# COMMAND ----------

print(f'The chance is: {chanceForOptimizing}')
if chanceForOptimizing >= 0.75:
    print(f'We are optimizing the table by using liquid clustering on {BUSINESS_KEYS}')
    spark.sql(
        f"""
        OPTIMIZE {TABLE_NAME}
        """
    )
