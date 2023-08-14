# Databricks notebook source
try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
    TABLE_NAME = dbutils.widgets.get("wg_tableName")
    BUSINESS_KEYS = dbutils.widgets.get("wg_businessKeys").replace("[","").replace("]","").split(',')
except:
    dbutils.widgets.text(name = "wg_tableName", defaultValue = 'AZIENDA')
    dbutils.widgets.text(name = "wg_businessKeys", defaultValue = 'COD_AZIENDA')
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
    TABLE_NAME = dbutils.widgets.get("wg_tableName")
    BUSINESS_KEYS = dbutils.widgets.get("wg_businessKeys").replace("[","").replace("]","").split(',')

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA tag02;

# COMMAND ----------

from pyspark.sql.functions import *

target_df = spark.read.table(f'silver_{ENVIRONMENT}.tag02.{TABLE_NAME}')

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

target_columns = target_df.columns

source_df = (
            spark
            .read
            .table(f'bronze_{ENVIRONMENT}.tag02.{TABLE_NAME}')
            .withColumn('Sys_Silver_InsertDateTime_UTC', current_timestamp())
            .withColumn('Sys_Silver_ModifedDateTime_UTC', current_timestamp())
            .select(target_columns)
            .where(col('Sys_Bronze_InsertDateTime_UTC') > currentWatermark)
            )

updates_df = source_df

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.window import Window
# MAGIC window = Window.orderBy(col("Sys_Bronze_InsertDateTime_UTC").desc()).partitionBy(BUSINESS_KEYS)
# MAGIC
# MAGIC for column in source_df.columns:
# MAGIC     updates_df = updates_df.withColumn(column, first(column).over(window))

# COMMAND ----------

target_update_columns = [column for column in target_columns if column != 'Sys_Silver_InsertDateTime_UTC']
source_update_columns = [f's.{column}' for column in target_update_columns]

# COMMAND ----------

updateDict = dict(zip(target_update_columns,source_update_columns))

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark,tableOrViewName=f"silver_{ENVIRONMENT}.tag02.{TABLE_NAME}")

condition = " AND ".join([f's.{BUSINESS_KEYS[i]} = t.{BUSINESS_KEYS[i]}' for i in range(len(BUSINESS_KEYS))])

(deltaTable.alias("t").merge(
updates_df.alias("s"),
condition)
.whenMatchedUpdate(set = updateDict)
.whenNotMatchedInsertAll()
.execute()
)

# COMMAND ----------

spark.sql(f"""
          VACUUM {TABLE_NAME}
          """)

# COMMAND ----------

currentVersion = spark.sql(f"""
          DESCRIBE HISTORY {TABLE_NAME}
          """).agg(max('version').alias('current_version')).collect()[0]['current_version']

# COMMAND ----------

print(f'The current table version is: {currentVersion}')
if currentVersion % 5 == 0:
    print(f'We are optimizing the table by using bin packing and z-orderin on {BUSINESS_KEYS}')
    deltaTable.optimize().executeCompaction()
    deltaTable.optimize().executeZOrderBy(BUSINESS_KEYS)
else:
    print(f'Since {currentVersion} is not divisible by 5 we are not optimizing.')
