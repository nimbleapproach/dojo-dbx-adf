# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE OBT

# COMMAND ----------

obt_df = spark.read.table('globaltransactions')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

obt_df.groupBy('GroupEntityCode','EntityCode','TransactionDate').count().withColumnRenamed('count','Sys_Gold_NumberOfRows').orderBy('TransactionDate','GroupEntityCode','EntityCode').withColumn('Sys_Gold_Observation_DateTime_UTC', current_timestamp()).write.mode('append').option("mergeSchema", "true").saveAsTable('globaltransactions_logging')
