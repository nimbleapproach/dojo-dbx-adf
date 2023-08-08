# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions", 32)
spark.conf.set("spark.sql.adaptive.enabled", True)

# COMMAND ----------

TABLE_NAME = dbutils.widgets.get("tableName")
BUSINESS_KEYS = dbutils.widgets.get("businessKeys").split(',')
FULL_LOAD = eval(dbutils.widgets.get("fullLoad"))

# COMMAND ----------

#TABLE_NAME = 'Item Ledger Entry'
#BUSINESS_KEYS = ['EntryNo_', 'Sys_DatabaseName']
#FULL_LOAD = True

# COMMAND ----------

from pyspark.sql.functions import col, lit, last
from delta.tables import *
import datetime
 


def processBronzeTable(tableName : str, businessKeys : list , fullLoad : bool = False):
    cleanedTableName = tableName.replace(" ", "")
    # ct stores current time
    ct = datetime.datetime.now()

    checkpoint_path = f"/mnt/checkpoints/bronze/tag02/{tableName}/"

    if fullLoad:
        dbutils.fs.rm(checkpoint_path, recurse=True)

    df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(f"/mnt/bronze/tag02/{tableName}/")
    #.withColumn('Sys_Silver_InsertDateTime_UTC',lit(ct))
    .withColumn('Sys_Silver_ModifiedDateTime_UTC',lit(ct)))

    #updateDictonary = dict(zip(df.columns,[f"s.{column}" for column in df.columns]))
    #del updateDictonary['Sys_Silver_InsertDateTime_UTC']
    #set = updateDictonary


    if not spark.catalog.tableExists(f'Silver.tag02.{cleanedTableName}'):
        spark.catalog.createTable(f"Silver.tag02.{cleanedTableName}", schema=df.schema)

    deltaTable = DeltaTable.forName(spark,tableOrViewName=f"Silver.tag02.{cleanedTableName}")

    # Function to upsert microBatchOutputDF into Delta table using merge
    def upsertToDelta(microBatchOutputDF, batchId):
        condition = " AND ".join([f's.{businessKeys[i]} = t.{businessKeys[i]}' for i in range(len(businessKeys))])

        (deltaTable.alias("t").merge(
        microBatchOutputDF.alias("s"),
        condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    aggColumns = [last(col(x)).alias(x) for x in df.columns if x not in businessKeys]

    (df.groupBy(businessKeys).agg(*aggColumns).writeStream
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .foreachBatch(upsertToDelta)
    .outputMode("update")
    .start())

# COMMAND ----------

processBronzeTable(tableName=TABLE_NAME, businessKeys = BUSINESS_KEYS, fullLoad=FULL_LOAD)
