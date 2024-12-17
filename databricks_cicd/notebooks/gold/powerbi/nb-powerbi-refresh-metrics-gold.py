# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from delta.tables import *

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")

# COMMAND ----------

## get current data and select required cols

refresh_columns = ["RequestId", "Id", "RefreshType", "StartTime", "EndTime", "Status", "DatasetId", "SysGoldInsertedDateTimeUTC"]
attempts_columns = ["RequestId", "Id", "DatasetId", "RefreshAttempts", "SysGoldInsertedDateTimeUTC"]
dataset_columns = ["DatasetId", "DatasetName", "WorkspaceId", "WorkspaceName", "IsCurrent", "SysGoldModifiedDateTimeUTC"]

todays_refreshes = (spark.read.table(f"silver_{ENVIRONMENT}.powerbi.refreshes")
                    .where(F.col("StartTime").cast("date") == metrics_date)
                    .withColumn("IsCurrent", F.lit(1))
                    .withColumn("SysGoldInsertedDateTimeUTC", F.current_timestamp())
                    .withColumn("SysGoldModifiedDateTimeUTC", F.current_timestamp()))

refresh_table = todays_refreshes.select(refresh_columns)

attempts_table = todays_refreshes.select(attempts_columns)

datasets_table = (todays_refreshes
                  .select(dataset_columns)
                  .dropDuplicates())

# COMMAND ----------

## Append new refresh data to gold layer

refresh_table.write.format("delta").mode("append").saveAsTable(f"gold_{ENVIRONMENT}.powerbi.fact_refreshes")

# COMMAND ----------

## Append new refresh attempt data to gold layer

attempts_table = (attempts_table.select("RequestId", "Id", "DatasetId", F.explode("RefreshAttempts"), "SysGoldInsertedDateTimeUTC")
              .select("RequestId", "Id", "DatasetId", "col.attemptId", "col.startTime", "col.endTime", "col.type", "SysGoldInsertedDateTimeUTC")
              .dropDuplicates())

starting_attempt_cols = attempts_table.columns
updated_attempt_cols = [x[0].upper() + x[1:] for x in starting_attempt_cols]
attempts_table_renamed = attempts_table.toDF(*updated_attempt_cols)

attempts_table_renamed = (attempts_table_renamed.withColumn("startTime", F.col("startTime").cast("timestamp"))
                          .withColumn("endTime", F.col("endTime").cast("timestamp")))

attempts_table_renamed.write.format("delta").mode("append").saveAsTable(f"gold_{ENVIRONMENT}.powerbi.fact_refresh_attempts")

# COMMAND ----------

existing_datasets = DeltaTable.forName(spark, tableOrViewName=f"gold_{ENVIRONMENT}.powerbi.dim_datasets")

dataset_updates = (datasets_table 
  .alias("updates") 
  .join(existing_datasets.toDF().alias("existing"), "DatasetId") 
  .where("existing.IsCurrent = 1 AND (updates.DatasetName <> existing.DatasetName OR updates.WorkspaceId <> existing.WorkspaceId OR updates.WorkSpaceName <> existing.WorkSpaceName)"))

staged_updates = (
  dataset_updates.withColumn("MergeKey", F.lit(-1)).select(["MergeKey", "updates.*"])
  .unionByName(datasets_table.withColumn("MergeKey", F.col("DatasetId")), allowMissingColumns=False)
)

(existing_datasets.alias("existing").merge(
  staged_updates.alias("staged"),
  "existing.DatasetId = staged.MergeKey") 
.whenMatchedUpdate(
  condition = "existing.IsCurrent = 1 AND (existing.DatasetName <> staged.DatasetName OR existing.WorkspaceId <> staged.WorkspaceId OR existing.WorkSpaceName <> staged.WorkSpaceName)",
  set = {                                      
    "IsCurrent": F.lit(0),
    "SysGoldModifiedDateTimeUTC": "staged.SysGoldModifiedDateTimeUTC"
  }
).whenNotMatchedInsert(  
    values = {
    "DatasetId": "staged.DatasetId",
    "DatasetName": "staged.DatasetName",
    "WorkspaceId": "staged.WorkspaceId",
    "WorkspaceName": "staged.WorkspaceName",
    "IsCurrent": "staged.IsCurrent",
    "SysGoldModifiedDateTimeUTC": "staged.SysGoldModifiedDateTimeUTC"
  })
.execute())
