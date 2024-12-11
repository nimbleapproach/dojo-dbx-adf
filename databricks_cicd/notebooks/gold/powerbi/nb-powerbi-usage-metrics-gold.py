# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from delta.tables import *
from pyspark.sql.window import Window

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")

# COMMAND ----------

activity_columns = ["Id", "UserId", "Activity", "Operation", "ArtifactId", "ConsumptionMethod", "CapacityName", "CreationTime", "SysGoldInsertedDateTimeUTC"]

initial_report_columns = ["ArtifactId", "ArtifactName", "ArtifactKind", "WorkSpaceName", "IsCurrent", "CreationTime", "SysGoldModifiedDateTimeUTC"]
final_report_columns = ["ArtifactId", "ArtifactName", "ArtifactKind", "WorkSpaceName", "IsCurrent", "SysGoldModifiedDateTimeUTC"]

# COMMAND ----------

activity_table = (spark.read.table(f"silver_{ENVIRONMENT}.powerbi.activities")
                  .where(F.col("CreationTime").cast("date") == metrics_date)
                  .withColumn("SysGoldInsertedDateTimeUTC", F.current_timestamp())
                  .select(activity_columns))

reports_table = (spark.read.table(f"silver_{ENVIRONMENT}.powerbi.activities")
                  .where(F.col("CreationTime").cast("date") == metrics_date)
                  .withColumn("IsCurrent", F.lit(1))
                  .withColumn("SysGoldModifiedDateTimeUTC", F.current_timestamp())
                  .select(initial_report_columns)
                  .dropDuplicates())

# COMMAND ----------

## take latest version of a report, using event creation time to determine latest version of name, etc.
## Needs to be done in case a report has a name update in the day, meaning two records would move through to the merge process

time_window = Window.partitionBy("ArtifactId")
reports_table_max = reports_table.withColumn("LatestTime", F.max(F.col("CreationTime")).over(time_window))
reports_table_latest = (reports_table_max.where(F.col("CreationTime") == F.col("LatestTime"))
                        .select(final_report_columns))

# COMMAND ----------

existing_reports = DeltaTable.forName(spark, tableOrViewName=f"gold_{ENVIRONMENT}.powerbi.dim_reports")

report_updates = (reports_table_latest 
  .alias("updates") 
  .join(existing_reports.toDF().alias("existing"), "ArtifactId") 
  .where("existing.IsCurrent = 1 AND (updates.ArtifactName <> existing.ArtifactName OR updates.WorkSpaceName <> existing.WorkSpaceName)"))

staged_updates = (
  report_updates.withColumn("MergeKey", F.lit(-1)).select(["MergeKey", "updates.*"])
  .unionByName(reports_table_latest.withColumn("MergeKey", F.col("ArtifactId")), allowMissingColumns=False)
)

(existing_reports.alias("existing").merge(
  staged_updates.alias("staged"),
  "existing.ArtifactId = staged.MergeKey") 
.whenMatchedUpdate(
  condition = "existing.IsCurrent = 1 AND (existing.ArtifactName <> staged.ArtifactName OR existing.WorkSpaceName <> staged.WorkSpaceName)",
  set = {                                      
    "IsCurrent": F.lit(0),
    "SysGoldModifiedDateTimeUTC": "staged.SysGoldModifiedDateTimeUTC"
  }
).whenNotMatchedInsert(  
    values = {
    "ArtifactId": "staged.ArtifactId",
    "ArtifactName": "staged.ArtifactName",
    "ArtifactKind": "staged.ArtifactKind",
    "WorkSpaceName": "staged.WorkSpaceName",
    "IsCurrent": "staged.IsCurrent",
    "SysGoldModifiedDateTimeUTC": "staged.SysGoldModifiedDateTimeUTC"
  })
.execute())

# COMMAND ----------

## Append new activity data to gold layer

activity_table.write.format("delta").mode("append").saveAsTable(f"gold_{ENVIRONMENT}.powerbi.fact_activities")
