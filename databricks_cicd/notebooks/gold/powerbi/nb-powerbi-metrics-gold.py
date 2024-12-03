# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from delta.tables import *

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")

# COMMAND ----------

activity_columns = ["Id", "UserId", "Activity", "Operation", "ArtifactId", "ConsumptionMethod", "CapacityName", "CreationTime", "SysGoldInsertedDateTimeUTC"]

report_columns = ["ArtifactId", "ArtifactName", "ArtifactKind", "WorkSpaceName", "IsCurrent", "SysGoldModifiedDateTimeUTC"]

# COMMAND ----------

activity_table = (spark.read.table(f"silver_{ENVIRONMENT}.powerbi.activities")
                  .where(F.col("CreationTime").cast("date") == metrics_date)
                  .withColumn("SysGoldInsertedDateTimeUTC", F.current_timestamp())
                  .select(activity_columns))

reports_table = (spark.read.table(f"silver_{ENVIRONMENT}.powerbi.activities")
                  .where(F.col("CreationTime").cast("date") == metrics_date)
                  .withColumn("IsCurrent", F.lit(1))
                  .withColumn("SysGoldModifiedDateTimeUTC", F.current_timestamp())
                  .select(report_columns)
                  .dropDuplicates())

# COMMAND ----------

existing_reports = DeltaTable.forName(spark, tableOrViewName=f"gold_{ENVIRONMENT}.powerbi.dim_reports")

new_reports = (reports_table 
  .alias("updates") 
  .join(existing_reports.toDF().alias("existing"), "ArtifactId") 
  .where("existing.IsCurrent = 1 AND (updates.ArtifactName <> existing.ArtifactName OR updates.WorkSpaceName <> existing.WorkSpaceName)"))

staged_updates = (
  new_reports
  .selectExpr("NULL as MergeKey", "updates.*")   # Rows for 1
  .union(reports_table.selectExpr("updates.ArtifactId as MergeKey", "*"))  # Rows for 2.
)
  
# Apply SCD Type 2 operation using merge
(existing_reports.alias("existing").merge(
  staged_updates.alias("staged"),
  "existing.ArtifactId = MergeKey") 
.whenMatchedUpdate(
  condition = "existing.IsCurrent = 1 AND (updates.ArtifactName <> existing.ArtifactName OR updates.WorkSpaceName <> existing.WorkSpaceName)",
  set = {                                      # Set current to false and endDate to source's effective date.
    "current": 0,
    "SysGoldModifiedDateTimeUTC": "staged_updates.SysGoldModifiedDateTimeUTC"
  }
).whenNotMatchedInsert(
  values = {
    "ArtifactId": "staged_updates.ArtifactId",
    "ArtifactName": "staged_updates.ArtifactName",
    "ArtifactKind": "staged_updates.ArtifactKind",
    "WorkSpaceName": "staged_updates.WorkSpaceName",  # Set current to true along with the new address and its effective date.
    "IsCurrent": "staged_updates.IsCurrent",
    "SysGoldModifiedDateTimeUTC": "staged_updates.SysGoldModifiedDateTimeUTC"
  }
).execute())

# COMMAND ----------

## Append new activity data to gold layer

activity_table.write.format("delta").mode("append").saveAsTable(f"gold_{ENVIRONMENT}.powerbi.fact_activities")
