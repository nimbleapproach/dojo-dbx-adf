# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")

# COMMAND ----------

## get latest activity data, change string types to timestamp and add inserted date

silver_activity_columns = ["Id", "UserId", "Activity", "Operation", "ArtifactId", "ArtifactName", "ArtifactKind", "WorkSpaceName", "ConsumptionMethod", "CapacityName", "CreationTime"]

activity_table = spark.read.table(f"bronze_{ENVIRONMENT}.powerbi.activities")

todays_activities = (activity_table.where(F.col("CreationTime").cast("date") == metrics_date)
                     .select(silver_activity_columns)
                     .withColumn("CreationTime", F.col("CreationTime").cast("timestamp"))
                     .withColumn("SysSilverInsertedDateTimeUTC", F.current_timestamp())
                     .dropDuplicates())

todays_activities.write.format("delta").mode("append").saveAsTable(f"silver_{ENVIRONMENT}.powerbi.activities")

# COMMAND ----------

## get latest refresh data, change string types to timestamp, add inserted date and rename columns to titlecase

refresh_table = spark.read.table(f"bronze_{ENVIRONMENT}.powerbi.refreshes")

todays_refreshes = (refresh_table.where(F.col("startTime").cast("date") == metrics_date)
                     .withColumn("startTime", F.to_timestamp(F.col("startTime")))
                     .withColumn("endTime", F.to_timestamp(F.col("endTime")))
                     .withColumn("SysSilverInsertedDateTimeUTC", F.current_timestamp())
                     .drop(*["SysBronzeInsertedDateTimeUTC"]))

starting_cols = todays_refreshes.columns
updated_cols = [x[0].upper() + x[1:] for x in starting_cols]
todays_refreshes_renamed = todays_refreshes.toDF(*updated_cols)

todays_refreshes_renamed.write.format("delta").mode("append").saveAsTable(f"silver_{ENVIRONMENT}.powerbi.refreshes")
