# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
silver_columns = ["Id", "UserId", "Activity", "Operation", "ArtifactId", "ArtifactName", "ArtifactKind", "WorkSpaceName", "ConsumptionMethod", "CapacityName", "CreationTime"]

# COMMAND ----------

activity_table = spark.read.table(f"bronze_{ENVIRONMENT}.powerbi.activities")

# COMMAND ----------

todays_activities = (activity_table.where(F.col("CreationTime").cast("date") == metrics_date)
                     .select(silver_columns)
                     .withColumn("CreationTime", F.col("CreationTime").cast("timestamp"))
                     .withColumn("SysSilverInsertedDateTimeUTC", F.current_timestamp())
                     .dropDuplicates())

# COMMAND ----------

todays_activities.write.format("delta").mode("append").saveAsTable(f"silver_{ENVIRONMENT}.powerbi.activities")
