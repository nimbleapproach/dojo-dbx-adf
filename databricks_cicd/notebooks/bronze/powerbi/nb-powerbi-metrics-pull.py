# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

# MAGIC %run  ./nb-powerbi-api-utils

# COMMAND ----------

client_id = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-admin-app-id")
client_secret = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-admin-app-secret")
metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
infinigateSession = requests.Session()

# COMMAND ----------

PowerBIHandler = PowerBIHandler(infinigateSession, client_id, client_secret)

# COMMAND ----------

activity_list = []

initial_call = PowerBIHandler.getDailyReportViews(intial_run = True, date = metrics_date, continuation_uri = "NA")
initial_activities = initial_call.get("activityEventEntities")
activity_list = activity_list + initial_activities
continuation_uri = initial_call.get("continuationUri")

while continuation_uri:
    ## if continuation uri present, get data
    call = PowerBIHandler.getDailyReportViews(intial_run = False, date = "NA", continuation_uri = continuation_uri)
    activities = call.get("activityEventEntities")
    activity_list = activity_list + activities
    continuation_uri = call.get("continuationUri")

# COMMAND ----------

if len(activity_list) != 0:
    activity_df = spark.createDataFrame(activity_list).withColumn("SysBronzeInsertedDateTimeUTC", F.current_timestamp())
    (activity_df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable(f"bronze_{ENVIRONMENT}.powerbi.activities"))
