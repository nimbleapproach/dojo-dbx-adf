# Databricks notebook source
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, ArrayType, MapType

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

# set schema for refresh data, to allow dataframe conversion

refresh_schema = StructType([
    StructField("requestId", StringType(),True),
    StructField("id", LongType(),True),
    StructField("refreshType", StringType(),True),
    StructField("startTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("status", StringType(), True),
    StructField("refreshAttempts", ArrayType(MapType(StringType(), StringType()), True)),
    StructField("workspaceName", StringType(), True),
    StructField("workspaceId", StringType(), True),
    StructField("datasetName", StringType(), True),
    StructField("datasetId", StringType(), True),
  ])

# COMMAND ----------

# MAGIC %run  ./nb-powerbi-api-utils

# COMMAND ----------

admin_client_id = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-admin-app-id")
admin_client_secret = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-admin-app-secret")
client_id = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-app-id")
client_secret = dbutils.secrets.get(scope="kv-ig-westeurope-shared",key="secret-powerbi-app-secret")
metrics_date = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d")
AdminSession = requests.Session()
DatasetSession = requests.Session()

# COMMAND ----------

AdminPowerBIHandler = PowerBIHandler(AdminSession, admin_client_id, admin_client_secret)
PowerBIHandler = PowerBIHandler(DatasetSession, client_id, client_secret)

# COMMAND ----------

# get activity audit data via admin API

activity_list = []

initial_call = AdminPowerBIHandler.getDailyReportViews(intial_run = True, date = metrics_date, continuation_uri = "NA")
initial_activities = initial_call.get("activityEventEntities")
activity_list = activity_list + initial_activities
continuation_uri = initial_call.get("continuationUri")

while continuation_uri:
    ## if continuation uri present, get data
    call = AdminPowerBIHandler.getDailyReportViews(intial_run = False, date = "NA", continuation_uri = continuation_uri)
    activities = call.get("activityEventEntities")
    activity_list = activity_list + activities
    continuation_uri = call.get("continuationUri")

if len(activity_list) != 0:
    activity_df = spark.createDataFrame(activity_list).withColumn("SysBronzeInsertedDateTimeUTC", F.current_timestamp())
    (activity_df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable(f"bronze_{ENVIRONMENT}.powerbi.activities"))

# COMMAND ----------

## get dataset refresh data from standard API

workspaces = []
bi_workspaces = ['Data Model', 'Data Model DEV', 'Data Model TEST', 'Reports DEV', 'Reports TEST', 'Raw Data DEV', 'Raw Data TEST', 'Raw Data']
workspace_call = PowerBIHandler.getWorkspaces()
for workspace in workspace_call.get("value"):
    if workspace.get("name") in bi_workspaces:
        workspaces.append({"workspace_name" : workspace.get("name"), "workspace_id" : workspace.get("id")})

for workspace in workspaces:
    datasets = []
    workspace_id = workspace.get("workspace_id")
    dataset_call = PowerBIHandler.getDatasets(workspace_id)

    for dataset in dataset_call.get("value"):
        datasets.append({"dataset_name" : dataset.get("name"), "dataset_id" : dataset.get("id")})

    workspace["datasets"] = datasets

refresh_data = []
for workspace in workspaces:
    workspace_name = workspace.get("workspace_name")
    workspace_id = workspace.get("workspace_id")

    for dataset in workspace.get("datasets"):
        dataset_name = dataset.get("dataset_name")
        dataset_id = dataset.get("dataset_id")

        refresh_call = PowerBIHandler.getRefreshDataset(dataset_id , workspace_id)
        refresh_list = refresh_call.get("value")

        if refresh_list == None:
            continue
        else:
            for refresh in refresh_list:
                refresh["workspaceName"] = workspace_name
                refresh["workspaceId"] = workspace_id
                refresh["datasetName"] = dataset_name
                refresh["datasetId"] = dataset_id

        refresh_data = refresh_data + refresh_list

if len(refresh_data) != 0:
    refresh_df = (spark.createDataFrame(refresh_data, schema = refresh_schema)
                  .where(F.col("startTime").cast("date") == metrics_date)
                .withColumn("SysBronzeInsertedDateTimeUTC", F.current_timestamp()))
    
    (refresh_df.write.format("delta")
        .mode("append")
        .saveAsTable(f"bronze_{ENVIRONMENT}.powerbi.refreshes"))
