# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'powerbi'

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.refreshes (
  RequestId STRING NOT NULL COMMENT 'The system generated Id for the refresh',
  Id LONG NOT NULL COMMENT 'The system generated Id for the api call',
  RefreshType STRING COMMENT 'The type of refresh that has occured on the dataset',
  StartTime TIMESTAMP COMMENT 'The start time of the refresh',
  EndTime TIMESTAMP COMMENT 'The end time of the refresh',
  Status STRING COMMENT 'The current status of the refresh',
  RefreshAttempts ARRAY<MAP<STRING, STRING>>  COMMENT 'Array of refresh attempt data',
  WorkspaceName STRING COMMENT 'The workspace in which the dataset resides',
  WorkspaceId STRING COMMENT 'The id of the workspace in which the dataset resides',
  DatasetName STRING COMMENT 'The name of the dataset being refreshed',
  DatasetId STRING COMMENT 'The id of the dataset being refreshed',
  SysSilverInsertedDateTimeUTC TIMESTAMP COMMENT 'The datetime the data was added to the silver layer',
  CONSTRAINT `RequestId` PRIMARY KEY (`RequestId`))
USING delta
CLUSTER BY (DatasetId)
TBLPROPERTIES (
  'delta.checkpointPolicy' = 'v2',
  'delta.constraints.datewithinrange_start_time' = 'startTime >= "1900-01-01"',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.checkConstraints' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported')
""")
