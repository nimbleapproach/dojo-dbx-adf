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
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.activities (
  Id STRING NOT NULL COMMENT 'The system generated Id for the activity',
  UserId STRING NOT NULL COMMENT 'The email of the user who performed the activity',
  Activity STRING COMMENT 'The activity that has been performed',
  ArtifactId STRING COMMENT 'The id of the associated PBI artefact',
  Operation STRING COMMENT 'The operation that has been performed',
  ArtifactName STRING COMMENT 'The name of the artefact that the operation has been performed on',
  ArtifactKind STRING COMMENT 'The type of artefact',
  WorkSpaceName STRING COMMENT 'The workspace in which the artefact resides',
  ConsumptionMethod STRING COMMENT 'The method by which the artefact was viewed',
  CapacityName STRING COMMENT 'The pbi capacity used to view the artefact',
  CreationTime TIMESTAMP COMMENT 'The datetime the activity happened',
  SysSilverInsertedDateTimeUTC TIMESTAMP COMMENT 'The datetime the data was added to the silver layer',
  CONSTRAINT `Id` PRIMARY KEY (`Id`))
USING delta
CLUSTER BY (ArtifactId)
TBLPROPERTIES (
  'delta.checkpointPolicy' = 'v2',
  'delta.constraints.datewithinrange_start_datetime' = 'CreationTime >= "1900-01-01"',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.checkConstraints' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported')
""")
