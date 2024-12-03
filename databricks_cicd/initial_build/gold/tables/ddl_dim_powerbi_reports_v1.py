# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'powerbi'

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_reports (
  ArtifactId STRING NOT NULL COMMENT 'Id of the pbi artifact',
  ArtifactName STRING COMMENT 'Name of the artifact',
  ArtifactKind STRING COMMENT 'Type of artifact',
  WorkSpaceName STRING COMMENT 'Name of the associated pbi workspace',
  IsCurrent INT COMMENT 'Flag to indicate if this is the active dimension record per code',
  SysGoldModifiedDateTimeUTC TIMESTAMP COMMENT 'The timestamp when this record was last updated in gold',
  CONSTRAINT `ArtifactId` PRIMARY KEY (`ArtifactId`))
USING delta
CLUSTER BY (ArtifactId)
TBLPROPERTIES (
  'delta.checkpointPolicy' = 'v2',
  'delta.constraints.valid_is_current_value' = 'IsCurrent IN ( 1 , 0 )',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.checkConstraints' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported')
""")
