# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.dojo_metadata.ddl_deployment (
    `object` STRING NOT NULL PRIMARY KEY COMMENT "full path to table, view etc",
    `type` STRING DEFAULT NULL COMMENT "table, view, function or other",
    `version` SMALLINT NOT NULL COMMENT "the number which we put under v",
    `deployment_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL COMMENT "time when we deployed ddl",
    `test_timestamp` TIMESTAMP DEFAULT NULL COMMENT "after deployment we check version of object and type of object"
    )
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true,
        delta.enableDeletionVectors = true,
        delta.columnMapping.mode = 'name',
        delta.feature.allowColumnDefaults = 'supported'
        )
    COMMENT "The ddl_deployment table stores information about the deployment of database objects such as tables, views, and functions. It includes the full path to the object, its type, and the version number. The deployment_timestamp column indicates when the object was deployed, while the test_timestamp column indicates when it was last tested after deployment. This table is useful for tracking changes to database objects and ensuring that they are properly deployed and tested.";
"""
)