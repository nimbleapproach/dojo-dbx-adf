# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Databricks notebook source
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
# MAGIC -- MAGIC If there is no widget defined, Data Factory will automatically create them.
# MAGIC -- MAGIC For us while developing we can use the try and excep trick here.
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %python
# MAGIC -- MAGIC import os
# MAGIC -- MAGIC
# MAGIC -- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %md
# MAGIC -- MAGIC The target catalog depens on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- MAGIC %python
# MAGIC -- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC USE SCHEMA tag02;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_exchange_rate
# MAGIC   ( dim_exchange_rate_pk bigint
# MAGIC     GENERATED BY DEFAULT AS IDENTITY
# MAGIC     ,exchange_rate_code STRING NOT NULL
# MAGIC       COMMENT 'The derived dimensional code concatenating the scenario code, the period and the currency code'
# MAGIC     ,scenario_code STRING NOT NULL 
# MAGIC       COMMENT 'The scenario code related to the exchange rate'
# MAGIC     ,period STRING NOT NULL
# MAGIC       COMMENT 'The priod related to the exchange rate'
# MAGIC     ,currency_code STRING NOT NULL
# MAGIC       COMMENT 'The currency code related to the exchange rate'
# MAGIC     ,exchange_rate DECIMAL(18,4)
# MAGIC       COMMENT 'Exchange rate applicable for this dimensions point in time'
# MAGIC     ,exchange_rate_hash_key STRING
# MAGIC       COMMENT 'Hash value of the dimensional attributes per scenario, period and currency code combination'
# MAGIC     ,start_datetime TIMESTAMP NOT NULL 
# MAGIC       COMMENT 'The dimensional start date of the record'
# MAGIC     ,end_datetime TIMESTAMP
# MAGIC       COMMENT 'The dimensional end date of the record, those with a NULL value is curent'                      
# MAGIC     ,is_current INTEGER
# MAGIC       COMMENT 'Flag to indicate if this is the active dimension record per code'
# MAGIC     ,Sys_Gold_InsertedDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this record was inserted into gold'
# MAGIC     ,Sys_Gold_ModifiedDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this record was last updated in gold'
# MAGIC ,CONSTRAINT dim_exchange_rate_primary_key PRIMARY KEY(dim_exchange_rate_pk)
# MAGIC   )
# MAGIC COMMENT 'This is table for FX rate'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (exchange_rate_code);
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC ALTER TABLE dim_exchange_rate ADD CONSTRAINT dateWithinRange_start_datetime CHECK (start_datetime >= '1900-01-01');
# MAGIC ALTER TABLE dim_exchange_rate ADD CONSTRAINT valid_is_current_value CHECK (is_current IN (1, 0));

# COMMAND ----------

# MAGIC %md
# MAGIC **Now the table is created we want to insert the unknown member...**

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql("""
# MAGIC SELECT CAST(-1 AS BIGINT) AS dim_exchange_rate_pk,
# MAGIC        CAST('N/A' AS STRING) AS exchange_rate_code,
# MAGIC        CAST('N/A' AS STRING) AS scenario_code,
# MAGIC        CAST('N/A' AS STRING) AS period,
# MAGIC        CAST('N/A' AS STRING) AS currency_code,
# MAGIC        CAST(NULL AS DECIMAL(18,4)) AS exchange_rate,
# MAGIC        CAST(NULL AS STRING) AS exchange_rate_hash_key,
# MAGIC        CAST('1900-01-01' AS TIMESTAMP) AS start_datetime,
# MAGIC        CAST(NULL AS TIMESTAMP) AS end_datetime,
# MAGIC        CAST(1 AS INTEGER) AS is_current,
# MAGIC        CAST(NULL AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
# MAGIC        CAST(NULL AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC       
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
# MAGIC sqldf.write.mode("append").option("mergeSchema", "true").saveAsTable(f"gold_{ENVIRONMENT}.tag02.dim_exchange_rate")
