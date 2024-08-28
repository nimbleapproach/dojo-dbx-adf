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
# MAGIC CREATE OR REPLACE TABLE lup_account_type
# MAGIC   ( account_code STRING
# MAGIC       COMMENT 'Account Code'
# MAGIC     ,total_revenue INT
# MAGIC       COMMENT 'Flag to indicate if this account code relates to total revenue'
# MAGIC     ,total_cogs INT
# MAGIC       COMMENT 'Flag to indicate if this account code relates to total cogs'
# MAGIC     ,gp1 INT
# MAGIC       COMMENT 'Flag to indicate if this account code relates to GP1'
# MAGIC ,CONSTRAINT lup_account_type PRIMARY KEY(account_code)
# MAGIC   )
# MAGIC
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (account_code);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Populate the relevant meta data...**

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql(f"""
# MAGIC INSERT INTO gold_{ENVIRONMENT}.tag02.lup_account_type (account_code, total_revenue, total_cogs, gp1)
# MAGIC VALUES('309988', 1, NULL, 1),
# MAGIC       ('310188', 1, NULL, 1),
# MAGIC       ('310288', 1, NULL, 1),
# MAGIC       ('310388', 1, NULL, 1),
# MAGIC       ('310488', 1, NULL, 1),
# MAGIC       ('310588', 1, NULL, 1),
# MAGIC       ('310688', 1, NULL, 1),
# MAGIC       ('310788', 1, NULL, 1),
# MAGIC       ('310888', 1, NULL, 1),
# MAGIC       ('310988', 1, NULL, 1),
# MAGIC       ('311088', 1, NULL, 1),
# MAGIC       ('312088', 1, NULL, 1),
# MAGIC       ('313088', 1, NULL, 1),
# MAGIC       ('314088', 1, NULL, 1),
# MAGIC       ('320988', 1, NULL, 1),
# MAGIC       ('322988', 1, NULL, 1),
# MAGIC       ('350988', NULL, NULL, 1),
# MAGIC       ('351988', NULL, NULL, 1),
# MAGIC       ('370988', 1, NULL, 1),
# MAGIC       ('371988', NULL, NULL, 1),
# MAGIC       ('391988', NULL, NULL, 1),
# MAGIC       ('400988', NULL, 1, 1),
# MAGIC       ('401988', NULL, 1, 1),
# MAGIC       ('402988', NULL, 1, 1),
# MAGIC       ('409999_REF', NULL, 1, 1),
# MAGIC       ('420988', NULL, 1, 1),
# MAGIC       ('420999_REF', NULL, 1, 1),
# MAGIC       ('421988', NULL, 1, 1),
# MAGIC       ('422988', NULL, 1, 1),
# MAGIC       ('440188', NULL, 1, 1),
# MAGIC       ('440288', NULL, 1, 1),
# MAGIC       ('440388', NULL, 1, 1),
# MAGIC       ('440488', NULL, 1, 1),
# MAGIC       ('440588', NULL, 1, 1),
# MAGIC       ('440688', NULL, 1, 1),
# MAGIC       ('440788', NULL, 1, 1),
# MAGIC       ('440888', NULL, 1, 1),
# MAGIC       ('449988', NULL, 1, 1),
# MAGIC       ('450888', NULL, 1, 1),
# MAGIC       ('450988', NULL, 1, 1),
# MAGIC       ('451988', NULL, NULL, 1),
# MAGIC       ('452788', NULL, NULL, 1),
# MAGIC       ('452888', NULL, 1, 1),
# MAGIC       ('452988', NULL, 1, 1),
# MAGIC       ('468988', NULL, 1, 1),
# MAGIC       ('469988', NULL, NULL, 1),
# MAGIC       ('471988', NULL, 1, 1),
# MAGIC       ('499988', NULL, NULL, 1)
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)