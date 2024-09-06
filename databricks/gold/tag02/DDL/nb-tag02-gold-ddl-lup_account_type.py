# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA tag02;

# COMMAND ----------

# MAGIC %sql
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
# MAGIC     ,account_vendor_override STRING
# MAGIC       COMMENT 'Value to use to force account level aggregation rather than inclusion in a vendor. Should be set as a vendor_code to be used elsewhere. Start with AVO_ if creating here. Set to NULL to allow vendor level aggregation'
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
# MAGIC INSERT INTO gold_{ENVIRONMENT}.tag02.lup_account_type (account_code, total_revenue, total_cogs, gp1, account_vendor_override)
# MAGIC VALUES('309988', 1, NULL, 1, NULL),
# MAGIC       ('310188', 1, NULL, 1, "AVO_TS"), -- Training Services
# MAGIC       ('310288', 1, NULL, 1, "AVO_PS"), -- Professional Services
# MAGIC       ('310388', 1, NULL, 1, "AVO_SS"), -- Support Services
# MAGIC       ('310488', 1, NULL, 1, "AVO_TMS"), -- Telemarketing Services
# MAGIC       ('310588', 1, NULL, 1, "AVO_AS"), -- Astaro 24/7 Services
# MAGIC       ('310688', 1, NULL, 1, "AVO_OS"), -- Other Services
# MAGIC       ('310788', 1, NULL, 1, "AVO_IES"), -- Infinigate Enhanced Services
# MAGIC       ('310888', 1, NULL, 1, "AVO_EM"), -- Event Marketing
# MAGIC       ('310988', 1, NULL, 1, "AVO_RS"), -- Revenue Services
# MAGIC       ('311088', 1, NULL, 1, "AVO_ICARE"), -- InfCARE (Warranty)
# MAGIC       ('312088', 1, NULL, 1, "AVO_CMS"), -- Cloud Managed Services
# MAGIC       ('313088', 1, NULL, 1, "AVO_CMkS"), -- Cloud Marketing Services
# MAGIC       ('314088', 1, NULL, 1, "AVO_CPS"), -- Cloud Platform Services
# MAGIC       ('320988', 1, NULL, 1, NULL),
# MAGIC       ('322988', 1, NULL, 1, NULL),
# MAGIC       ('350988', NULL, NULL, 1, NULL),
# MAGIC       ('351988', NULL, NULL, 1, NULL),
# MAGIC       ('370988', 1, NULL, 1, NULL),
# MAGIC       ('371988', NULL, NULL, 1, NULL),
# MAGIC       ('391988', NULL, NULL, 1, NULL),
# MAGIC       ('400988', NULL, 1, 1, NULL),
# MAGIC       ('401988', NULL, 1, 1, NULL),
# MAGIC       ('402988', NULL, 1, 1, NULL),
# MAGIC       ('409999_REF', NULL, 1, 1, NULL),
# MAGIC       ('420988', NULL, 1, 1, NULL),
# MAGIC       ('420999_REF', NULL, 1, 1, NULL),
# MAGIC       ('421988', NULL, 1, 1, NULL),
# MAGIC       ('422988', NULL, 1, 1, NULL),
# MAGIC       ('440188', NULL, 1, 1, NULL),
# MAGIC       ('440288', NULL, 1, 1, NULL),
# MAGIC       ('440388', NULL, 1, 1, NULL),
# MAGIC       ('440488', NULL, 1, 1, NULL),
# MAGIC       ('440588', NULL, 1, 1, NULL),
# MAGIC       ('440688', NULL, 1, 1, NULL),
# MAGIC       ('440788', NULL, 1, 1, NULL),
# MAGIC       ('440888', NULL, 1, 1, NULL),
# MAGIC       ('449988', NULL, 1, 1, NULL),
# MAGIC       ('450888', NULL, 1, 1, NULL),
# MAGIC       ('450988', NULL, 1, 1, NULL),
# MAGIC       ('451988', NULL, NULL, 1, NULL),
# MAGIC       ('452788', NULL, NULL, 1, NULL),
# MAGIC       ('452888', NULL, 1, 1, NULL),
# MAGIC       ('452988', NULL, 1, 1, NULL),
# MAGIC       ('468988', NULL, 1, 1, NULL),
# MAGIC       ('469988', NULL, NULL, 1, NULL),
# MAGIC       ('471988', NULL, 1, 1, NULL),
# MAGIC       ('499988', NULL, NULL, 1, NULL)
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
