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
# MAGIC CREATE OR REPLACE TABLE entity_group_mapping
# MAGIC   ( entity_code STRING
# MAGIC       COMMENT 'The IG entity code'
# MAGIC     ,entity_group STRING
# MAGIC       COMMENT 'The high level entity grouping' 
# MAGIC     ,entity_code_legacy STRING
# MAGIC       COMMENT 'Legacy reference to the Infinigate entity Id'                    
# MAGIC ,CONSTRAINT entity_group_mapping PRIMARY KEY(entity_code)
# MAGIC   )
# MAGIC
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (entity_code);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Populate the relevant lookup data...**

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql(f"""
# MAGIC INSERT INTO gold_{ENVIRONMENT}.tag02.entity_group_mapping ( entity_code, entity_group, entity_code_legacy )
# MAGIC VALUES('AT1', 'Infinigate','AT1+DE1'),
# MAGIC ('AT2', 'Nuvias','AT2'),
# MAGIC ('BE1', 'Infinigate','BE1+NL1'),
# MAGIC ('BE2', 'Nuvias','BE2'),
# MAGIC ('BE3', 'Nuvias','BE3'),
# MAGIC ('BG1', 'Netsafe','BG1'),
# MAGIC ('CH1', 'Infinigate','CH1'),
# MAGIC ('CH3', 'Nuvias','CH3'),
# MAGIC ('DE1', 'Infinigate','AT1+DE1'),
# MAGIC ('DE2', 'Infinigate','DE2'),
# MAGIC ('DE4', 'Nuvias','DE4'),
# MAGIC ('DK1', 'Infinigate','DK1'),
# MAGIC ('DK2', 'Nuvias','DK2'),
# MAGIC ('ES1', 'Nuvias','ES1'),
# MAGIC ('FI1', 'Infinigate','FI1'),
# MAGIC ('FI2', 'Nuvias','FI2'),
# MAGIC ('FR1', 'Infinigate','FR1'),
# MAGIC ('FR2', 'Infinigate','FR2'),
# MAGIC ('FR3', 'Nuvias','FR3'),
# MAGIC ('HR2', 'Netsafe','HR2'),
# MAGIC ('IT1', 'Nuvias','IT1'),
# MAGIC ('AE1', 'Starlink','AE1'),
# MAGIC ('NL1', 'Infinigate','BE1+NL1'),
# MAGIC ('NL2', 'Nuvias','NL2'),
# MAGIC ('NL3', 'Nuvias','NL3'),
# MAGIC ('NO1', 'Infinigate','NO1'),
# MAGIC ('NO2', 'Nuvias','NO2'),
# MAGIC ('PL1', 'Nuvias','PL1'),
# MAGIC ('RO2', 'Netsafe','RO2'),
# MAGIC ('SE1', 'Infinigate','SE1'),
# MAGIC ('SE2', 'Nuvias','SE2'),
# MAGIC ('SI1', 'Netsafe','SI1'),
# MAGIC ('UK1', 'Infinigate','UK1'),
# MAGIC ('UK2', 'IG Cloud','UK2'),
# MAGIC ('UK3', 'Nuvias','UK3'),
# MAGIC ('UK4', 'Nuvias','UK4'),
# MAGIC ('NaN', 'Other','NaN'),
# MAGIC ('VU', 'IG Cloud','VU'),
# MAGIC ('HL5', 'HQ','HL5'),
# MAGIC ('HL8', 'HQ','HL8'),
# MAGIC ('NL4', 'HQ','NL4')
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
