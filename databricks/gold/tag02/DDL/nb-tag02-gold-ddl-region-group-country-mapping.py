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
# MAGIC CREATE OR REPLACE TABLE region_group_country_mapping
# MAGIC   ( region_code STRING
# MAGIC       COMMENT 'The Tagetik region code'
# MAGIC     ,country STRING
# MAGIC       COMMENT 'The name of the country'
# MAGIC     ,country_detail STRING
# MAGIC       COMMENT 'Context of the country potentially including the reference to entity'
# MAGIC     ,country_visuals STRING
# MAGIC       COMMENT 'Country name to be used in Power BI visualisations'
# MAGIC     ,region_group STRING
# MAGIC       COMMENT 'The high level region grouping'             
# MAGIC ,CONSTRAINT region_group_country_mapping PRIMARY KEY(region_code)
# MAGIC   )
# MAGIC
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (region_code);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Populate the relevant lookup data...**

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC sqldf= spark.sql(f"""
# MAGIC INSERT INTO gold_{ENVIRONMENT}.tag02.region_group_country_mapping ( region_code, country, country_detail, country_visuals, region_group )
# MAGIC VALUES('AT1', 'Austria','IG Austria','Austria','DACH'),
# MAGIC ('AT2', 'Austria','NU Austria','Austria','DACH'),
# MAGIC ('BE1', 'Belgium','IG Belgium','Belgium','BENELUX'),
# MAGIC ('BE2', 'Belgium','DCB BE','Belgium','BENELUX'),
# MAGIC ('BE3', 'Belgium','Deltalink','Belgium','BENELUX'),
# MAGIC ('BG1', 'Bulgaria','NS Bulgaria','Bulgaria','Eastern Europe'),
# MAGIC ('CH1', 'Switzerland','IG Switzerland','Switzerland','DACH'),
# MAGIC ('CH3', 'Switzerland','NU Switzerland','Switzerland','DACH'),
# MAGIC ('DE1', 'Germany','IG Germany','Germany','DACH'),
# MAGIC ('DE2', 'Germany','Acmeo','Germany','DACH'),
# MAGIC ('DE4', 'Germany','NU Germany','Germany','DACH'),
# MAGIC ('DK1', 'Denmark','IG Denmark','Denmark','Nordics'),
# MAGIC ('DK2', 'Denmark','NU Denmark','Denmark','Nordics'),
# MAGIC ('ES1', 'Spain','NU Spain','Spain','Southern Europe'),
# MAGIC ('FI1', 'Finland','IG Finland','Finland','Nordics'),
# MAGIC ('FI2', 'Finland','NU Finland','Finland','Nordics'),
# MAGIC ('FR1', 'France','IG France','France','Southern Europe'),
# MAGIC ('FR2', 'France','D2B','France','Southern Europe'),
# MAGIC ('FR3', 'France','NU France','France','Southern Europe'),
# MAGIC ('HR2', 'Croatia','NS Croatia','Croatia','Eastern Europe'),
# MAGIC ('IT1', 'Italy','NU Italy','Italy','Southern Europe'),
# MAGIC ('AE1', 'Middle East + Africa','Starlink','UAE','MEA'),
# MAGIC ('NL1', 'Netherlands','IG Netherlands','Netherlands','BENELUX'),
# MAGIC ('NL2', 'Netherlands','NU Netherlands','Netherlands','BENELUX'),
# MAGIC ('NL3', 'Netherlands','DCB NL','Netherlands','BENELUX'),
# MAGIC ('NO1', 'Norway','IG Norway','Norway','Nordics'),
# MAGIC ('NO2', 'Norway','NU Norway','Norway','Nordics'),
# MAGIC ('PL1', 'Poland','NU Poland','Poland','Eastern Europe'),
# MAGIC ('RO2', 'Romania','NS Romania','Romania','Eastern Europe'),
# MAGIC ('SE1', 'Sweden','IG Sweden','Sweden','Nordics'),
# MAGIC ('SE2', 'Sweden','NU Sweden','Sweden','Nordics'),
# MAGIC ('SI1', 'Slovenia','NS Slovenia','Slovenia','Eastern Europe'),
# MAGIC ('UK1', 'UK','IG UK','UK','UK'),
# MAGIC ('UK2', 'Vuzion','VZ UK','UK','UK'),
# MAGIC ('UK3', 'UK','NU UK','UK','UK'),
# MAGIC ('UK4', 'UK','Cloud UK','UK','UK'),
# MAGIC ('NaN', 'Other','Other','Other','Other'),
# MAGIC ('VU', 'Vuzion','Vuzion','UK','UK'),
# MAGIC ('HL5', 'HQ','','HQ','HQ'),
# MAGIC ('HL8', 'HQ','','HQ','HQ'),
# MAGIC ('NL4', 'HQ','','HQ','HQ')
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)