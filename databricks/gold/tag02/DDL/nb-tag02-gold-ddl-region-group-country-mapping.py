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
# MAGIC     ,country_code STRING
# MAGIC       COMMENT 'The ISO country code related to the country'      
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
# MAGIC INSERT INTO gold_{ENVIRONMENT}.tag02.region_group_country_mapping ( region_code, country, country_code, country_detail, country_visuals, region_group )
# MAGIC VALUES('AT1', 'Austria', 'AT', 'IG Austria','Austria','DACH'),
# MAGIC ('AT2', 'Austria', 'AT', 'NU Austria','Austria','DACH'),
# MAGIC ('AT3', 'Austria', 'AT', 'IG Austria','Austria','DACH'),
# MAGIC ('AU1', 'Australia', 'AU', 'IG Australia','Australia','Australasia'),
# MAGIC ('BE1', 'Belgium', 'BE', 'IG Belgium','Belgium','BENELUX'),
# MAGIC ('BE2', 'Belgium', 'BE', 'DCB BE','Belgium','BENELUX'),
# MAGIC ('BE3', 'Belgium', 'BE', 'Deltalink','Belgium','BENELUX'),
# MAGIC ('BE4', 'Belgium', 'BE', 'IG Belgium','Belgium','BENELUX'),
# MAGIC ('BG1', 'Bulgaria', 'BG', 'NS Bulgaria','Bulgaria','Eastern Europe'),
# MAGIC ('CH1', 'Switzerland', 'CH', 'IG Switzerland','Switzerland','DACH'),
# MAGIC ('CH2', 'Switzerland', 'CH', 'NU Switzerland','Switzerland','DACH'),
# MAGIC ('CH3', 'Switzerland', 'CH', 'NU Switzerland','Switzerland','DACH'),
# MAGIC ('CH4', 'Switzerland', 'CH', 'NU Switzerland','Switzerland','DACH'),
# MAGIC ('DE1', 'Germany', 'DE', 'IG Germany','Germany','DACH'),
# MAGIC ('DE2', 'Germany', 'DE', 'Acmeo','Germany','DACH'),
# MAGIC ('DE3', 'Germany', 'DE', 'IG Germany','Germany','DACH'),
# MAGIC ('DE4', 'Germany', 'DE', 'NU Germany','Germany','DACH'),
# MAGIC ('DE5', 'Germany', 'DE', 'IG Germany','Germany','DACH'),
# MAGIC ('DE6', 'Germany', 'DE', 'IG Germany','Germany','DACH'),
# MAGIC ('DK1', 'Denmark', 'DK', 'IG Denmark','Denmark','Nordics'),
# MAGIC ('DK2', 'Denmark', 'DK', 'NU Denmark','Denmark','Nordics'),
# MAGIC ('DK3', 'Denmark', 'DK', 'IG Denmark','Denmark','Nordics'),
# MAGIC ('DK4', 'Denmark', 'DK', 'IG Denmark','Denmark','Nordics'),
# MAGIC ('ES1', 'Spain', 'ES', 'NU Spain','Spain','Southern Europe'),
# MAGIC ('ES2', 'Spain', 'ES', 'NU Spain','Spain','Southern Europe'),
# MAGIC ('FI1', 'Finland', 'FI', 'IG Finland','Finland','Nordics'),
# MAGIC ('FI2', 'Finland', 'FI', 'NU Finland','Finland','Nordics'),
# MAGIC ('FR1', 'France', 'FR', 'IG France','France','Southern Europe'),
# MAGIC ('FR2', 'France', 'FR', 'D2B','France','Southern Europe'),
# MAGIC ('FR3', 'France', 'FR', 'NU France','France','Southern Europe'),
# MAGIC ('HR1', 'Croatia', 'HR', 'NS Croatia','Croatia','Eastern Europe'),
# MAGIC ('HR2', 'Croatia', 'HR', 'NS Croatia','Croatia','Eastern Europe'),
# MAGIC ('IE1', 'Vuzion', 'VU', 'VZ UK','UK','UK'),
# MAGIC ('IT1', 'Italy', 'IT', 'NU Italy','Italy','Southern Europe'),
# MAGIC ('IT2', 'Italy', 'IT', 'NU Italy','Italy','Southern Europe'),
# MAGIC ('AE1', 'Middle East + Africa', 'AE', 'Starlink','UAE','MEA'),
# MAGIC ('NL1', 'Netherlands', 'NL', 'IG Netherlands','Netherlands','BENELUX'),
# MAGIC ('NL2', 'Netherlands', 'NL', 'NU Netherlands','Netherlands','BENELUX'),
# MAGIC ('NL3', 'Netherlands', 'NL', 'DCB NL','Netherlands','BENELUX'),
# MAGIC ('NL4', 'Netherlands', 'NL', 'IG Netherlands','Netherlands','BENELUX'),
# MAGIC ('NL5', 'Netherlands', 'NL', 'IG Netherlands','Netherlands','BENELUX'),
# MAGIC ('NO1', 'Norway', 'NO', 'IG Norway','Norway','Nordics'),
# MAGIC ('NO2', 'Norway', 'NO', 'NU Norway','Norway','Nordics'),
# MAGIC ('PL1', 'Poland', 'PL', 'NU Poland','Poland','Eastern Europe'),
# MAGIC ('RO1', 'Romania', 'RO', 'NS Romania','Romania','Eastern Europe'),
# MAGIC ('RO2', 'Romania', 'RO', 'NS Romania','Romania','Eastern Europe'),
# MAGIC ('SE1', 'Sweden', 'SE', 'IG Sweden','Sweden','Nordics'),
# MAGIC ('SE2', 'Sweden', 'SE', 'NU Sweden','Sweden','Nordics'),
# MAGIC ('SI1', 'Slovenia', 'SI', 'NS Slovenia','Slovenia','Eastern Europe'),
# MAGIC ('UK1', 'UK', 'GB', 'IG UK','UK','UK'),
# MAGIC ('UK2', 'Vuzion', 'GB', 'VZ UK','UK','UK'),
# MAGIC ('UK3', 'UK', 'GB', 'NU UK','UK','UK'),
# MAGIC ('UK4', 'UK', 'GB', 'Cloud UK','UK','UK'),
# MAGIC ('US1', 'USA', 'US', 'IG United States of America','USA','North America'),
# MAGIC ('NaN', 'Other', 'NA', 'Other','Other','Other'),
# MAGIC ('VU', 'Vuzion', 'GB', 'Vuzion','UK','UK'),
# MAGIC ('HL1', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL10', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL11', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL12', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL13', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL14', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL15', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL16', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL17', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL18', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL19', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL2', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL20', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL3', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL4', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL5', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL6', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL7', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL8', 'HQ', 'HQ', '','HQ','HQ'),
# MAGIC ('HL99', 'HQ', 'HQ', '','HQ','HQ')
# MAGIC """)
# MAGIC
# MAGIC #display(sqldf)
