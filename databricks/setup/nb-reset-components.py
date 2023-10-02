# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS igsql03;
# MAGIC CREATE SCHEMA IF NOT EXISTS tag02;
# MAGIC CREATE SCHEMA IF NOT EXISTS NUAV_PRODTRANS_SQLBYOD;
# MAGIC CREATE SCHEMA IF NOT EXISTS NUAV_PROD_SQLBYOD;
# MAGIC CREATE SCHEMA IF NOT EXISTS netsuite;
# MAGIC CREATE SCHEMA IF NOT EXISTS akeneo;
# MAGIC CREATE SCHEMA IF NOT EXISTS cloudblue_pba;
# MAGIC CREATE SCHEMA IF NOT EXISTS cloudblue_oss;
# MAGIC CREATE SCHEMA IF NOT EXISTS nuvias_operations;

# COMMAND ----------

for table in spark.catalog.listTables('igsql03'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema tag02

# COMMAND ----------

for table in spark.catalog.listTables('tag02'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema NUAV_PRODTRANS_SQLBYOD

# COMMAND ----------

for table in spark.catalog.listTables('NUAV_PRODTRANS_SQLBYOD'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema NUAV_PROD_SQLBYOD

# COMMAND ----------

for table in spark.catalog.listTables('NUAV_PROD_SQLBYOD'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS igsql03;
# MAGIC CREATE SCHEMA IF NOT EXISTS tag02;
# MAGIC CREATE SCHEMA IF NOT EXISTS NUAV_PRODTRANS_SQLBYOD;
# MAGIC CREATE SCHEMA IF NOT EXISTS NUAV_PROD_SQLBYOD;
# MAGIC CREATE SCHEMA IF NOT EXISTS netsuite;
# MAGIC CREATE SCHEMA IF NOT EXISTS akeneo;
# MAGIC CREATE SCHEMA IF NOT EXISTS cloudblue_pba;
# MAGIC CREATE SCHEMA IF NOT EXISTS cloudblue_oss;
# MAGIC CREATE SCHEMA IF NOT EXISTS nuvias_operations;

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS obt;
