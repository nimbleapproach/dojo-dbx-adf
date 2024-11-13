# Databricks notebook source

# Importing Libraries
import os

# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT
# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

catalog = spark.catalog.currentCatalog()

# COMMAND ---------- 
schema = "dummy"

# COMMAND ----------
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
    """
)


if 'prod' in catalog:
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema} TO az_edw_data_engineers;
        """
    )
    
    spark.sql(
        f"""
        GRANT SELECT ON SCHEMA {catalog}.{schema} TO az_edw_data_engineers_ext_db;
        """
    )     
    spark.sql(
        f"""
        GRANT USAGE ON SCHEMA {catalog}.{schema} TO az_edw_data_engineers_ext_db;
        """
    )   

if 'dev' in catalog:
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.{schema} OWNER TO `az_edw_data_engineers`;
        """
    )
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema} TO az_edw_data_engineers_ext_db;
        """
    )   
if 'uat' in catalog:
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.{schema} OWNER TO `az_edw_data_engineers`;
        """
    )
    spark.sql(
        f"""
        GRANT SELECT ON SCHEMA {catalog}.{schema} TO az_edw_data_engineers_ext_db;
        """
    )   
    spark.sql(
        f"""
        GRANT USAGE ON SCHEMA {catalog}.{schema} TO az_edw_data_engineers_ext_db;
        """
    )