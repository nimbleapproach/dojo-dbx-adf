# Databricks notebook source

# Importing Libraries
import os
# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT
# COMMAND ----------
import warnings
# Check if the user has permission to create a catalog
try:
    query = f"""
    CREATE CATALOG IF NOT EXISTS bida_metadata_{ENVIRONMENT}
    COMMENT 'This is Database to hold all the meta data around the cicd build & parts of the etl'
    """
    spark.sql(query)
    # Execute the SQL command
    spark.sql(f"""GRANT USAGE ON CATALOG bida_metadata_{ENVIRONMENT} TO az_edw_data_engineers_ext_db;""")
    
    has_permission = True
except Exception as e:
    print(f"Permission check failed: {str(e)}")
    warnings.warn("User does not have permission to create a catalog. Continuing with the rest of the code.")
    has_permission = False
# COMMAND ----------
spark.catalog.setCurrentCatalog(f"bida_metadata_{ENVIRONMENT}")

catalog = spark.catalog.currentCatalog()

# COMMAND ---------- 
schema = "meta"
# COMMAND ----------

# If the user has permission, create the catalog
if has_permission:
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
else:
    print("User does not have permission to create a catalog.")