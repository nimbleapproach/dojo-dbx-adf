# Databricks notebook source

dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')


dbutils.widgets.text('schema_ar', 'dojo_db1')
schema_ar = dbutils.widgets.get('schema_ar')
# COMMAND ----------



spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_ar};
    """
)

if catalog != 'prod_demo_cicd':
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema_ar} TO dojo_auto;
        """
    )   

if catalog == 'dev_demo_cicd':
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.{schema_ar} OWNER TO `dojo_auto`;
        """
    ) 