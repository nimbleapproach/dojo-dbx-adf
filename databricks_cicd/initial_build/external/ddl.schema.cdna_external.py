# Databricks notebook source
dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.dojo_external;
    """
)

if catalog != 'prod_demo_cicd':
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.dojo_external TO dojo_auto;
        """
    )   

if catalog == 'dev_demo_cicd':
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.`dojo_external` OWNER TO `dojo_auto`;
        """
    )
