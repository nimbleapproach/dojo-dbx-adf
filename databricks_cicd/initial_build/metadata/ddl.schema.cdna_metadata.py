# Databricks notebook source
dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.dojo_metadata;
    """
)

if catalog == 'dev_demo_cicd':
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.dojo_metadata OWNER TO `dojo_auto`;
        """
    ) 

if catalog != 'prod_demo_cicd':
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.dojo_metadata TO dojo_auto;
        """
    )   

# Create volumes used in that schema
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.dojo_metadata.autodoc;
    """
)

# Create volumes used in that schema
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.dojo_metadata.checkpoints COMMENT "streaming checkpoints";
    """
)

