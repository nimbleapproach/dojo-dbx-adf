# Databricks notebook source

dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.dojo_gold;
    """
)

if catalog != 'prod_demo_cicd':
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.dojo_gold TO dojo_auto;
        """
    )   

if catalog == 'dev_demo_cicd':
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.`dojo_gold` OWNER TO `dojo_auto`;
        """
    )

# permissions to access models
spark.sql(
    f"""
    GRANT USE_SCHEMA, EXECUTE, READ_VOLUME, SELECT ON SCHEMA {catalog}.dojo_gold TO `account users`;
    """
)
