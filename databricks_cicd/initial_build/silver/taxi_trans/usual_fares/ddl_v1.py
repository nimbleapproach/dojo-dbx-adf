# Databricks notebook source
dbutils.widgets.text('catalog', '')
catalog = dbutils.widgets.get('catalog')

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.dojo_silver.mapping_prod_category (
        catg_id INT NOT NULL COMMENT 'Category ID',
        catg_desc STRING NOT NULL COMMENT 'Category description',
        supcatg_id INT COMMENT 'Supercategory ID FK',
        valid_from DATE COMMENT 'Date which the category is valid from, nullable as may be unknown',
        valid_to DATE COMMENT 'Date which the category is valid to, the date pertains to the date when the category changed or ended',
        next_catg_id ARRAY<INT> COMMENT 'Array of catg_id values that the given row has changed to on valid_to date',
        prev_catg_id ARRAY<INT> COMMENT 'Array of catg_id values that the given row was previously before the valid_to date'
    )
    COMMENT 'This table provides a mapping of category changes.'
    TBLPROPERTIES (
    delta.enableChangeDataFeed = true,
    delta.columnMapping.mode = 'name',
    table_schema.version = 1
    )
    """
)
