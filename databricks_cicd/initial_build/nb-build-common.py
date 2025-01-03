# Databricks notebook source
def add_column_if_not_exists(catalog, schema, table, column_name, column_type):
    # Check if column exists
    columns = spark.sql(f"DESCRIBE {catalog}.{schema}.{table}").select("col_name").collect()
    if column_name not in [row.col_name for row in columns]:
        spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} ADD COLUMN {column_name} {column_type}")

