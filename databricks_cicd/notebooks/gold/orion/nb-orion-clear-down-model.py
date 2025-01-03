# Databricks notebook source
# MAGIC %run ./nb-orion-meta

# COMMAND ----------

file_path = 'meta.json'
replacements = {
    "processing_notebook": processing_notebook,
    "ENVIRONMENT": ENVIRONMENT,
    "orion_schema": orion_schema
}
data = read_and_replace_json(file_path, replacements)

# order of deletes - link tables, fact tables, dim tables
# link tables clear all
clear_tables('link',data)
# fact tables clear all
clear_tables('fact',data) 
# dim tables clear where _pk > 0
clear_tables('dim',data)

# now we have to clear down the fact_delta_timestamp table to allow facts to be re-processed as the timestamp in here
# is used in the fact views to select fact records > max_transaction_line_timestamp.  Hence remove all timestamps > 2000-01-01
# This will cause a full refresh of the fact table
reset_fact_delta_timestamp(catalog,orion_schema)