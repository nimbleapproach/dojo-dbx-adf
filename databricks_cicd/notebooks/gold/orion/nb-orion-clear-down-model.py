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
# link tables remove all
clear_tables('link',data)
# fact tables remove all
clear_tables('fact',data) 
# dim tables remove where _pk > 0
clear_tables('dim',data)

#now we have to clear down the fact_delta_timestamp table to allow facts to be re-processed
spark.sql(f"Delete From {catalog}.{orion_schema}.fact_delta_timestamp  Where max_transaction_line_timestamp >CAST('2000-01-01' AS TIMESTAMP)")
df=spark.sql(f"SELECT * FROM {catalog}.{orion_schema}.fact_delta_timestamp")
#df.display()