# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog bronze;

# COMMAND ----------

for table in spark.catalog.listTables('igsql03'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)

# COMMAND ----------

for table in spark.catalog.listTables('tag02'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)
