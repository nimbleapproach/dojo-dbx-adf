# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema igsql03

# COMMAND ----------

for table in spark.catalog.listTables('igsql03'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema tag02

# COMMAND ----------

for table in spark.catalog.listTables('tag02'):
    spark.sql(f"""
              DROP TABLE {table.name}
              """)
