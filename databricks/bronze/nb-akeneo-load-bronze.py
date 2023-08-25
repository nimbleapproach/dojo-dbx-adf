# Databricks notebook source
# MAGIC %run ../setup/akeneo/nb-setup-akeneo

# COMMAND ----------

try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
except:
    dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices =  ['dev','uat','prod'])
    ENVIRONMENT = dbutils.widgets.get("wg_environment")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema akeneo;

# COMMAND ----------

bronzeProducts_df = spark.read.table('products')

# COMMAND ----------

currentWatermark = str(bronzeProducts_df.select(max('updated').cast('Timestamp').alias('currentWatermark')).collect()[0]['currentWatermark'])

# COMMAND ----------

if currentWatermark == 'None':
    currentWatermark = "1900-01-01 10:00:00"

# COMMAND ----------

currentWatermark

# COMMAND ----------

products = Products()
jsonData = products.getData(updatedAfter=currentWatermark)

# COMMAND ----------

products.writeProducts(updatedAfter=currentWatermark,onlyFirstPage=False)
