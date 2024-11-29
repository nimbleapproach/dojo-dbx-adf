# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

table_name = dbutils.widgets.get('table_name')
watermark_column = dbutils.widgets.get('watermark_column')

# table_name = 'LedgerJournalTransBiEntities'
# watermark_column = 'TransDate'

# COMMAND ----------


df = spark.sql(f"select cast(to_date(max({watermark_column})) as varchar(50)) max_watermark from bronze_{ENVIRONMENT}.nuvias_d365.{table_name.lower()}")

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col
max_watermark = df.select(col("max_watermark")).first()[0]

current_date = datetime.now().strftime("%Y-%m-%d")

if current_date < max_watermark:
    max_watermark = current_date
    print("Max watermark < current date!")
else:
    print(f"Max watermark {max_watermark}")

# COMMAND ----------

# sheet_names_to_load = get_sheet_names(p = '/mnt/external/vuzion/monthly_export/pending/')
dbutils.notebook.exit(max_watermark)
