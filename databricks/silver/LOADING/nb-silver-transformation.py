# Databricks notebook source
try:
    ENVIRONMENT = dbutils.widgets.get("wg_environment")
except:
    dbutils.widgets.dropdown(name = "wg_environment", defaultValue = 'dev', choices = ['dev','uat','prod'])
    ENVIRONMENT = dbutils.widgets.get("wg_environment")

# COMMAND ----------

try:

    TABLE_NAME = dbutils.widgets.get("wg_tableName")
except:
    dbutils.widgets.text(name = "wg_tableName", defaultValue = 'AZIENDA')
    TABLE_NAME = dbutils.widgets.get("wg_tableName")

# COMMAND ----------

try:
    TABLE_SCHEMA = dbutils.widgets.get("wg_tableSchema")
except:
    dbutils.widgets.text(name = "wg_tableSchema", defaultValue = 'tag02')
    TABLE_SCHEMA = dbutils.widgets.get("wg_tableSchema")

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

spark.sql(f"""
          USE SCHEMA {TABLE_SCHEMA}
          """)


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Populating the empty fields in CurrencyCode column with local currency of the entity based on the source DB name

# COMMAND ----------

data = [{"CountryCode": 'CH', "CurrencyCode": 'CHF'},
        {"CountryCode": 'DE', "CurrencyCode": 'EUR'},
        {"CountryCode": 'FR', "CurrencyCode": 'EUR'},
        {"CountryCode": 'UK', "CurrencyCode": 'GBP'},
        {"CountryCode": 'NL', "CurrencyCode": 'EUR'},
        {"CountryCode": 'NO', "CurrencyCode": 'NOK'},
        {"CountryCode": 'SE', "CurrencyCode": 'SEK'},
        {"CountryCode": 'DK', "CurrencyCode": 'DKK'},
        {"CountryCode": 'FI', "CurrencyCode": 'EUR'}
        ]

df_currency = spark.createDataFrame(data)
df_currency.createOrReplaceTempView("currency")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW fact_updated AS
# MAGIC
# MAGIC select a.*, 
# MAGIC case when a.CurrencyCode = '' then currency.CurrencyCode 
# MAGIC else a.CurrencyCode end as CurrencyCodeUpdate 
# MAGIC FROM silver_dev.igsql03.sales_invoice_header_copy a
# MAGIC LEFT JOIN currency ON right(a.Sys_DatabaseName, 2) = currency.CountryCode
# MAGIC WHERE a.CurrencyCode ='';
# MAGIC
# MAGIC MERGE INTO silver_dev.igsql03.sales_invoice_header_copy  t 
# MAGIC using fact_updated on t.Sys_RowNumber = fact_updated.Sys_RowNumber
# MAGIC AND t.Sys_Silver_HashKey = fact_updated.Sys_Silver_HashKey
# MAGIC WHEN MATCHED THEN UPDATE SET CurrencyCode = fact_updated.CurrencyCodeUpdate
# MAGIC
# MAGIC

# COMMAND ----------

spark.catalog.dropTempView("fact_updated")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Fill in the empty or NULLs with "NoValue"

# COMMAND ----------


df = spark.sql(f"select * from silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}")

# fill NULL
df = df.fillna(value= 'NoValue') 
# fill blanks
df = df.replace('', 'NoValue')


# COMMAND ----------

df = df.fillna(value= 'NoValue')
df = df.replace('', 'NoValue')

# COMMAND ----------

SILVER_PRIMARY_KEYS = [key['column_name'] for key in spark.sql(f"""
SELECT a.column_name FROM information_schema.constraint_column_usage a
join information_schema.table_constraints b
on a.constraint_name = b.constraint_name
where a.table_schema = '{TABLE_SCHEMA}'
and a.table_name = '{TABLE_NAME}'
and b.constraint_type = 'PRIMARY KEY'
""").collect()]

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark,tableOrViewName=f"silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}")

condition = " AND ".join([f's.{SILVER_PRIMARY_KEYS[i]} = t.{SILVER_PRIMARY_KEYS[i]}' for i in range(len(SILVER_PRIMARY_KEYS))])
(deltaTable.alias("t").merge(
df.alias("s"),
condition)
.whenMatchedUpdate(set = df)
# .whenNotMatchedInsertAll()
.execute()
)
