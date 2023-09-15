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

try:
    WATERMARK_COLUMN = dbutils.widgets.get("wg_watermarkColumn")
except:
    dbutils.widgets.text(name = "wg_watermarkColumn", defaultValue = 'DATEUPD')
    WATERMARK_COLUMN = dbutils.widgets.get("wg_watermarkColumn")

# COMMAND ----------

TABLE_NAME = TABLE_NAME.lower()
TABLE_SCHEMA = TABLE_SCHEMA.lower()

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

spark.sql(f"""
          USE SCHEMA {TABLE_SCHEMA}
          """)


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

target_df = spark.read.table(f'silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}')

# COMMAND ----------

SILVER_PRIMARY_KEYS = [key['column_name'] for key in spark.sql(f"""
SELECT a.column_name FROM information_schema.constraint_column_usage a
join information_schema.table_constraints b
on a.constraint_name = b.constraint_name
where a.table_schema = '{TABLE_SCHEMA}'
and a.table_name = '{TABLE_NAME}'
and b.constraint_type = 'PRIMARY KEY'
""").collect()]

BUSINESS_KEYS = SILVER_PRIMARY_KEYS
BUSINESS_KEYS.remove(WATERMARK_COLUMN)

# COMMAND ----------

currentWatermark = (
                    target_df
                    .agg(
                        coalesce(
                            max(col('Sys_Bronze_InsertDateTime_UTC').cast('TIMESTAMP')),
                            lit('1900-01-01').cast('TIMESTAMP')
                            )
                        .alias('current_watermark'))
                    .collect()[0]['current_watermark']
                    )

# COMMAND ----------

target_columns = target_df.columns
hash_columns = [col(column) for column in target_columns if not (column.startswith('Sys_') or column == f'{WATERMARK_COLUMN}')]

# COMMAND ----------

source_df = (
            spark
            .read
            .table(f'bronze_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}')
            .withColumn('Sys_Silver_InsertDateTime_UTC', current_timestamp())
            .withColumn('Sys_Silver_ModifedDateTime_UTC', current_timestamp())
            .withColumn('Sys_Silver_HashKey', hash(*hash_columns))
            .select(target_columns)
            .where(col('Sys_Bronze_InsertDateTime_UTC') > currentWatermark)
            .dropDuplicates()
            )

# COMMAND ----------

deduped_df = source_df.dropDuplicates(SILVER_PRIMARY_KEYS)

# COMMAND ----------

target_update_columns = [column for column in target_columns if column != 'Sys_Silver_InsertDateTime_UTC']
source_update_columns = [f's.{column}' for column in target_update_columns]
updateDict = dict(zip(target_update_columns,source_update_columns))

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forName(spark,tableOrViewName=f"silver_{ENVIRONMENT}.{TABLE_SCHEMA}.{TABLE_NAME}")

condition = " AND ".join([f's.{SILVER_PRIMARY_KEYS[i]} = t.{SILVER_PRIMARY_KEYS[i]}' for i in range(len(SILVER_PRIMARY_KEYS))])
(deltaTable.alias("t").merge(
deduped_df.alias("s"),
condition)
.whenMatchedUpdate(set = updateDict)
.whenNotMatchedInsertAll()
.execute()
)

# COMMAND ----------

spark.sql(f"""
          VACUUM {TABLE_NAME}
          """)

# COMMAND ----------

currentVersion = spark.sql(f"""
          DESCRIBE HISTORY {TABLE_NAME}
          """).agg(max('version').alias('current_version')).collect()[0]['current_version']

# COMMAND ----------

print(f'The current table version is: {currentVersion}')
if currentVersion % 5 == 0:
    print(f'We are optimizing the table by using liquid clustering on {BUSINESS_KEYS}')
    spark.sql(
        f"""
        OPTIMIZE {TABLE_NAME}
        """
    )
else:
    print(f'Since {currentVersion} is not divisible by 5 we are not optimizing.')
