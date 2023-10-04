# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
SCHEMAS = ["cloudblue_pba","igsql03","netsuite","nuvias_operations","tag02"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

import pyspark.sql.utils

def addColumnIfNotExists(tableSchema : str, tableName : str, columnName : str, dataType : str) -> None:
    try:
        spark.sql(f"""ALTER TABLE {tableSchema}.{tableName} ADD COLUMN  {columnName} {dataType}""")
    except pyspark.sql.utils.AnalysisException:
        print("Column already exists")

# COMMAND ----------

def addSystemColumns(tableSchema : str, tableName : str) -> None:
    columnList = [('Sys_Silver_InsertDateTime_UTC','TIMESTAMP'), ('Sys_Silver_ModifedDateTime_UTC','TIMESTAMP'),( 'Sys_Silver_HashKey','BIGINT'), ('Sys_Silver_IsCurrent','BOOLEAN')]

    assert spark.catalog.databaseExists(tableSchema), f"Schema {tableSchema} does not exists in catalog silver."
    spark.catalog.setCurrentDatabase(tableSchema)
    assert spark.catalog.tableExists(f"{tableSchema}.{tableName}"), f"Table {tableName} does not exists in schema {tableSchema}."

    for column in columnList:
        addColumnIfNotExists(tableSchema = tableSchema,
                            tableName = tableName,
                            columnName = column[0],
                            dataType = column[1])

# COMMAND ----------

for schema in SCHEMAS:
    for table in spark.catalog.listTables(schema):
        addSystemColumns(tableSchema=schema,tableName=table.name)
