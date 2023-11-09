# Databricks notebook source
import pyspark
from pyspark.sql.functions import col,max
from pyspark.sql.window import Window

# COMMAND ----------

def silver_table_has_active_keys(df: pyspark.sql.DataFrame, businessKeys : list) -> str:
    if (businessKeys is None) or (businessKeys == []):
        return 'WARNING, Provided list of keys is NONE or EMPTY. Please hand over a none empty list.'
    
    columnList = df.columns
    
    windowSpec  = Window.partitionBy(businessKeys)
    df_unvalid = df_test.withColumn("ISACTIVE",max('Sys_Silver_IsCurrent').over(windowSpec)).where(col("ISACTIVE") == False)

    unvalidCount = df_unvalid.count()

    if unvalidCount != 0:
        unvalidExample = df_unvalid.select(businessKeys).first()
        return f'ERROR, Found {unvalidCount} unvalid values given keys {businessKeys}. One unvalid example is "{unvalidExample}".'

    return 'OK'

# COMMAND ----------

def silver_table_has_bronze_keys(df_silver: pyspark.sql.DataFrame,df_bronze: pyspark.sql.DataFrame, businessKeys : list) -> str:
    if (businessKeys is None) or (businessKeys == []):
        return 'WARNING, Provided list of keys is NONE or EMPTY. Please hand over a none empty list.'
    
    columnListSilver = df_silver.columns
    columnListBronze = df_bronze.columns

    for columnName in businessKeys:
        if columnName not in columnListSilver:
            return f'WARNING, The column "{columnName}" is not part of the silver schema {columnListSilver}.'
        elif columnName not in columnListBronze:
            return f'WARNING, The column "{columnName}" is not part of the bronze schema {columnListBronze}.'
        
    df_silver.createOrReplaceTempView("SILVER")
    df_bronze.createOrReplaceTempView("BRONZE")

    
    condition = " AND ".join([f"s.{key} == b.{key}" for key in businessKeys])

    df_unvalid = spark.sql(f"SELECT s.* FROM SILVER s LEFT ANTI JOIN BRONZE b ON {condition}")
    for c in businessKeys:
        df_unvalid = df_unvalid.filter(col(c) !='NaN')

    unvalidCount = df_unvalid.count()

    if unvalidCount != 0:
        unvalidExample = df_unvalid.select(businessKeys).first()
        return f'ERROR, Found {unvalidCount} unvalid values given keys {businessKeys}. One unvalid example is "{unvalidExample}".'

    return 'OK'

# COMMAND ----------

def column_contains_only_valid_values(df: pyspark.sql.DataFrame, columnName : str, validValues : list) -> str:
    if (validValues is None) or (validValues == []):
        return 'WARNING, Provided list of valid values is NONE or EMPTY. Please hand over a none empty list.'
    
    columnList = df.columns
    if columnName not in columnList:
        return f'WARNING, The column "{columnName}" is not part of the schema {columnList}.'
    
    df_unvalid = df.filter(~col(columnName).isin(validValues))
    unvalidCount = df_unvalid.count()

    if unvalidCount != 0:
        unvalidExample = df_unvalid.first()[columnName]
        return f'ERROR, Found {unvalidCount} unvalid values in column "{columnName}" given list {validValues}. One unvalid example is "{unvalidExample}".'

    return 'OK'
