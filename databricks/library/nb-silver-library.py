# Databricks notebook source
import pyspark

# COMMAND ----------

test_df = spark.read.table("bronze_dev.tag02.azienda")

# COMMAND ----------

def fillnas(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame: 
    columns = df.columns

    for col in columns:
        if  dict(df.dtypes)[col] =='string':
            df = (df.fillna(value= 'NaN',subset=[col])
                .replace('', 'NaN', col)
                .replace(' ', 'NaN', col))
            
        # elif dict(df.dtypes)[col] =='timestamp':
        #     df = (df.fillna(value= "1900-01-01", subset=[col]))
        # else:
        #     df = (df.fillna(value= -float('Inf'), subset=[col]))
    
    return df
