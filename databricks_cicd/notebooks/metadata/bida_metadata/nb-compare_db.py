# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa


# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER EXTERNAL LOCATION bronze OWNER TO 'akhtar.miah@infinigate.com'
# MAGIC

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'bida_metadata'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import time

# This method compares the 2 databricks databases in terms of table names, table count, column names, column counts and column data types
def compare_2_schemas_structure(database1, database2):
  
  dftables1 = get_all_tables(database1)
  dftables2 = get_all_tables(database2)
  
  if dftables1.collect() != dftables2.collect():
    print('Table list are different for 2 databases')
    return False
  
  dfcolumns1 = get_all_table_columns(database1)
  dfcolumns2 = get_all_table_columns(database2)
  if dfcolumns1.collect() != dfcolumns2.collect():
    print('Column list or column data types list are different for 2 databases')
    return False
  print('Schemas (tables and columns) are exactly the same for 2 databases')
  return True

# This method compares the 2 databricks tables in terms of column names, column counts and column data types
def compare_2_tables_structure(database1, table1, database2, table2):
  
  dfcolumns1 = get_table_columns(database1, table1)
  dfcolumns2 = get_table_columns(database2, table2)
  
  if dfcolumns1.collect() != dfcolumns2.collect():
    print('Table structures and/or data columns data types are different for 2 tables')
    return False
  
  print('Table structures are the same for 2 tables')
  return True
# This method compares the content of the 2 databricks tables in terms of data (Note: This might run very long according to the data size in the comparison tables)
def compare_2_tables_data(database1, table1, database2, table2):
  
  # first check the data structure before running the content test for time efficiency, if structure is the same then compare the content
  
  dfcolumns1 = get_table_columns(database1, table1)
  dfcolumns2 = get_table_columns(database2, table2)
  
  if dfcolumns1.collect() != dfcolumns2.collect():
    print('Table structures and/or data columns data types are different for 2 tables')
    return False
  
  print('Table structures are the same for 2 tables')
  
  # table structures are the same now check the data
  
  dftable1 = spark.sql('select * from {}.{}'.format(database1, table1))
  dftable2 = spark.sql('select * from {}.{}'.format(database2, table2))
  
  if dftable1.collect() != dftable2.collect():
    print('Data stored in 2 tables is different')
    return False
  
  print('Table data is the same for 2 tables')
  return True
# This method returns the table list in a databricks database
def get_all_tables(database):
  dftables = spark.sql('show tables from {}'.format(database))
  return dftables

# This method returns the column list and data types of a table
def get_table_columns(database, table):
  dfcols = spark.sql('desc table {}.{}'.format(database, table))
  dfcols = dfcols.select(
                   lit(database).alias('database'), 
                   lit(tablename).alias('table_name'), 
                   'col_name', 
                   'data_type', 
                   'comment'
                ) 
  
  return dfcols

# This method gets all table columns for all tables in a database
def get_all_table_columns(database):
  
  schema = StructType([
    StructField('database', StringType(), True),
    StructField('table_name', StringType(), True),
    StructField('col_name', StringType(), True),
    StructField('data_type', StringType(), True),
    StructField('comment', StringType(), True)
  ])
  dfcols = spark.createDataFrame(spark.sparkContext.emptyRDD(),                        
                                     schema)  
  dftabs = spark.sql('show tables from {}'.format(database))
  
  print('{} tables in {} database'.format(dftabs.count(), database))  
  # for each table collect the columns and append 
  for row in dftabs.rdd.collect():
    dfcol_tmp = spark.sql('desc table {}.{}'.format(row[0], row[1]))
    
    dfcol_tmp = dfcol_tmp.select(
                                  lit(row[0]).alias('database'), 
                                  lit(row[1]).alias('table_name'), 
                                  'col_name', 
                                  'data_type', 
                                  'comment'
                                ) 
    dfcols = dfcols.union(dfcol_tmp)  
  
  print('{} columns in {} database'.format(dfcols.count(),database))
  
  return dfcols
  

# # finding all tables in bronze_dev, and finding all tables contain product keyword in the table name
# df = get_all_tables('bronze_dev')
# df2 = df.filter(df.tableName.contains('product'))
# display(df2)

# # finding all columns in bronze_uat, and finding all columns contain payment keyword in the column name
# df3 = get_all_table_columns('bronze_uat')
# df4 = df3.filter(df.col_name.contains('payment'))
# display(df4)

# full comparing the structures of wh_staging and wh_prod databases
 
compare_2_schemas_structure('bronze_dev', 'bronze_uat')
# # comparing the structures of wh_staging.customer table and and wh_prod.customer2 table
# compare_2_tables_structure('bronze_dev', 'customer', 'bronze_uat', 'customer2')
# # comparing the table contents (data) of wh_staging.customer and wh_prod.customer2 tables
 
# ompare_2_tables_data('bronze_dev', 'customer', 'bronze_uat', 'customer2')
