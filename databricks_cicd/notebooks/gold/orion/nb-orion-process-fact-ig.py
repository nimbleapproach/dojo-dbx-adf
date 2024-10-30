# Databricks notebook source
# MAGIC %run ./nb-orion-meta

# COMMAND ----------

# Importing Libraries
import os
spark = spark  # noqa


# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT
spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

# Example usage
file_path = './meta.json'
replacements = {
    "processing_notebook": processing_notebook,
    "ENVIRONMENT": ENVIRONMENT,
    "orion_schema": orion_schema
}
data = read_and_replace_json(file_path, replacements)

# 
# Notebook to process a single dimension based on the name being passed via widgets
#
dbutils.widgets.text("dimension_name", "", "Dimension Name")
dimension_name = dbutils.widgets.get("dimension_name")
#DEBUG dimension_name = 'entity_to_entity_group_link'
dimension_name = 'fact_sales_transaction_ig' # only called dimension name because the parent notebook uses it

#grab the config items fopr the fgact tables
fact_table_name_only = dimension_name
fact_config = get_object_detail(data, fact_table_name_only)
destination_fact_table_name = fact_config['destination_table_name'] # string
# grab all the destination fact table columns.  Need this to eliminate joining to a dimension table
# that the fact table is not directly related to
destination_columns_list = spark.table(f"{destination_fact_table_name}").columns
fact_direct_dimensions = [fk_column.replace("_fk","") for fk_column in destination_columns_list if '_fk' in fk_column]

source_fact_table_name = fact_config['source_table_name'] # string
destination_fact_key_columns = fact_config['destination_key_columns'] # string
source_fact_key_columns = fact_config['source_key_columns'] # string

# only grab the columns that exist in both source and destination
join_columns = list(set(destination_fact_key_columns) & set(source_fact_key_columns))
join_sql = ""
for col in join_columns:
    join_sql = f" AND dest.{col} = src.{col} " + join_sql  
join_sql = join_sql[5:] # remove the first AND statement

# COMMAND ----------

from pyspark.sql import functions as F
# build a dataframe for each dimension of the _pk ciolumn AND the business keys FROM the config
# grab the fact daraframe AND JOIN the dataframes together dropping the business keys AND hence keeping the _pk columns AND measures
def get_dimension_keys(dimension_name):
    
    # grab the config FROM the dictionary
    dimension_config = get_object_detail(data, dimension_name)
    # destination config
    destination_key_columns = dimension_config['destination_key_columns'] # string
    destination_dimension_table_name = dimension_config['destination_table_name'] # string
    destination_key_columns_lower = [destination_col.lower() for destination_col in destination_key_columns]
    
    # grab the latest dimensions members
    # Future - will have to incorporate start and end datetimes
    # need to know the datetime column name from the fact staging layer is it simply document_date
    # we already know the dim start and end column names, start_datetime, end_datetime
    select_sql = f"""SELECT {dimension_name}_pk,{', '.join(destination_key_columns_lower)} 
                     FROM {destination_dimension_table_name}
                     WHERE is_current = 1"""
    dimension_df = spark.sql(select_sql)
    
    return dimension_df, destination_key_columns_lower
    
# get the current timestamp for fact insert
insert_ts = F.current_timestamp()

df_fact_staging = spark.table(source_fact_table_name)
df_fact_staging.cache()

# get a list of the active dimensions from json that we are going to use to join to the fact table
dimension_names = list({key: value for key, value in data.items() if value.get("type") == "dim"  and value.get("active") == 1})

# how to handle -1 dim members
for dim_name in dimension_names:
    # only try and join dimensions that are directly related to the fact
    if dim_name not in fact_direct_dimensions:
        continue
    df_dim, bus_keys = get_dimension_keys(dim_name.lower()) #bus_keys should all be LOWER CASE now
    print("processing" , dim_name, "using", bus_keys)
    #print(df_fact_staging.count(),dim_name)
    
    # Future - might need to incorporate the fact staging document_date as a filter to the dimension join
    # Future - the is_current might need to be removed from the above get_dimension_keys function
    # Future - and then this filter added to the .join
    # .filter(F.col("start_datetime") > F.col("document_date") AND F.col("coalesce(end_datetime, '2099-01-01'") <= F.col("document_date"))
    df_fact_temp = df_fact_staging.join(df_dim, [*bus_keys], 'inner')
    if "source_system_fk" in bus_keys:
        bus_keys.remove("source_system_fk") # don't remove this column from the dataframe
    if "document_source" in bus_keys:
        bus_keys.remove("document_source") # don't remove this column from the dataframe
    df_fact_temp_insert = df_fact_temp.drop(*bus_keys) # drop the business keys e.g. product_code, document_code etc..
    df_fact_staging = df_fact_temp_insert

# COMMAND ----------

# rename all the fact _pk columns to _fk
from functools import reduce

col_list = df_fact_temp_insert.columns
df_fact_renamed = reduce(lambda data, name: data.withColumnRenamed(name, name.replace('_pk', '_fk')), 
        col_list, df_fact_temp_insert)

df_fact_delta = df_fact_renamed.withColumn('Sys_Gold_FactProcessedDateTime_UTC', insert_ts)

# Merge the FACT table records for those local_fact_ids, source_system_ids that have changed
# df_fact_delta contains all the new and updated fact records, upsert it - no SCD2 here
deltaTableFact = DeltaTable.forName(spark, destination_fact_table_name)

# this is the UPSERT logic for the F&D fact table 
# the upsert below will always insert new records and update existing records if joined on source_system_id, document_source and local_fact_id
deltaTableFact.alias('dest') \
.merge(df_fact_delta.alias("src"), join_sql) \
.whenMatchedUpdateAll() \
.whenNotMatchedInsertAll() \
.execute()

spark.sql(f"OPTIMIZE {destination_fact_table_name}")

# now update the fact_delta_timestamp table for each document source aka transaction line max(inserted or modified datetime)
spark.sql(f"""
with cte_max_timestamp
(
        SELECT 
                document_source, 
                CASE WHEN MAX(Sys_Gold_InsertedDateTime_UTC) > MAX(Sys_Gold_ModifiedDateTime_UTC)
                        THEN MAX(Sys_Gold_InsertedDateTime_UTC)
                        ELSE  MAX(Sys_Gold_ModifiedDateTime_UTC)
                END AS max_transaction_line_timestamp
        FROM {source_fact_table_name}
        GROUP BY document_source
)
INSERT INTO {catalog}.{schema}.fact_delta_timestamp (document_source,max_transaction_line_timestamp, Sys_Gold_FactProcessedDateTime_UTC)
SELECT cte_max_timestamp.document_source, cte_max_timestamp.max_transaction_line_timestamp, current_timestamp()
FROM cte_max_timestamp
"""
)

# COMMAND ----------
dbutils.notebook.exit(0)