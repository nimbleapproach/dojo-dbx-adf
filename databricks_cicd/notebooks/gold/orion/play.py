# Databricks notebook source
# MAGIC %run ./99_orion_global

# COMMAND ----------

# create some views of master data in the masterdata catalog - this can be pointing to a table or a cte etc..
# create some 


# COMMAND ----------

dimensions_dict = {
    "product" : {"destination_table_name" : "products", "key_column" : "product_code", "destination_columns" : ['product_description'],
                 "source_table_name" : "staging_products" , "source_columns" : ['product_code','product_description']
                 },
#    "date" : "date",
    "currency" : {"destination_table_name" : "currency",  "key_column" : "product_code",
                 "source_table_name" : "staging_products" ,  "key_column" : "product_code"
                 }
}

dimension_config = dimensions_dict['product']
key_column = dimension_config['key_column']
print(key_column)

# for p_id, p_info in dimensions_dict.items():
#     print("\ndimension:", p_id)
    
#     for key in p_info:
#         print(key + ':', p_info[key])




# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gold_dev;
# MAGIC use schema orion;

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


data = [
    ("550e8400-e29b-41d4-a716-446655440000", "a", 1000),
    ("550e8400-e29b-41d4-a716-446655440000", "b", 2000),
    ("550e8400-e29b-41d4-a716-446655440001", "a", 3000),
    ("550e8400-e29b-41d4-a716-446655440001", "b", 4000),
    ("550e8400-e29b-41d4-a716-446655440002", "a", 5000)
]
columns = ["guid", "category", "value"]
df = spark.createDataFrame(data, columns)

windowSpec = Window.partitionBy("guid").orderBy("value")
df = df.withColumn("row_number", row_number().over(windowSpec))
display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE FORMATTED silver_dev.igsql03.g_l_account;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RowCount").getOrCreate()

# Get list of tables in the database
database_name = "your_database_name"
tables = spark.sql(f"SHOW TABLES IN {database_name}").select("tableName").collect()

# Function to get row count from table statistics
def get_row_count(table_name):
    result = spark.sql(f"DESCRIBE DETAIL {database_name}.{table_name}").select("numRows").collect()
    return result[0]["numRows"] if result else 0

# Get row counts for all tables
row_counts = {table.tableName: get_row_count(table.tableName) for table in tables}
print(row_counts)


# COMMAND ----------

common_dimension_columns = [
    "start_datetime", 
    "end_datetime", 
    "is_current", 
    "Sys_Gold_InsertedDateTime_UTC", 
    "Sys_Gold_ModifiedDateTime_UTCTIMESTAMP"
    ]
cols = spark.table(f"gold_dev.orion.products").columns
for c in common_dimension_columns:
    if c in cols:
        print(f"{c} exists")
    else:
        print(f"{c} does not exist in table")

# x = [*common_dimension_columns]
# c = f"""
#     SELECT 
#         account_description,
#         account_description_extended,
#         account_hash_key,
#         {', '.join(x)}
        
#     FROM blah    
#     """
# print(c)

# COMMAND ----------

import os
dimensions_dict = {
    # "products" : {"destination_table_name" : "products", "destination_key_column" : "product_code", "destination_columns" : ["product_description","product_type_internal", "products_hash_key"],
    #              "source_table_name" : "products_staging" , "source_key_column" : "product_code", "source_columns" : ["product_code","product_description","product_type_internal", "products_hash_key"]
    #              },

    "currency" : {"destination_table_name" : "currency",  "destination_key_columns" : ["currency_code"],
                 "source_table_name" : "currency_staging" ,  "source_key_columns" : ["currency_code"]
                 },
    
    "vendors" : {"destination_table_name" : "vendors", "destination_key_columns" : ["vendor_code", "another"],
                 "source_table_name" : "vendors_staging" , "source_key_columns" : ["vendor_code", "another"]

                }       
}

common_dimension_columns = [
    "start_datetime", 
    "end_datetime", 
    "is_current", 
    "Sys_Gold_InsertedDateTime_UTC", 
    "Sys_Gold_ModifiedDateTime_UTC"
    ]
env = os.environ["__ENVIRONMENT__"]
catalog = f"gold_{env}"
schema = "orion"


dimension_name = "vendors"

#grab the config from the dictionary
dimension_config = dimensions_dict[dimension_name] # dict
# destination config
destination_key_columns = dimension_config['destination_key_columns'] # string
destination_dimension_table_name = dimension_config['destination_table_name'] # string

# source of data config
updates_dimension_table = dimension_config['source_table_name'] # string
source_key_columns = dimension_config['source_key_columns'] # string
join_columns = list(set(destination_key_columns) & set(source_key_columns))

join_sql = ""
for col in join_columns:
    join_sql = f" AND dim.{col} = updates.{col}" + join_sql  
print(join_sql)

insert_columns_sql = ""
select_columns_sql = ""
destination_columns_list = spark.table(f"{catalog}.{schema}.{dimension_name}").columns
source_columns_list = spark.table(f"{catalog}.{schema}.{updates_dimension_table}").columns
destination_columns_lower = [destination_col.lower() for destination_col in destination_columns_list]
destination_columns_lower.remove(f"{dimension_name}_pk")
source_columns_lower = [source_col.lower() for source_col in source_columns_list]
# case sensitive
for destination_column in destination_columns_lower:
    if destination_column in source_columns_lower:
        #print(f"{destination_column} exists in both source and destination")
        insert_columns_sql = insert_columns_sql + f"{destination_column},"
        select_columns_sql = select_columns_sql + f"{destination_column},"
    else:
        #print(f"{destination_column} does not exist in table")
        insert_columns_sql = insert_columns_sql + f"{destination_column},"
        select_columns_sql = select_columns_sql + f"NULL as {destination_column},"
        
# remove the final comma
insert_columns_sql = insert_columns_sql[:-1]
select_columns_sql = select_columns_sql[:-1]
x = f"""
INSERT INTO {catalog}.{schema}.{dimension_name}
({insert_columns_sql})
SELECT {source_key_columns},
    {select_columns_sql} 
FROM {catalog}.{schema}.{updates_dimension_table}     
"""
print(x)

# COMMAND ----------

df = spark.sql(f"""
select 
                                    coalesce(case when max(Sys_Gold_InsertedDateTime_UTC) > max(Sys_Gold_ModifiedDateTime_UTC)
                                    then max(Sys_Gold_InsertedDateTime_UTC)
                                    else max(Sys_Gold_ModifiedDateTime_UTC)
                                    end, CAST('1990-12-31' AS TIMESTAMP)) as delta_timestamp
                                from gold_dev.orion.vendors""")
#df.display()

delta_timestamp = df.first()['delta_timestamp']

print(delta_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gold_dev;
# MAGIC use schema orion;
# MAGIC drop table if exists delta_test;
# MAGIC create table delta_test(Sys_Gold_InsertedDateTime_UTC timestamp, Sys_Gold_ModifiedDateTime_UTCTIMESTAMP timestamp) using delta;
# MAGIC insert into delta_test values('2020-01-01 13:00:00.000',null);
# MAGIC select * from delta_test;
# MAGIC

# COMMAND ----------


delta_timestamp = spark.sql(f"""select 
                                    case when max(coalesce(Sys_Gold_InsertedDateTime_UTC, CAST('1990-12-31' AS TIMESTAMP))) > max(coalesce(Sys_Gold_ModifiedDateTime_UTCTIMESTAMP, CAST('1990-12-31' AS TIMESTAMP)))
                                    then max(coalesce(Sys_Gold_InsertedDateTime_UTC, CAST('1990-12-31' AS TIMESTAMP)))
                                    else max(coalesce(Sys_Gold_ModifiedDateTime_UTCTIMESTAMP, CAST('1990-12-31' AS TIMESTAMP)))
                                    end as delta_timestamp
                                from gold_dev.orion.delta_test
                                """)
x = delta_timestamp.first()['delta_timestamp']
print(x)

df_results = spark.sql(f"select * from gold_dev.orion.delta_test where Sys_Gold_InsertedDateTime_UTC >='{x}'")
df_results.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_dev.obt.datanowarr

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ga.ConsolidationAccountName 
# MAGIC from silver_dev.igsql03.g_l_account ga 
# MAGIC where ga.Sys_Silver_IsCurrent=1 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_dev.igsql03.inf_msp_usage_header h 
# MAGIC -- inner join silver_dev.igsql03.sales_invoice_line l on l.DocumentNo_ = h.No_
# MAGIC -- and l.Sys_DatabaseName = h.Sys_DatabaseName
# MAGIC -- inner join silver_dev.igsql03.dimension_set_entry ds on ds.DimensionSetID = h.DimensionSetID
# MAGIC -- and ds.Sys_DatabaseName = h.Sys_DatabaseName
# MAGIC -- inner join silver_dev.igsql03.dimension_value dv on dv.Sys_DatabaseName = h.Sys_DatabaseName
# MAGIC -- --and dv.Code = l.
# MAGIC --  --where DimensionSetID = '4263556' 
# MAGIC -- where h.No_ = 'PSI-24000125'
# MAGIC -- and ds.Sys_Silver_IsCurrent = 1 
# MAGIC -- and h.Sys_Silver_IsCurrent = 1
# MAGIC -- and l.Sys_Silver_IsCurrent = 1
# MAGIC -- and h.Sys_DatabaseName = 'ReportsBE'
# MAGIC  limit 50;
# MAGIC /*
# MAGIC what are columns processcode, shortcutdimension1code and DimensionSetID used for ?
# MAGIC what are tables dimension_set_entry (is this dimension Group) and dimension_value used for ?
# MAGIC select distinct DimensionCode from silver_dev.igsql03.dimension_value limit 50;
# MAGIC
# MAGIC --select * from silver_dev.igsql03.sales_invoice_line limit (100)
# MAGIC
# MAGIC what is sales_invoice_line type
# MAGIC
# MAGIC ON sih.No_ = sil.DocumentNo_
# MAGIC     AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
# MAGIC     AND sih.Sys_Silver_IsCurrent = true
# MAGIC     AND sil.Sys_Silver_IsCurrent = true
# MAGIC */
# MAGIC --select distinct processcode from silver_dev.igsql03.sales_invoice_header

# COMMAND ----------

# MAGIC %sql
# MAGIC --FACT INSERT
# MAGIC

# COMMAND ----------

# Parallel
from multiprocessing.pool import ThreadPool
pool = ThreadPool(2)
#notebooks = ['dim_1', 'dim_2']
pool.map(lambda path: dbutils.notebook.run("./nb-orion-process-dimension", timeout_seconds= 60, arguments={"dimension_name": "entity"}))

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
parallel_max = 2

# always run source_system first
dbutils.notebook.run(path = "./nb-orion-process-dimension",
                                        timeout_seconds = 600, 
                                        arguments = {"dimension_name":["source_system"]})

# list of dimension names to process in parallel
dim_names = ['currency','entity','entity_group','sales_order','sales_invoice','product','vendor']

def process_dimension(dim_name):
    dbutils.notebook.run(path = "./nb-orion-process-dimension",
                                        timeout_seconds = 600, 
                                        arguments = {"dimension_name":dim_name})
with ThreadPoolExecutor(parallel_max) as executor:
  results = executor.map(process_dimension, dim_names)


# COMMAND ----------

# MAGIC %run ./nb-orion-global

# COMMAND ----------

dimension_names = list(dimensions_dict.keys())
for dim_name in dimension_names:
    if dimensions_dict[dim_name].get('processing_notebook') = 'nb-orion-process-notebook':
        print(dimensions_dict[dim_name].get('processing_notebook'))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from gold_dev.orion.entity_staging;
# MAGIC -- select * from gold_dev.orion.entity_group_staging;
# MAGIC select * from gold_dev.orion.entity_to_entity_group_link;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --reseller
# MAGIC create view if not exists gold_dev.orion.reseller_staging as 
# MAGIC select distinct
# MAGIC     coalesce(cu.No_, 'NaN') AS ResellerCode,
# MAGIC     case
# MAGIC       when cu.Name2 = 'NaN' THEN cu.Name
# MAGIC       ELSE concat_ws(' ', cu.Name, cu.Name2)
# MAGIC     END AS ResellerNameInternal,
# MAGIC     cu.Country_RegionCode AS ResellerGeographyInternal,
# MAGIC     to_date(cu.Createdon) AS ResellerStartDate,
# MAGIC     coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
# MAGIC     coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
# MAGIC     to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate
# MAGIC FROM
# MAGIC     silver_dev.igsql03.sales_invoice_header sih
# MAGIC LEFT JOIN silver_dev.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
# MAGIC     AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
# MAGIC     AND cu.Sys_Silver_IsCurrent = true
# MAGIC LEFT JOIN gold_dev.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
# MAGIC LEFT JOIN silver_dev.masterdata.resellergroups AS rg
# MAGIC     ON rg.ResellerID = cu.No_
# MAGIC     AND rg.Entity = UPPER(entity.TagetikEntityCode)
# MAGIC     AND rg.Sys_Silver_IsCurrent = true
# MAGIC WHERE sih.Sys_Silver_IsCurrent = true
# MAGIC union
# MAGIC select distinct
# MAGIC   coalesce(cu.No_, 'NaN') AS ResellerCode,
# MAGIC   case
# MAGIC       when cu.Name2 = 'NaN' THEN cu.Name
# MAGIC       ELSE concat_ws(' ', cu.Name, cu.Name2)
# MAGIC     END AS ResellerNameInternal,
# MAGIC     cu.Country_RegionCode AS ResellerGeographyInternal,
# MAGIC     to_date(cu.Createdon) AS ResellerStartDate,
# MAGIC     coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
# MAGIC     coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
# MAGIC     to_date('1900-01-01', 'yyyy-MM-dd') AS ResellerGroupStartDate
# MAGIC FROM
# MAGIC     silver_dev.igsql03.sales_cr_memo_header sih
# MAGIC    LEFT JOIN silver_dev.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
# MAGIC     AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
# MAGIC     AND cu.Sys_Silver_IsCurrent = true
# MAGIC     LEFT JOIN gold_dev.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
# MAGIC     LEFT JOIN silver_dev.masterdata.resellergroups AS rg
# MAGIC     ON rg.ResellerID = cu.No_
# MAGIC     AND rg.Entity = UPPER(entity.TagetikEntityCode)
# MAGIC     AND rg.Sys_Silver_IsCurrent = true
# MAGIC WHERE sih.Sys_Silver_IsCurrent = true
# MAGIC
# MAGIC     
