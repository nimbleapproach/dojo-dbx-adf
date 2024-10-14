# Databricks notebook source
# 
# Notebook to setup common variables and functions used by other notebooks
# which are ORION COMMON ONLY - such as a dict of dimension names and tables
# dict of fact names and tables etc.
#
import os
from pyspark.sql.functions import levenshtein, regexp_extract, col, row_number, lit
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing.pool import ThreadPool
from pyspark.sql import Window
from delta.tables import DeltaTable

# set up some constants
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
catalog = f"gold_{ENVIRONMENT}"
orion_schema = "orion"
# this is number of notebooks to run in parallel
parallel_max = 3
processing_notebook = "nb-orion-process-dimension"

# this should be an orion global function to return the dimension name and the orion table name as a dict
# and the fact tables and names
dimensions_dict = {
    "source_system" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_source_system", 
        "destination_key_columns" : ["source_system"], 
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_source_system_staging" , 
        "source_key_columns" : ["source_system"]
                },
    
# comment out because it takes too long to process when testing - but works
    "product" : {
        #"processing_notebook" : f"{processing_notebook}",
        "processing_notebook" : f"nb-orion-process-dimension-demo-product",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_product", 
        "destination_key_columns" : ["product_code", "source_system_id"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_product_staging" , 
        "source_key_columns" : ["product_code", "source_system_id"]
                 },

    "currency" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_currency",  
        "destination_key_columns" : ["currency_code"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_currency_staging" ,  
        "source_key_columns" : ["currency_code"]
                 },
    
    "entity" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_entity",  
        "destination_key_columns" : ["entity_code"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_entity_staging" ,  
        "source_key_columns" : ["entity_code"]
                 },
    
    "entity_group" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_entity_group",  
        "destination_key_columns" : ["entity_group_code"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_entity_group_staging" ,  
        "source_key_columns" : ["entity_group_code"]
                 },
    
    "sales_document" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_sales_document",  
        "destination_key_columns" : ["document_id", "source_system_id"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_sales_document_staging" ,  
        "source_key_columns" : ["document_id", "source_system_id"]
                 },
    
    # "document_type" : {
    #     "processing_notebook" : f"{processing_notebook}",
    #     "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.document_type",  
    #     "destination_key_columns" : ["document_type"],
    #     "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.document_type_staging" ,  
    #     "source_key_columns" : ["document_type"]
    #              },
    
    "region" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_region",  
        "destination_key_columns" : ["region_code"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_region_staging" ,  
        "source_key_columns" : ["region_code"]
                 },
    
    "entity_to_entity_group_link" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.link_entity_to_entity_group",  
        "destination_key_columns" : ["entity_group_pk","entity_pk"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_link_entity_to_entity_group_staging" ,  
        "source_key_columns" : ["entity_group_pk","entity_pk"]
                 },
    
    "vendor" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_vendor", 
        "destination_key_columns" : ["vendor_code", "source_system_id"], 
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_vendor_staging" , 
        "source_key_columns" : ["vendor_code", "source_system_id"]
                },

    "reseller" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_reseller", 
        "destination_key_columns" : ["reseller_code", "source_system_id"], 
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_reseller_staging" , 
        "source_key_columns" : ["reseller_code", "source_system_id"]
                },
    
    "reseller_group" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_reseller_group", 
        "destination_key_columns" : ["reseller_group_code"], 
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.vw_dim_reseller_group_staging" , 
        "source_key_columns" : ["reseller_group_code"]
                }
    

}

common_dimension_columns = [
    "start_datetime", 
    "end_datetime", 
    "is_current", 
    "Sys_Gold_InsertedDateTime_UTC", 
    "Sys_Gold_ModifiedDateTime_UTC"
    ]

# COMMAND ----------


# this should be an orion global function to return the dimension name and the orion table name as a dict
# and the fact tables and names
facts_dict = {
    "source_system" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_source_system", 
        "destination_key_columns" : ["source_system"], 
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.dim_source_system_staging" , 
        "source_key_columns" : ["source_system"]
                },
    
# comment out because it takes too long to process when testing - but works
    "product" : {
        "processing_notebook" : f"{processing_notebook}",
        "destination_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.fact_transaction", 
        "destination_key_columns" : ["product_code", "source_system_id"],
        "source_table_name" : f"gold_{ENVIRONMENT}.{orion_schema}.product_staging" , 
        "source_key_columns" : ["product_code", "source_system_id"]
                 },


}

# COMMAND ----------

def match_dimensions(df_data_to_match, df_shared_dimension_data):
    # stub function to run this process locally in the notebooks to simulate the matching process until it is written
    # each dataframe should have an "id" column and a "name" column - strict rules

    output_df = df_shared_dimension_data.join(df_data_to_match, df_data_to_match.name == df_shared_dimension_data.name, how="inner").drop(df_data_to_match.id)
    return output_df


