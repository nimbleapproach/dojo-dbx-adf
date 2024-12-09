# Databricks notebook source
# MAGIC %run ./nb-orion-meta

# COMMAND ----------
# 
# Notebook to process a single dimension based on the name being passed via widgets
#
from datetime import datetime
dbutils.widgets.text("dimension_name", "", "Dimension Name")
dimension_name = dbutils.widgets.get("dimension_name")
#DEBUG dimension_name = 'entity_to_entity_group_link'
#dimension_name = 'reseller'
#dimension_name = 'reseller_group'

# needed for ending a dimension = end_datetime
run_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------
spark = spark  # noqa

# file_path = 'meta.json'
# replacements = {
#     "processing_notebook": processing_notebook,
#     "ENVIRONMENT": ENVIRONMENT,
#     "orion_schema": orion_schema
# }
# data = read_and_replace_json(file_path, replacements)

#source system table name needed for joinging in a cross join to work out the MAX Delta timestamp
# for each source system
source_system_config = get_object_detail(data, 'source_system')
source_system_table_name = source_system_config['destination_table_name'] # string
   
# COMMAND ----------

def merge_dimension(dimension_name):

    #print(f"processing {dimension_name}")

    #grab the config from the dictionary
    dimension_config = get_object_detail(data, dimension_name)
    # destination config
    destination_table_name_only = dimension_config['destination_table_name'].split('.')[-1] # string
    destination_key_columns = dimension_config['destination_key_columns'] # string
    destination_dimension_table_name = dimension_config['destination_table_name'] # string
    
    # Get the last part of the string after the period, then split at the first underscore
    table_pk = destination_table_name_only.split('.')[-1].split('_', 1)[1] + "_pk"

    # source of data config
    updates_dimension_table = dimension_config['source_table_name'] # string
    source_key_columns = dimension_config['source_key_columns'] # string
    join_columns = list(set(destination_key_columns) & set(source_key_columns))

    join_sql = ""
    for col in join_columns:
        join_sql = f" AND dim.{col} = updates.{col}" + join_sql  

    # need a column list here from the dictionary + standard dim columns
    # this is where we insert the entire updates table  The updates table contains only
    # the new and changed dim records i.e. the delta
    # problem might be if the column order is different to that of the lists
    # ENSURE lists of columns are in the same order as the table - problem sorted
    #
    # get a list of column names for the insert and a list of column names for the select
    # put in a NULL in the select part if the destination column is not present in the source
    # build the insert_columns_sql and select_columns_sql strings

    insert_columns_sql = ""
    select_columns_sql = ""
    destination_columns_list = spark.table(f"{destination_dimension_table_name}").columns
    source_columns_list = spark.table(f"{updates_dimension_table}").columns
    destination_columns_lower = [destination_col.lower() for destination_col in destination_columns_list]
    destination_columns_lower.remove(table_pk)
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

    # in order to grab a correct delta per source system, we might need to pull in the keys and a MAX(timestamp)
    # grouping by the source_system and non dimension name column 
    # eg line_item_type and source_system_fk , document_source and source_system_fk
    # that way we can run selects from the dims joining to this tempo view of delta timestamp per source system/key combo
    # 

    # grab a timestamp of the data from the destination dimension
    # this is where we only pull through stuff that has changed from 
    # into destination_dimension_table_name
    # can only do this on dims that have a source_system_fk 
    # NEED TO EXCLUDE N/A FROM THE UTC UPDATES IN merge statement
    if 'source_system_fk' in source_key_columns:
        # we can do a temp view join to get the max timestamp per source system
        spark.sql(f"""create temporary view vw_dim_delta_timestamps as 
                                select src.source_system_pk as delta_source_system_fk,
                                    coalesce(case when max(d.Sys_Gold_InsertedDateTime_UTC) > max(d.Sys_Gold_ModifiedDateTime_UTC)
                                    then max(d.Sys_Gold_InsertedDateTime_UTC)
                                    else max(d.Sys_Gold_ModifiedDateTime_UTC)
                                    end, CAST('1990-12-31' AS TIMESTAMP)) as delta_timestamp
                                from {destination_dimension_table_name} d
                                cross join {source_system_table_name} src
                                group by src.source_system_pk
                                """)
        # the updates_dimension_table will contain all the changed and new records for a dim slice it by source_system_fk, 
        updates_df = spark.sql(f"""SELECT 
                            {'updates.'+', updates.'.join(source_key_columns)},
                            NOW() AS Sys_Gold_ModifiedDateTime_UTC, 
                            MIN(start_datetime) AS min_start_datetime ,
                            dateadd(second,-1,CAST('{run_date_time}' AS TIMESTAMP)) AS existing_dim_end_datetime
                            FROM {updates_dimension_table} updates
                            INNER JOIN vw_dim_delta_timestamps ds on ds.delta_source_system_fk = updates.source_system_fk
                            WHERE Sys_Gold_InsertedDateTime_UTC > ds.delta_timestamp 
                            OR coalesce(Sys_Gold_ModifiedDateTime_UTC, CAST('1900-01-01' AS TIMESTAMP)) > ds.delta_timestamp
                            GROUP BY {'updates.'+', updates.'.join(source_key_columns)}""")
    else:
        # grab a timestamp of the data from the destination dimension
        # this is where we only pull through stuff that has changed from 
        # into destination_dimension_table_name
        delta_df = spark.sql(f"""select 
                                        coalesce(case when max(Sys_Gold_InsertedDateTime_UTC) > max(Sys_Gold_ModifiedDateTime_UTC)
                                        then max(Sys_Gold_InsertedDateTime_UTC)
                                        else max(Sys_Gold_ModifiedDateTime_UTC)
                                        end, CAST('1990-12-31' AS TIMESTAMP)) as delta_timestamp
                                    from {destination_dimension_table_name}
                                    """)
        # pull into a string for extracting the delta
        delta_timestamp = delta_df.first()['delta_timestamp']
        #print('delta_timestamp',delta_timestamp)

        # the updates_dimension_table will contain all the changed and new records for a dim, not the entire dim
        # The first part will identify the updates
        updates_df = spark.sql(f"""SELECT 
                            {', '.join(source_key_columns)},
                            NOW() AS Sys_Gold_ModifiedDateTime_UTC, 
                            MIN(start_datetime) AS min_start_datetime  ,
                            dateadd(second,-1,CAST('{run_date_time}' AS TIMESTAMP)) AS existing_dim_end_datetime
                            FROM {updates_dimension_table} 
                            WHERE Sys_Gold_InsertedDateTime_UTC > '{delta_timestamp}' 
                            OR coalesce(Sys_Gold_ModifiedDateTime_UTC, CAST('1990-12-31' AS TIMESTAMP)) > '{delta_timestamp}' 
                            GROUP BY {', '.join(source_key_columns)}""")
        
    #updates_df.show()

    deltaTableAccount = DeltaTable.forName(spark, f"{destination_dimension_table_name}")

    # this is the SCD2 logic for the F&D dim table to reset the existing back to is_current = 0 and enddate it 
    # the insert below will always insert latest records both new and updates of a previous dim member
    deltaTableAccount.alias('dim') \
    .merge(
        updates_df.alias('updates'),
        f"""dim.is_current = 1 {join_sql}"""
    ) \
    .whenMatchedUpdate(set =
        {
        "dim.end_datetime": "updates.existing_dim_end_datetime",
        "dim.is_current": lit(0),
        "dim.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
        }
    ) \
    .execute()

    # now insert the delta as new records
    if 'source_system_fk' in source_key_columns:
        insert_sql = f"""INSERT INTO {destination_dimension_table_name}
                ({insert_columns_sql})
            SELECT 
                {select_columns_sql} 
            FROM {updates_dimension_table} d
            INNER JOIN vw_dim_delta_timestamps ds on ds.delta_source_system_fk = d.source_system_fk
            WHERE d.Sys_Gold_InsertedDateTime_UTC > ds.delta_timestamp
            OR coalesce(d.Sys_Gold_ModifiedDateTime_UTC, CAST('1990-12-31' AS TIMESTAMP)) > ds.delta_timestamp """
    else:
        insert_sql = f"""INSERT INTO {destination_dimension_table_name}
                ({insert_columns_sql})
            SELECT 
                {select_columns_sql} 
            FROM {updates_dimension_table} 
            WHERE Sys_Gold_InsertedDateTime_UTC > '{delta_timestamp}'
            OR coalesce(Sys_Gold_ModifiedDateTime_UTC, CAST('1990-12-31' AS TIMESTAMP)) > '{delta_timestamp}' """
    #print(insert_sql)

    sqldf= spark.sql(insert_sql)



# COMMAND ----------

# run the function
merge_dimension(dimension_name)
dbutils.notebook.exit(0)

