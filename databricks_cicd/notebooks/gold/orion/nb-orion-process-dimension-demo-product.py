# Databricks notebook source
# MAGIC %run ./nb-orion-global

# COMMAND ----------

# 
# Notebook to process a single dimension based on the name being passed via widgets
#
dbutils.widgets.text("dimension_name", "", "Dimension Name")
dimension_name = dbutils.widgets.get("dimension_name")
#DEBUG 
dimension_name = 'product'



# COMMAND ----------

def merge_dimension(dimension_name):

    #print(f"processing {dimension_name}")

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
                           MIN(start_datetime) AS min_start_datetime 
                           FROM {updates_dimension_table} 
                           WHERE Sys_Gold_InsertedDateTime_UTC > '{delta_timestamp}' 
                           OR coalesce(Sys_Gold_ModifiedDateTime_UTC, CAST('1990-12-31' AS TIMESTAMP)) > '{delta_timestamp}' 
                           GROUP BY {', '.join(source_key_columns)}""")

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
        "dim.end_datetime": "updates.min_start_datetime",
        "dim.is_current": lit(0),
        "dim.Sys_Gold_ModifiedDateTime_UTC": "updates.Sys_Gold_ModifiedDateTime_UTC",
        }
    ) \
    .execute()

    # now insert the delta as new records
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
