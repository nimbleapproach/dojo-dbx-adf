# Databricks notebook source
# MAGIC %md
# MAGIC ##### Notebook to produce Basic DQ metrics on a list of NUVIAS CRM tables

# COMMAND ----------

# MAGIC %run ../library/nb-dq-bronze-basic

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

try:
    EXPLORE_BRONZE = bool(dbutils.widgets.get("wg_explorebronze") == 'True')
except:
    dbutils.widgets.dropdown(name = "wg_explorebronze", defaultValue = 'False', choices =  ['False','True'])
    EXPLORE_BRONZE = bool(dbutils.widgets.get("wg_explorebronze") == 'True')


# COMMAND ----------

database_name = "nuav_prod_sqlbyod"
tables = spark.catalog.listTables(database_name)

# explore a database for all the tables that have been recently reloaded.
if EXPLORE_BRONZE:
    for table in tables:
        table_name = table.name
        df = spark.table(f"{database_name}.{table_name}")
        df = df.where("Sys_Bronze_InsertDateTime_UTC>'2024-07-01'")
        if df.count() > 0:
            print(f"Recently Loaded at....{database_name}.{table_name}")
else:
    print('skip')

# COMMAND ----------

# Define the source tables you wish to perform Basic DQ Chesk againsts
data = [
    ("nuav_prod_sqlbyod.ara_dim_customer", "CustomerID", ["CustomerName"]),
    (
        "nuav_prod_sqlbyod.dbo_purchpurchaseorderlinestaging",
        "ITEMNUMBER",
        ["PURCHASEREBATEVENDORGROUPID"],
    ),
    (
        "nuav_prod_sqlbyod.dbo_vendvendorgroupstaging",
        "VENDORGROUPID",
        ["VENDORGROUPID", "DESCRIPTION", "DATAAREAID"],
    ),
    (
        "nuav_prod_sqlbyod.dbo_vendvendorv2staging",
        "VENDORACCOUNTNUMBER",
        ["VENDORACCOUNTNUMBER", "VENDORGROUPID", "DATAAREAID"],
    ),
    (
        "nuav_prod_sqlbyod.pbi_customer",
        "CUSTOMERID",
        ["CUSTOMERNAME", "LASTTRANSACTIONDATE", "CUSTOMERSTAGE", "ENTITY"],
    ),
    (
        "nuav_prodtrans_sqlbyod.dbo_vendvendorv2staging",
        "VENDORACCOUNTNUMBER",
        ["VENDORGROUPID", "DATAAREAID"],
    ),
    (
        "nuav_prodtrans_sqlbyod.ora_account",
        "PartyID",
        ["PartyNumber","OrganizationName","PartyUniqueName","Type","DUNSNumber","LastUpdateDate","LastUpdatedBy"],
    ),
    (
        "nuav_prodtrans_sqlbyod.crm_account",
        "ID",
        ["PartyNumber","OrganizationName","PartyUniqueName","Type"],
    ),
    (
        "nuav_prodtrans_sqlbyod.ora_Oracle_Accounts",
        "RegistryID",
         ["Sales Account Id","Owner","Organization Name","Registry ID","Account Type","Trading Entity","Account Vendor Reseller Id","Account Vendor Levels","Last Update","Last_Update_Date","Credit","Address","Address_Line_2","Address_Line_3","Address_Line_4","Postal_Code","City","County","Country_Code","Primary_Contact_Name","Created_By","Creation_Date","Last_Updated_By","SHIPPING_Methode","Ship_Complete","Quote_Message","Reg_ID","Account_Name","Parent_Account","Partner_Type","Assigned_Level","Assignment_Date","Customer_Level","Mid_Market","Customer_Stage","Assigned_Engagement_Level","Engagement_Level","Frequency","Partner_Tenure","Transaction_Volume","Vendor_Count","Credit_Group","International_Account"],
    ),
]

# Create a DataFrame
df_dq_list = spark.createDataFrame(
    data, ["full_table_name", "key_column", "attribute_columns"]
)

# COMMAND ----------


TARGET_TABLE_NAME = "bronze_dq"
TARGET_TABLE_SCHEMA = "dq"
TARGET = f"silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"
RECREATE = True
RECREATE_ALL = False

if RECREATE_ALL:
    # Verify that the table exists
    if spark.catalog.tableExists(TARGET):
        for row in df_dq_list.select("full_table_name").distinct().collect():
            # remove exists stats held within the bronze_dq table for the tables that are about to be processed
            table_name = row["full_table_name"]
            spark.sql(
                f"delete from {TARGET} where COALESCE(schema ,'.',table) ='{table_name}'"
            )


for row in df_dq_list.collect():
    # create paramaters that are being passed into the dq_load_bronze_stats function
    table_name = row["full_table_name"]
    key = row["key_column"]
    attributes = row["attribute_columns"]
    print(f"dq_load_bronze_stats... {table_name} key {key} attributes {attributes}")
    # execute the function to load the stats for the source table
    dq_load_bronze_stats(src_full_table_name=table_name, column_id=key, column_attr=attributes, recreate=RECREATE)

# COMMAND ----------

# Read the table into a DataFrame
df = spark.table(TARGET)

# Identify and keep the rows with the maximum `as_of` value for each combination of `schema`, `table`, and `col_name`
from pyspark.sql import functions as F
from pyspark.sql.window import Window
window_spec = Window.partitionBy("schema", "table", "col_name").orderBy(F.desc("as_of"))
df_cleaned_dupes = df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") > 1)
#check for dupes
display(df_cleaned_dupes)

if df_cleaned_dupes.count()>1:
# drop if dupes
    df_cleaned = df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1).drop("row_number")
    df_cleaned.write.mode("overwrite").saveAsTable(TARGET)


