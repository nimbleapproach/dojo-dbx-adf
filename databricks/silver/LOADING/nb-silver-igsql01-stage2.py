# Databricks notebook source
# MAGIC %run ../../library/nb-enable-imports

# COMMAND ----------

import os

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

from library.silver_loading import get_select_from_column_map

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
CATALOG = f"silver_{ENVIRONMENT}"
SCHEMA = 'igsql01'

print(CATALOG, SCHEMA)

# COMMAND ----------

spark.catalog.setCurrentCatalog(CATALOG)
spark.sql(f'USE SCHEMA {SCHEMA}')

# COMMAND ----------

# source table
ACCOUNTBASE_TABLE = "accountbase"

# target table
ACCOUNT_TABLE = "tbl_account"

account_column_map = {
    "account_pk" : "SID",
    "account_id" : "AccountId",
    "account_number" : "AccountNumber",
    "account_extended_additional" : None,
    "name" : "Name",
    "description" : "inf_Name2",
    "country_fk" : "Inf_CountryId",
    "fax" : "Fax",
    "telephone" : "Telephone1",
    "parent_account_fk" : "ParentAccountId",
    "created_on" : "CreatedOn",
    "created_by" : None,
    "modified_on" : "ModifiedOn",
    "modified_by" : None,
    "entity_fk" : "OwningBusinessUnit",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent"
}

# COMMAND ----------

tbl_acc = spark.table(ACCOUNTBASE_TABLE).select(get_select_from_column_map(account_column_map))
tbl_acc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ACCOUNT_TABLE)

# COMMAND ----------

COUNTRY_TABLE = "tbl_country"

country_column_map = {
    "country_pk" : "SID",
    "country_id" : "Inf_countryId",
    "country" : "Inf_Description",
    "country_code" : None,
    "country_iso" : "Inf_name",
    "created_on" : "CreatedOn",
    "created_by" : None,
    "modified_on" : "ModifiedOn",
    "modified_by" : None,
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent",
}

# COMMAND ----------

tbl_ctry = spark.table('inf_countrybase').select(get_select_from_column_map(country_column_map))
tbl_ctry.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(COUNTRY_TABLE)

# COMMAND ----------

ENTITY_TABLE = "tbl_entity"

entity_column_map = {
    "entity_pk" : "SID",
    "entity_id" : "BusinessUnitId",
    "entity_code" : "Name",
    "entity_description" : None,
    "created_on" : "CreatedOn",
    "created_by" : None,
    "modified_on" : "ModifiedOn",
    "modified_by" : None,
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent",
}

# COMMAND ----------

tbl_entity = spark.table('businessunitbase').select(get_select_from_column_map(entity_column_map))
tbl_entity.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ENTITY_TABLE)

# COMMAND ----------

RESELLER_TABLE = "tbl_reseller"

reseller_column_map = {
    "reseller_pk" : "SID",
    "reseller_id" : "AccountId",
    "reseller" : "Name",
    "reseller_code" : "inf_customerno",
    "address_line_1" : None,
    "address_line_2" : None,
    "city" : None,
    "country_fk" : "Inf_CountryId",
    "created_on" : "CreatedOn",
    "created_by" : None,
    "modified_on" : "ModifiedOn",
    "modified_by" : None,
    "entity_fk" : "OwningBusinessUnit",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent",
}

# COMMAND ----------

NULL = lit('NaN')
CUSTOMER_ID_COL = col('inf_customerno')

has_infinigate_customer_code = CUSTOMER_ID_COL != NULL

tbl_reseller = (
    spark.table(ACCOUNTBASE_TABLE)
         .select(get_select_from_column_map(reseller_column_map))
         .where(has_infinigate_customer_code)
)

tbl_reseller.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RESELLER_TABLE)

# COMMAND ----------

RESELLER_GROUP_LINK_TABLE = 'tbl_reseller_group_link'

# COMMAND ----------

resellers = (
    spark.table(ACCOUNTBASE_TABLE)
         .where(has_infinigate_customer_code)
)
parents = spark.table(ACCOUNTBASE_TABLE)
grandparents = spark.table(ACCOUNTBASE_TABLE)

reseller_group_link = (
    resellers.join(parents, 
                   (resellers.ParentAccountId == parents.AccountId) 
                    & (resellers.Sys_DatabaseName == parents.Sys_DatabaseName),
                   how="inner" )
             .join(grandparents,
                   (parents.ParentAccountId == grandparents.AccountId)
                   & (parents.Sys_DatabaseName == grandparents.Sys_DatabaseName),
                   how='left')
             .select(
                 resellers.SID.alias('reseller_fk'),
                 F.ifnull(grandparents.SID, parents.SID).alias('reseller_group_fk')
             )
)

reseller_group_link.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RESELLER_GROUP_LINK_TABLE)

# COMMAND ----------

RESELLER_GROUP_TABLE = 'tbl_reseller_group'

reseller_group_column_map = {
    "reseller_group_pk" : "SID",
    "reseller_group_id" : "AccountId",
    "reseller_group" : "Name",
    "reseller_group_code" : "inf_customerno",
    "created_on" : "CreatedOn",
    "created_by" : None,
    "modified_on" : "ModifiedOn",
    "modified_by" : None,
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent"
}

# COMMAND ----------

groupbase = spark.table(ACCOUNTBASE_TABLE).select(get_select_from_column_map(reseller_group_column_map))
tbl_reseller_group = (
    # filter to only those found in hierarchy of accounts
    groupbase.join(reseller_group_link,
                   groupbase.reseller_group_pk == reseller_group_link.reseller_group_fk,
                   how='left_semi')
)

tbl_reseller_group.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RESELLER_GROUP_TABLE)
