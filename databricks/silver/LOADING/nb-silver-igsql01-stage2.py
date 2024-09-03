# Databricks notebook source
# MAGIC %run ../../library/nb-enable-imports

# COMMAND ----------

from datetime import datetime, timezone
import os

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

from library.silver_loading import (
    get_current_silver_rows,
    get_select_from_column_map,
)
from silver.LOADING import silver_igsql01_loading as sil

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
CATALOG = f"silver_{ENVIRONMENT}"
SCHEMA = "igsql01"

print(CATALOG, SCHEMA)

spark.catalog.setCurrentCatalog(CATALOG)
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# constants for reuse

# source tables
ACCOUNTBASE_SOURCE_TABLE = "accountbase"
RESELLER_GROUP_SOURCE_TABLE = "inf_keyaccountbase"

# target tables
RESELLER_TABLE = "tbl_reseller"
RESELLER_GROUP_TABLE = "tbl_reseller_group"

# COMMAND ----------

# load the tbl_account table by using simple mapping from the accountbase table

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
    "created_by" : "CreatedBy",
    "modified_on" : "ModifiedOn",
    "modified_by" : "ModifiedBy",
    "entity_fk" : "OwningBusinessUnit",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent"
}

tbl_acc = spark.table(ACCOUNTBASE_SOURCE_TABLE).select(get_select_from_column_map(account_column_map))
tbl_acc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_account")

# COMMAND ----------

# load the tbl_country table by using simple mapping from inf_countrybase

country_column_map = {
    "country_pk" : "SID",
    "country_id" : "Inf_countryId",
    "country" : "Inf_Description",
    "country_code" : None,
    "country_iso" : "Inf_name",
    "created_on" : "CreatedOn",
    "created_by" : "CreatedBy",
    "modified_on" : "ModifiedOn",
    "modified_by" : "ModifiedBy",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent",
}

tbl_ctry = spark.table("inf_countrybase").select(get_select_from_column_map(country_column_map))
tbl_ctry.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_country")

# COMMAND ----------

# load the tbl_entity table by using simple mapping from businessunitbase

entity_column_map = {
    "entity_pk" : "SID",
    "entity_id" : "BusinessUnitId",
    "entity_code" : "Name",
    "entity_description" : None,
    "created_on" : "CreatedOn",
    "created_by" : "CreatedBy",
    "modified_on" : "ModifiedOn",
    "modified_by" : "ModifiedBy",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
    "sys_silver_insert_date_time_utc" : "Sys_Silver_InsertDateTime_UTC",
    "sys_silver_modified_date_time_utc" : "Sys_Silver_ModifedDateTime_UTC",
    "sys_silver_hash_key" : "Sys_Silver_HashKey",
    "sys_silver_is_current" : "Sys_Silver_IsCurrent",
}

tbl_entity = spark.table("businessunitbase").select(get_select_from_column_map(entity_column_map))
tbl_entity.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tbl_entity")

# COMMAND ----------

# load tbl_reseller

# get source data from accountbase
reseller_source_raw = sil.get_reseller_source(spark.table(ACCOUNTBASE_SOURCE_TABLE))

# remap columns
reseller_column_map = {
    "reseller_id" : "AccountId",
    "reseller" : "Name",
    "reseller_code" : "inf_customerno",
    "address_line_1" : None,
    "address_line_2" : None,
    "city" : None,
    "country_fk" : "Inf_CountryId",
    "created_on" : "CreatedOn",
    "created_by" : "CreatedBy",
    "modified_on" : "ModifiedOn",
    "modified_by" : "ModifiedBy",
    "entity_fk" : "OwningBusinessUnit",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
}
additional_columns = [col("all_names")]
select_arg = get_select_from_column_map(reseller_column_map) + additional_columns
reseller_source_mapped = reseller_source_raw.select(select_arg)

# add sys fields
reseller_business_keys = ["reseller_code", "sys_database_name"]
reseller_updatable = [k for k in reseller_column_map.keys() if k not in reseller_business_keys]
current_timestamp = datetime.now(timezone.utc)
reseller_source = sil.add_sys_silver_columns(reseller_source_mapped, reseller_updatable, current_timestamp)

# merge into delta table
dt_reseller = DeltaTable.forName(spark, tableOrViewName=f"{CATALOG}.{SCHEMA}.{RESELLER_TABLE}")
sil.merge_into_stage2_table(dt_reseller, reseller_source, reseller_business_keys)

# COMMAND ----------

# load tbl_reseller_group

# get source data from accountbase
group_source_raw = sil.get_reseller_group_source(spark.table(RESELLER_GROUP_SOURCE_TABLE))

# remap columns
reseller_group_column_map = {
    "reseller_group_id" : "inf_keyaccountId",
    "reseller_group" : None,
    "reseller_group_code" : "inf_name",
    "created_on" : "CreatedOn",
    "created_by" : "CreatedBy",
    "modified_on" : "ModifiedOn",
    "modified_by" : "ModifiedBy",
    "sys_bronze_insert_date_time_utc" : "Sys_Bronze_InsertDateTime_UTC",
    "sys_database_name" : "Sys_DatabaseName",
}
group_source_mapped = group_source_raw.select(get_select_from_column_map(reseller_group_column_map))

reseller_group_business_keys = ["reseller_group_code", "sys_database_name"]
reseller_group_updatable = [k for k in reseller_group_column_map.keys() 
                            if k not in reseller_group_business_keys]

current_timestamp = datetime.now(timezone.utc)
reseller_group_source = sil.add_sys_silver_columns(group_source_mapped, reseller_group_updatable, current_timestamp)

dt_reseller_group = DeltaTable.forName(spark, tableOrViewName=f"{CATALOG}.{SCHEMA}.{RESELLER_GROUP_TABLE}")
sil.merge_into_stage2_table(dt_reseller_group, reseller_group_source, reseller_group_business_keys)

# COMMAND ----------

# load tbl_reseller_group_link

# find the latest links grouping resellers within acountbase 
# and collect required attributes 
grouped_resellers_raw = sil.get_grouped_reseller_source(spark.table(ACCOUNTBASE_SOURCE_TABLE))
grouped_resellers = (grouped_resellers_raw
                        .select(col("AccountId"),
                                col("inf_customerno"),
                                col("inf_KeyAccount").alias("group_id"),
                                col("Sys_Bronze_InsertDateTime_UTC"),
                                col("Sys_DatabaseName"))
)
# select source ids for linking groups
current_groups = get_current_silver_rows(spark.table(RESELLER_GROUP_SOURCE_TABLE))
groups = current_groups.select(col("inf_keyaccountId").alias("group_id"),
                               col("Sys_DatabaseName"),
                               col("inf_name"))

# link the two and leave source business key of the account for tracking
link = (
    grouped_resellers.join(groups,
                           (grouped_resellers.group_id == groups.group_id)
                           & (grouped_resellers.Sys_DatabaseName == groups.Sys_DatabaseName)
                           )
                     .drop("group_id")
                     .drop(groups.Sys_DatabaseName)
)

# link to stage 2 tables using primary keys and get PKs to link
target_resellers = spark.table(RESELLER_TABLE)
target_groups = spark.table(RESELLER_GROUP_TABLE)

reseller_group_link_source = (
    link.join(
            target_resellers,
            (link.inf_customerno == target_resellers.reseller_code)
             & (link.Sys_DatabaseName == target_resellers.sys_database_name)
             & target_resellers.sys_silver_is_current
        ).join(
            target_groups,
            (link.inf_name == target_groups.reseller_group_code)
             & (link.Sys_DatabaseName == target_groups.sys_database_name)
             & target_groups.sys_silver_is_current
        ).select(
            # mapping of target columns:
            col("reseller_pk").alias("reseller_fk"),
            col("reseller_group_pk").alias("reseller_group_fk"),
            col("Sys_Bronze_InsertDateTime_UTC").alias("sys_bronze_insert_date_time_utc"),
            col("Sys_DatabaseName").alias("sys_database_name"),
            col("AccountId").alias("link_source_account_id")
        )
)

reseller_group_link_business_keys = ["reseller_fk", "reseller_group_fk"]
reseller_group_link_updatable = ["sys_bronze_insert_date_time_utc", "sys_database_name", "link_source_account_id"]
current_timestamp = datetime.now(timezone.utc)
reseller_group_link_source = sil.add_sys_silver_columns(reseller_group_link_source,
                                                        reseller_group_link_updatable,
                                                        current_timestamp)

dt_reseller_group_link = DeltaTable.forName(spark, tableOrViewName=f"{CATALOG}.{SCHEMA}.tbl_reseller_group_link")
sil.merge_into_stage2_table(dt_reseller_group_link, reseller_group_link_source, reseller_group_link_business_keys)
