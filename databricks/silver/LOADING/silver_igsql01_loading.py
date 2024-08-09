from delta.tables import DeltaTable
from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from typing import List


from library.silver_loading import (
    get_current_silver_rows,
    get_insert_update_maps_from_columns,
    get_merge_condition_expr,
)


# nulls in strings are replaced but bronze_to_silver
# with the special value
NAN = "NaN"

# target silver stage2 sys field names
SYS_CURRENT_COL = "sys_silver_is_current"
SYS_HASH_COL = "sys_silver_hash_key"
SYS_INSERT_COL = "sys_silver_insert_date_time_utc"
SYS_MODIFY_COL = "sys_silver_modified_date_time_utc"

# source column names
SRC_DB_COL = "Sys_DatabaseName"
SRC_MODIFIED_COL = "ModifiedOn"
# in accountbase table only
CUSTOMER_ID_COL = "inf_customerno"
ACCOUNT_ID_COL = "AccountId"


def add_sys_silver_columns(df, hash_columns: List[str], current_datetime):
    current_timestamp = lit(current_datetime).cast("TIMESTAMP")
    return (
        df.withColumn(SYS_INSERT_COL, current_timestamp)
          .withColumn(SYS_MODIFY_COL, current_timestamp)
          .withColumn(SYS_HASH_COL, F.xxhash64(*hash_columns))
          .withColumn(SYS_CURRENT_COL, lit(True))
    )


def get_customers_from_accounts(accounts):
    return accounts.where(col(CUSTOMER_ID_COL) != lit(NAN))


def get_accounts_with_group(accounts):
    # column of the accountbase table that can contain a link to group
    GROUP_CODE_COL = col("inf_KeyAccount")
    return accounts.where(GROUP_CODE_COL != lit(NAN))


def collect_best_customer(customers_df):
    # window spec to find most recently modified customer
    # with second level of sort to add determinism (in case of equal timestamps)
    best_reseller_win = (Window.partitionBy(col(CUSTOMER_ID_COL), col(SRC_DB_COL))
                               .orderBy(col(SRC_MODIFIED_COL).desc(), col(ACCOUNT_ID_COL)))
    # win spec to collect all names
    unsorted_reseller_win = Window.partitionBy(col(CUSTOMER_ID_COL), col(SRC_DB_COL))

    return (
        customers_df.withColumn("_row", F.row_number().over(best_reseller_win))
                    .withColumn("all_names", 
                                F.collect_list(col("Name")).over(unsorted_reseller_win))
                    .where(col("_row") == lit(1))
                    .drop("_row")
    )


def collect_best_group(keyaccount_df: DataFrame) -> DataFrame:
    # window spec to find the latest version of the group with
    # additional sort to add determinism
    distinct_reseller_group_window = (
        Window.partitionBy(col("inf_name"), col(SRC_DB_COL))
              .orderBy(col(SRC_MODIFIED_COL).desc(), col("inf_keyaccountId")) 
    )
    return (
        keyaccount_df.withColumn("_row", F.row_number().over(distinct_reseller_group_window))
                     .where(col("_row") == lit(1))
                     .drop("_row")
    )


def collect_best_link(customers_df):
    # window spec to find most recently modified link
    # NOTE: second level of sort is to add determinism (in case of equal timestamps), also
    #       logically the same as in reseller load above, but without mapped fields
    distinct_link_window = (
        Window.partitionBy(col(CUSTOMER_ID_COL), col(SRC_DB_COL))
              .orderBy(col(SRC_MODIFIED_COL).desc(), col(ACCOUNT_ID_COL))
    )
    return (
        customers_df.withColumn("_row", F.row_number().over(distinct_link_window))
                    .where(col("_row") == lit(1))
                    .drop("_row")
    )


def get_reseller_source(accountbase_df: DataFrame) -> DataFrame:
    current_only = get_current_silver_rows(accountbase_df)
    customers_only = get_customers_from_accounts(current_only)
    deduped = collect_best_customer(customers_only)
    return deduped


def get_grouped_reseller_source(accountbase_df: DataFrame) -> DataFrame:
    current_only = get_current_silver_rows(accountbase_df)
    customers_only = get_customers_from_accounts(current_only)
    with_group_only = get_accounts_with_group(customers_only)
    deduped = collect_best_link(with_group_only)
    return deduped


def get_reseller_group_source(keyaccountbase_df: DataFrame) -> DataFrame:
    current_only = get_current_silver_rows(keyaccountbase_df)
    deduped = collect_best_group(current_only)
    return deduped


# merging - non-testable but reusable
def merge_into_stage2_table(target_delta_table:DeltaTable ,source_data:DataFrame, business_keys) -> None:
    # merge on business keys and the sys_silver_is_current, so not current records
    # in the target are never updated - instead, new current records are inserted
    merge_condition = get_merge_condition_expr(business_keys + [SYS_CURRENT_COL])
    insert_map, update_map = get_insert_update_maps_from_columns(source_data.columns, SYS_INSERT_COL)
    (
        target_delta_table.alias("target")
                      .merge(source_data.alias("source"), merge_condition)
                      .whenMatchedUpdate(
                          f"target.{SYS_HASH_COL} <> source.{SYS_HASH_COL}",
                          set=update_map)
                      .whenNotMatchedBySourceUpdate(set={f"target.{SYS_CURRENT_COL}" : lit(False)})
                      .whenNotMatchedInsert(values=insert_map)
                      .execute()
    )
  