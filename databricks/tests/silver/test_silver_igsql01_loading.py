from datetime import datetime

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
from pyspark.testing import assertDataFrameEqual
import pytest

from silver.LOADING.silver_igsql01_loading import (
    add_sys_silver_columns,
    collect_best_customer,
    collect_best_link,
    get_accounts_with_group,
    get_customers_from_accounts,
)


def test_add_sys_silver_columns(spark_fixture):
    test_input_df = (spark_fixture.range(5)
                                  .withColumn("a", F.concat(col("id"), lit("bla")))
                                  .withColumn("b", col("id")*lit(3))
                                  .withColumn("s", lit("value_not_in_hash")))
    hash_columns = ["a", "b"]
    test_datetime = datetime(2024, 8, 7, 13, 44, 9)

    actual_df = add_sys_silver_columns(test_input_df, hash_columns, test_datetime)

    expected_df = (
        test_input_df.withColumn("sys_silver_insert_date_time_utc", lit(test_datetime).cast("TIMESTAMP"))
          .withColumn("sys_silver_modified_date_time_utc", lit(test_datetime).cast("TIMESTAMP"))
          .withColumn("sys_silver_hash_key", F.xxhash64("a", "b"))
          .withColumn("sys_silver_is_current", lit(True))
    )

    assertDataFrameEqual(expected_df, actual_df)


def test_get_customers_from_accounts(spark_fixture):
    NAME_OF_CUSTOMER_FIELD = "inf_customerno"
    test_data = [
        (1, "Cust_890"),
        (2, "NaN"),
        (3, "890"),
        (4, "NaN"),
        (5, "BaNaNa"),
        (6, "X51"),
    ]
    columns = ["id", NAME_OF_CUSTOMER_FIELD]
    input_df = spark_fixture.createDataFrame(test_data, columns)

    expected_data = [
        (1, "Cust_890"),
        (3, "890"),
        (5, "BaNaNa"),
        (6, "X51"),       
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, columns)

    actual_df = get_customers_from_accounts(input_df)

    assertDataFrameEqual(expected_df, actual_df)


def test_get_accounts_with_group(spark_fixture):
    NAME_OF_GROUP_FIELD = "inf_KeyAccount"
    test_data = [
        (1, "Cust_890"),
        (2, "NaN"),
        (3, "890"),
        (4, "NaN"),
        (5, "BaNaNa"),
    ]
    columns = ["id", NAME_OF_GROUP_FIELD]
    input_df = spark_fixture.createDataFrame(test_data, columns)

    expected_data = [
        (1, "Cust_890"),
        (3, "890"),
        (5, "BaNaNa"),      
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, columns)

    actual_df = get_accounts_with_group(input_df)

    assertDataFrameEqual(expected_df, actual_df)


def test_collect_best_customer(spark_fixture):
    OLD_TS = datetime(2017,4,4,7,6,47)
    NEW_TS = datetime(2017,10,10,7,6,47)
    NEWEST_TS = datetime(2018,6,6,7,6,47)

    # cases:
    # - single customer (4)
    # - single customer id in two dbs (1,2) 
    # - multiple - most recent wins (3,5,7)
    # - multiple with same modification - first AccountId wins (alphabetically) (6,8,9)
    test_data= [
        (1,  "101", "C_01", "UK", OLD_TS, "Alice"),      # same id but diff db - single
        (2,  "102", "C_01", "PL", OLD_TS, "Alicja"),    # same id but diff db - single
        (3,  "103", "B_02", "BE", OLD_TS, "Bob"),       # B_02/BE
        (4,  "108", "B_03", "BE", OLD_TS, "Chad"),      # single
        (5,  "109", "B_02", "BE", NEWEST_TS, "Rob"),     # B_02/BE - most recent
        (6,  "110", "D_03", "DE", NEWEST_TS, "Hanna"),   # D_03/DE - first id
        (7,  "199", "B_02", "BE", NEW_TS, "Bobby"),      # B_02/BE
        (8,  "20",  "D_03", "DE", NEWEST_TS, "Hans"),    # D_03/DE
        (9,  "21",  "D_03", "DE", NEWEST_TS, "Herman"),  # D_03/DE
    ]
    in_schema = """Id BIGINT, AccountId STRING, inf_customerno STRING,
               Sys_DatabaseName STRING, ModifiedOn TIMESTAMP, Name STRING"""
    input_df = spark_fixture.createDataFrame(test_data, in_schema)

    out_schema = """Id BIGINT, AccountId STRING, inf_customerno STRING,
               Sys_DatabaseName STRING, ModifiedOn TIMESTAMP, Name STRING,
               all_names ARRAY<STRING>"""
    expected_data = [
        (1,  "101", "C_01", "UK", OLD_TS, "Alice", ["Alice",]),
        (2,  "102", "C_01", "PL", OLD_TS, "Alicja", ["Alicja",]),
        (4,  "108", "B_03", "BE", OLD_TS, "Chad", ["Chad",]),
        (5,  "109", "B_02", "BE", NEWEST_TS, "Rob", ["Rob", "Bobby", "Bob",]),
        (6,  "110", "D_03", "DE", NEWEST_TS, "Hanna", ["Hanna", "Hans", "Herman",]),
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, out_schema)

    actual_df = collect_best_customer(input_df)

    assertDataFrameEqual(expected_df, actual_df)


def test_collect_best_link(spark_fixture):
    OLD_TS = datetime(2017,4,4,7,6,47)
    NEW_TS = datetime(2017,10,10,7,6,47)
    NEWEST_TS = datetime(2018,6,6,7,6,47)
    
    # cases:
    # - multiple - most recent wins (3,5,7)
    # - multiple with same modification ts - first AccountId wins (alphabetically) (6,8,9)
    test_data= [
        (1,  "103", "B_02", "BE", OLD_TS, "Link_01"),        # B_02/BE
        (2,  "104", "B_03", "BE", OLD_TS, "Link_02"),        # single
        (3,  "105", "B_02", "BE", NEWEST_TS, "Link_03"),     # B_02/BE - most recent
        (4,  "110", "D_03", "DE", NEWEST_TS, "Link_04"),     # D_03/DE - first id
        (5,  "199", "B_02", "BE", NEW_TS, "Link_05"),        # B_02/BE
        (6,  "200",  "D_03", "DE", NEWEST_TS, "Link_06"),    # D_03/DE
        (7,  "201",  "D_03", "DE", NEWEST_TS, "Link_07"),    # D_03/DE
    ]
    schema = """Id BIGINT, AccountId STRING, inf_customerno STRING,
               Sys_DatabaseName STRING, ModifiedOn TIMESTAMP, Link STRING"""
    input_df = spark_fixture.createDataFrame(test_data, schema)

    expected_data = [
        (2,  "104", "B_03", "BE", OLD_TS, "Link_02"),        # single
        (3,  "105", "B_02", "BE", NEWEST_TS, "Link_03"),     # B_02/BE - most recent
        (4,  "110", "D_03", "DE", NEWEST_TS, "Link_04"),     # D_03/DE - first id
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, schema)

    actual_df = collect_best_link(input_df)

    assertDataFrameEqual(expected_df, actual_df)
