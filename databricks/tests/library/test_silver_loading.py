from pyspark.sql.functions import col, lit
from pyspark.testing import assertDataFrameEqual
import pytest

from library.silver_loading import (
    get_merge_condition_expr,
    get_current_silver_rows,
    get_insert_update_maps_from_columns,
    get_select_from_column_map,
)


def test_silver_loading_get_select_from_column_map():
    input = {
        "target_1": "source_x",
        "target_2": "source_y",
        "target_3": None,
        "target_n": "source_z",
    }

    expected = [
        col("source_x").alias("target_1"),
        col("source_y").alias("target_2"),
        lit("NaN").alias("target_3"),
        col("source_z").alias("target_n"),
    ]

    actual = get_select_from_column_map(input)

    assert len(expected) == len(actual)

    for exp, act in zip(expected, actual):
        # comparing str representations to avoid merging of the col expressions to bool
        assert str(exp) == str(act)


def test_get_merge_condition_expr_with_defaults():
    test_columns_list = ["col_a", "col_x", "col_z"]
    expected = "source.col_a = target.col_a AND source.col_x = target.col_x AND source.col_z = target.col_z"

    actual = get_merge_condition_expr(test_columns_list)

    assert actual == expected


def test_get_merge_condition_expr_with_aliases():
    test_columns_list = ["col_a", "col_x", "col_z"]
    expected = "src.col_a = tgt.col_a AND src.col_x = tgt.col_x AND src.col_z = tgt.col_z"

    actual = get_merge_condition_expr(test_columns_list,
                                      source_alias="src",
                                      target_alias="tgt")

    assert actual == expected


def test_get_insert_update_maps_from_columns():
    test_columns_list = ["col_a", "col_x", "col_z"]
    test_sys_insert_col = "col_x"

    expected_insert = {
        "target.col_a" : "source.col_a",  
        "target.col_x" : "source.col_x",  
        "target.col_z" : "source.col_z",  
    }
    expected_update = {
        "target.col_a" : "source.col_a",  
        "target.col_z" : "source.col_z",  
    }

    actual_insert, actual_update = get_insert_update_maps_from_columns(test_columns_list, test_sys_insert_col)

    assert expected_insert == actual_insert 
    assert expected_update == actual_update 


def test_get_insert_update_maps_from_columns_with_sys_insert_col_not_in():
    test_columns_list = ["col_a", "col_x", "col_z"]
    test_sys_insert_col = "column_not_in_list"

    # the dictionaries are expected to be the same
    expected_insert = {
        "target.col_a" : "source.col_a",  
        "target.col_x" : "source.col_x",  
        "target.col_z" : "source.col_z",  
    }
    expected_update = {
        "target.col_a" : "source.col_a",  
        "target.col_x" : "source.col_x",  
        "target.col_z" : "source.col_z",  
    }

    actual_insert, actual_update = get_insert_update_maps_from_columns(test_columns_list, test_sys_insert_col)

    assert expected_insert == actual_insert 
    assert expected_update == actual_update


def test_get_current_silver_rows_with_defaults(spark_fixture):
    # the default column name assigned by the bronze to silver generic pipeline
    IS_CURRENT_COL_NAME = "Sys_Silver_IsCurrent"
    COLUMNS = ["Id", IS_CURRENT_COL_NAME, "Comment"]

    test_data = [
        (1, True, "First"),
        (2, False, "No"),
        (3, None, "Null"),
        (4, True, "Second"),
        (5, False, "Null"),
        (6, True, "Third"),
    ]
    input_df = spark_fixture.createDataFrame(test_data, COLUMNS)

    expected_data = [
        (1, True, "First"),
        (4, True, "Second"),
        (6, True, "Third"),
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, COLUMNS)

    actual_df = get_current_silver_rows(input_df)

    assertDataFrameEqual(expected_df, actual_df)


def test_get_current_silver_rows_with_current_column(spark_fixture):
    IS_CURRENT_COL_NAME = "another_name_for_current_column"
    COLUMNS = ["Id", IS_CURRENT_COL_NAME, "Comment"]

    test_data = [
        (1, True, "First"),
        (2, False, "No"),
        (3, None, "Null"),
        (4, True, "Second"),
        (5, False, "Null"),
        (6, True, "Third"),
    ]
    input_df = spark_fixture.createDataFrame(test_data, COLUMNS)

    expected_data = [
        (1, True, "First"),
        (4, True, "Second"),
        (6, True, "Third"),
    ]
    expected_df = spark_fixture.createDataFrame(expected_data, COLUMNS)

    actual_df = get_current_silver_rows(input_df, IS_CURRENT_COL_NAME)

    assertDataFrameEqual(expected_df, actual_df)


def test_get_current_silver_rows_when_no_is_current_column(spark_fixture):
    # there is different than default "Sys_Silver_IsCurrent" 
    # column name in the data
    COLUMNS = ["Id", "some_other_is_current", "Comment"]

    test_data = [
        (1, True, "First"),
        (2, False, "No"),
        (3, None, "Null"),
        (4, True, "Second"),
        (5, False, "Null"),
        (6, True, "Third"),
    ]
    input_df = spark_fixture.createDataFrame(test_data, COLUMNS)

    with pytest.raises(Exception):
        get_current_silver_rows(input_df)
