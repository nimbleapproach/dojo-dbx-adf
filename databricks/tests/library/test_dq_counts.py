from datetime import datetime

from library import dq_counts


def test_count_missing_string_values(spark_fixture):
    COLUMN_NAME = "to_count"
    test_data = ["val1", "    ", "val2", None, "", "Val3"]
    expected = 3

    input = spark_fixture.createDataFrame([(val,) for val in test_data], [COLUMN_NAME])

    actual = dq_counts.count_missing_string_values(input, COLUMN_NAME)

    assert actual == expected, f"Expected {expected} but was {actual}."


def test_get_basic_counts(spark_fixture):
    COLUMN_TO_COUNT = "to_count"
    TEST_COLUMNS = ["Id", COLUMN_TO_COUNT, "Description"]
    TEST_DATA = [
        (1, "val1", "Non-null entry"),
        (2, "val2", "Non-null entry"),
        (3, None, "Null entry"),
        (4, "val1", "Non-null entry repeated value"),
        (5, None, "Null entry"),
        (6, "val3", "Non-null entry"),
    ]
    SCHEMA_NAME = "test_schema_name"
    TABLE_NAME = "test_table_name"
    COLUMN_TYPE = "test column_type"
    ENV_NAME = "test_env"
    AS_OF = datetime(1981, 2, 20)
    input = spark_fixture.createDataFrame(TEST_DATA, TEST_COLUMNS)

    actual = dq_counts.get_basic_counts(
        input,
        column_name=COLUMN_TO_COUNT,
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        column_type=COLUMN_TYPE,
        env=ENV_NAME,
        as_of=AS_OF,
    ).collect()

    assert 1 == len(actual), "Should be a single row."

    row = actual[0]

    assert row.schema == SCHEMA_NAME
    assert row.table == TABLE_NAME
    assert row.col_name == COLUMN_TO_COUNT
    assert row.col_type == COLUMN_TYPE
    assert row.env == ENV_NAME

    assert row.rows == 6
    assert row.non_null_col == 4
    assert row.null_col == 2
    assert row.distinct_col == 3

    assert row.as_of == AS_OF


def test_count_multiple_values(spark_fixture):
    COLUMN_TO_COUNT = "to_count"
    COLUMN_TO_GROUP = "key_col"
    TEST_COLUMNS = ["Id", COLUMN_TO_COUNT, "Description", COLUMN_TO_GROUP]
    TEST_DATA = [
        (1, "val1", "Non-null entry A1", "A"),
        (2, "val2", "Non-null entry B1", "B"),
        (3, None, "Null entry", "A"),
        (4, "val1", "Non-null entry C1", "C"),
        (5, None, "Null entry", "C"),
        (6, "val3", "Non-null entry B2", "B"),
        (7, "val2", "Non-null entry C2", "C"),
        (8, "val1", "Repeated entry A1", "A"),
    ]
    expected = 2
    input = spark_fixture.createDataFrame(TEST_DATA, TEST_COLUMNS)

    actual = dq_counts.count_multiple_values(
        input, id_column=COLUMN_TO_GROUP, name_column=COLUMN_TO_COUNT
    )

    assert expected == actual, f"Expected {expected} but was {actual}."
