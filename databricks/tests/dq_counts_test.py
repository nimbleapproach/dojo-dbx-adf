""" Example of unit test for library functions."""

from unittest import TestCase
from pyspark.sql import SparkSession
from library import dq_counts

class TestDqCounts(TestCase):

  def setUp(self) -> None:
    self.spark = SparkSession.builder.getOrCreate()

  def test_count_missing_string_values(self):
    COLUMN_NAME = "to_count"
    test_data = ["val1", "    ", "val2", None, "", "Val3"]
    expected_count = 3

    in_df = self.spark.createDataFrame([(val,) for val in test_data], [COLUMN_NAME])
    actual_count = dq_counts.count_missing_string_values(in_df, COLUMN_NAME)

    assert actual_count == expected_count, f"Expected {expected_count} but was {actual_count}."
