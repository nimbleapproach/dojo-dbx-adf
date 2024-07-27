from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from library.silver_loading import get_select_from_column_map

class TestSilverLoading(TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def test_get_select_from_column_map(self):

        input_map = {
            "target_1" : "source_x",
            "target_2" : "source_y",
            "target_3" : None,
            "target_n" : "source_z"
        }

        expected = [
            col("source_x").alias("target_1"),
            col("source_y").alias("target_2"),
            lit("NaN").alias("target_3"),
            col("source_z").alias("target_n"),
        ]
        
        actual = get_select_from_column_map(input_map)

        assert len(expected) == len(actual)
        for exp, act in zip(expected, actual):
            # comparing str representations to avoid merging of the col expressions to bool
            assert str(exp) == str(act)
