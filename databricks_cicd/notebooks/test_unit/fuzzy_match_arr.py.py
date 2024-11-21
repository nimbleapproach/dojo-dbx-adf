# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit

class FuzzyMatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("FuzzyMatchTest") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_exact_match(self):
        # Prepare input DataFrames
        df = self.spark.createDataFrame([
            Row(SKU="ABC123", VendorName="Test Vendor", is_matched=0, computed_VendorName="test vendor")
        ])
        x = self.spark.createDataFrame([
            Row(product_code="ABC123", vendor_code="Test Vendor")
        ])

        # Call udf_run_match with similarity_threshold=1
        result_df = udf_run_match(df, x, "SKU", "product_code", "VendorName", "vendor_code", initial_similarity_threshold=1)
        
        # Assert results for exact match
        self.assertEqual(result_df.filter("is_matched = 2").count(), 1)
        self.assertEqual(result_df.filter("matched_type = 'VendorName'").count(), 1)

    def test_fuzzy_match(self):
        # Prepare input DataFrames with slightly different names
        df = self.spark.createDataFrame([
            Row(SKU="DEF456", VendorName="Best Vendor", is_matched=0, computed_VendorName="best vendor")
        ])
        x = self.spark.createDataFrame([
            Row(product_code="DEF456", vendor_code="BestVendor")
        ])

        # Call udf_run_match with similarity_threshold < 1
        result_df = udf_run_match(df, x, "SKU", "product_code", "VendorName", "vendor_code", initial_similarity_threshold=0.9)
        
        # Assert results for fuzzy match (should find a match based on Levenshtein similarity)
        self.assertEqual(result_df.filter("is_matched = 3").count(), 1)
        self.assertTrue(result_df.filter("matched_type LIKE 'VendorName:th%'").count() > 0)

    def test_no_match(self):
        # Prepare input DataFrames where no match should be found
        df = self.spark.createDataFrame([
            Row(SKU="GHI789", VendorName="Another Vendor", is_matched=0, computed_VendorName="another vendor")
        ])
        x = self.spark.createDataFrame([
            Row(product_code="XYZ123", vendor_code="Different Vendor")
        ])

        # Call udf_run_match with similarity_threshold=0.9
        result_df = udf_run_match(df, x, "SKU", "product_code", "VendorName", "vendor_code", initial_similarity_threshold=0.9)
        
        # Assert results for no match
        self.assertEqual(result_df.filter("is_matched = 0").count(), 1)

if __name__ == '__main__':
    unittest.main()

