# Importing Libraries
from pyspark.sql import SparkSession, DataFrame
import os
import pytest

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read from silver_{ENVIRONMENT}.ref.calendar") \
    .getOrCreate()

# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------
spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------
catalog = spark.catalog.currentCatalog()
schema = 'ref'

# COMMAND ----------
def get_calendar() -> DataFrame:
    return spark.read.table(f"{catalog}.ref.calendar")

def test_calendar():
    calendar = get_calendar()
    assert calendar.count() > 20

# Run the test
test_calendar()
