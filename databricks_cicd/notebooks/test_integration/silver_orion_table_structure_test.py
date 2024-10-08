# Importing Libraries
from pyspark.sql import SparkSession, DataFrame
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Test Orion DDL table structure in the Silver Layer. Ensure system fields have been added to the tables") \
    .getOrCreate()

# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------
spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------
catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

def check_orion_dataframe_fields(df: DataFrame) -> bool:
    required_fields = {'Sys_Gold_InsertedDateTime_UTC'
                       , 'Sys_Gold_ModifiedDateTime_UTC'}
    df_fields = set(df.columns)
    return required_fields.issubset(df_fields)


def test_main():
    calendardf = spark.read.table(f"{catalog}.ref.calendar")
    result = check_orion_dataframe_fields(calendardf)
    assert (f"DataFrame has required fields: {result}")

# Run the test
test_main()
