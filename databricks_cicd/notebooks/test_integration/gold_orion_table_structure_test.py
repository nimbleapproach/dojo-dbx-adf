# Importing Libraries
from pyspark.sql import SparkSession, DataFrame
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Test Orion DDL table structure in the Gold Layer. Ensure system fields have been added to the tables") \
    .getOrCreate()

# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------
spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

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
    currencydf = spark.read.table(f"{catalog}.{schema}.currency")
    result = check_orion_dataframe_fields(currencydf)
    assert (f"DataFrame has required fields: {result}")
    
    entity_groupdf = spark.read.table(f"{catalog}.{schema}.entity_group")
    result = check_orion_dataframe_fields(entity_groupdf)
    assert (f"DataFrame has required fields: {result}")
    
    entity_to_entity_group_linkdf = spark.read.table(f"{catalog}.{schema}.entity_to_entity_group_link")
    result = check_orion_dataframe_fields(entity_to_entity_group_linkdf)
    assert (f"DataFrame has required fields: {result}")

    entitydf = spark.read.table(f"{catalog}.{schema}.entity")
    result = check_orion_dataframe_fields(entitydf)
    assert (f"DataFrame has required fields: {result}")
    
    productdf = spark.read.table(f"{catalog}.{schema}.product")
    result = check_orion_dataframe_fields(productdf)
    assert (f"DataFrame has required fields: {result}")

# Run the test
test_main()
