# Databricks notebook source

# Importing Libraries
import os
# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT
# COMMAND ----------


spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")


# COMMAND ---------- 

catalog = spark.catalog.currentCatalog()
schema = 'ref'
# COMMAND ----------


spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.calendar (
        date DATE NOT NULL,
        year SMALLINT NOT NULL,
        month TINYINT NOT NULL,
        day TINYINT NOT NULL,
        day_name STRING NOT NULL,
        day_of_week TINYINT NOT NULL,
        day_of_year SMALLINT NOT NULL,
        month_name STRING NOT NULL,
        ig_week SMALLINT NOT NULL,
        ig_week_date_start DATE NOT NULL,
        ig_week_date_end DATE NOT NULL,
        ig_year SMALLINT NOT NULL,
        ig_year_week INT NOT NULL

    )
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true,
        delta.columnMapping.mode = 'name',
        table_schema.version = 1)
    """
)

# COMMAND ----------

from datetime import date, timedelta
import pyspark.sql.functions as F

from pyspark.sql.types import DateType, StructType, StructField

def ig_week_number(d):
    # Find the first Saturday of the year
    first_saturday = date(d.year, 1, 1)
    while first_saturday.weekday() != 5:
        first_saturday += timedelta(days=1)

    # If the date is before the first Saturday, it belongs to the last week of the previous year
    if d < first_saturday:
        return ig_week_number(date(d.year -1, 12, 31))
    else:
        return (d - first_saturday).days // 7 + 1

def ig_week_start(col):
    return F.expr(
        f"""
            CASE 
                WHEN dayofweek({col}) = 7 THEN {col}
                ELSE date_sub({col}, (dayofweek({col}) + 7) % 7)
            END
        """
    )

start_date = date(2019, 1, 1)
end_date = date(2028, 12, 31)

dfschema = StructType([StructField("date", DateType(), False)])

date_range = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]
date_tuples = [(d,) for d in date_range]

df = spark.createDataFrame(date_tuples, dfschema)

ig_week_udf = F.udf(ig_week_number, "int")

df = (
    df.withColumns(
        {
            "year": F.year("date"),
            "month": F.month("date"),
            "day": F.dayofmonth("date"),
            "day_name": F.date_format("date", "E"),
            "day_of_week": F.dayofweek("date"),
            "day_of_year": F.dayofyear("date"),
            "month_name": F.date_format("date", "MMM"),
            "ig_week": ig_week_udf(F.col("date")),
            "ig_week_date_start" : ig_week_start("date"),
            "ig_week_date_end" : F.date_add("ig_week_date_start", 6),
            "ig_year": F.when((F.col("month") == 1) & (F.col("ig_week") > 50), F.col("year") - 1).otherwise(F.col("year")),
            "ig_year_week": F.concat(F.col("ig_year"), F.lpad(ig_week_udf(F.col("date")), 2, '0')).cast("int")
        }
    )
)

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

spark.sql(
    f"""
    INSERT INTO {catalog}.{schema}.calendar
    SELECT * FROM temp_view
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.ig_week_number (
        d DATE COMMENT 'date or timestamp'
    )
    RETURNS TINYINT 
    LANGUAGE SQL
    DETERMINISTIC
    READS SQL DATA
    RETURN (
        SELECT FIRST(ig_week)
        FROM {catalog}.{schema}.calendar
        WHERE date = d
    )
    """
)
