# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA obt;

# COMMAND ----------

spark.sql( f"""
CREATE OR REPLACE VIEW end_customer AS
WITH unique_end_customer AS (
SELECT No_, 
       Entity,
       Contact_No_,
       `Name`,
       NACE_2_Code,
       Description,
       split_part(NACE_2_Code, '.', 1) AS section_or_division
FROM
  ( SELECT No_, 
           Entity,
           Contact_No_,
           `Name`,
           NACE_2_Code,
           Description,
           row_number() OVER 
             (PARTITION BY Entity, Contact_No_ ORDER BY `timestamp` DESC) row_
    FROM silver_{ENVIRONMENT}.igsql03.end_customer
    WHERE Sys_Silver_IsCurrent
      AND NACE_2_Code != 'NaN'
  )
WHERE row_ = 1
)
SELECT uec.*, nc.section
FROM unique_end_customer uec
JOIN nace_2_codes nc
  ON (uec.section_or_division = nc.section AND nc.division IS NULL)
  OR uec.section_or_division = nc.division
""")
