# Databricks notebook source
# MAGIC %run ./nb-orion-common

# COMMAND ----------

# Importing Libraries
from databricks.sdk import WorkspaceClient
import re
import io
import base64
import os

from pyspark.sql.functions import col,round, lower,regexp_extract, when, levenshtein, lit, length, greatest,concat, row_number, format_number, sum, date_format
from pyspark.sql import DataFrame, Window



# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

from pyspark.sql.functions import col, lower, concat_ws

df_arr = spark.table(f"{catalog}.{schema}.arr_auto_3")

x = df_arr \
    .withColumnRenamed('sku', 'product_code') \
    .withColumnRenamed('vendor_name', 'vendor_code') \
    .withColumnRenamed('type', 'product_type') \
    .withColumnRenamed('months', 'Commitment_Duration_in_months') \
    .withColumnRenamed('duration', 'Commitment_Duration_Value') \
    .withColumnRenamed('frequency', 'Billing_Frequency') \
    .withColumnRenamed('consumption', 'Consumption_Model')\
    .withColumnRenamed('matched_type', 'matched_arr_type')
#.withColumnRenamed('producttype', 'product_type') \
#.withColumnRenamed('type', 'product_type') \
    

# Create product_vendor_code by concatenating vendor_code and product_code
x = x.withColumn('product_vendor_code', concat_ws('|', col('vendor_code'), col('product_code')))

# Convert specified columns to lowercase
x = x.withColumn('product_vendor_code', lower(col('product_vendor_code'))) \
     .withColumn('product_code', lower(col('product_code'))) \
     .withColumn('vendor_code', lower(col('vendor_code')))

# Select final columns and remove duplicates
x = x.select('product_code', 
             'vendor_code',
             'product_type',
             'product_vendor_code',
             'Commitment_Duration_in_months',
             'Commitment_Duration_Value',
             'Billing_Frequency',
             'Consumption_Model',
             'matched_arr_type').dropDuplicates()

# COMMAND ----------

#df_arr.filter(lower(col('sku'))==lower(lit('SS-WTP-ND-T9-C'))).display()

#x.filter(lower(col('product_code'))==lower(lit('SS-WTP-ND-T9-C'))).display()

x.filter(lower(col('matched_arr_type'))==lower(lit('mt=manual'))).display()




# COMMAND ----------

df_trans = spark.table(f"{catalog}.{schema}.vw_globaltransactions_obt_staging")

## analyse just sep 2024
df_trans =df_trans.filter(date_format(col('TransactionDate'), 'yyyy-MM') == '2024-11')

# df_trans = df_trans.replace({'NaN': None})

# Duration in years (rounded and formatted)
df_trans = df_trans.withColumn(
    "CommitmentDuration2Master",
    when(col("CommitmentDuration2Master").rlike(r'.*YR'),
        concat(
            format_number(
                regexp_extract(col("CommitmentDuration2Master"), r"(\d*\.?\d+)", 1).cast("float"),
                2
            ),
            lit(" YR")
        )
    ).otherwise(col("CommitmentDuration2Master")))



df_trans.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.globaltransactions_obt_v6")



# COMMAND ----------


df_trans_obt = spark.table(f"{catalog}.{schema}.globaltransactions_obt_v6")

# Nullify the specified columns
columns_to_nullify = [
    'ProductTypeMaster', 
    'CommitmentDuration1Master', 
    'CommitmentDuration2Master', 
    'BillingFrequencyMaster', 
    'ConsumptionModelMaster'
]

for col_name in columns_to_nullify:
    if col_name in df_trans_obt.columns:
        df_trans_obt = df_trans_obt.withColumn(col_name, lit(None).cast("string"))

df_trans_obt= df_trans_obt.withColumn('matched_arr_type', lit(''))

df_trans_obt.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.globaltransactions_auto")


# COMMAND ----------

# Run the OPTIMIZE command to compact files and improve performance.
#spark.sql(f"OPTIMIZE {catalog}.{schema}.globaltransactions_auto"
#spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "104857600") # Target 100 MB file size
spark.sql(f"OPTIMIZE {catalog}.{schema}.globaltransactions_auto ZORDER BY (SKUMaster, VendorNameMaster, SKUInternal)")


# COMMAND ----------


df_trans = spark.table(f"{catalog}.{schema}.globaltransactions_auto")

# Lowercase `SKUInternal`
df_trans = df_trans.withColumn('sku_internal_computed', lower(col('SKUInternal')))

# Compute `vendor_code_computed` with conditional logic
df_trans = df_trans.withColumn('vendor_code_computed',
                               when(col('VendorNameMaster') == 'NaN', 
                                    lower(col('VendorNameInternal')))
                               .otherwise(lower(col('VendorNameMaster'))))
# Add `used_internal_flag` column to indicate if VendorNameInternal was used
df_trans = df_trans.withColumn('used_internal_vendor_flag', 
                               when(col('VendorNameMaster') == 'NaN', True).otherwise(False))


# Show the resulting DataFrame schema and sample data for verification
df_trans.select('VendorNameMaster','VendorNameInternal', 'SKUInternal', 'vendor_code_computed', 'used_internal_vendor_flag').show()


# COMMAND ----------


def udf_run_match(df: DataFrame, x: DataFrame,
                  df_control_col: str, x_control_col:str, df_match_col: str, x_match_col: str, 
                  initial_similarity_threshold: float = 0.9, iterations: int = 3, final_pass: int = 0) -> DataFrame:
    """
    Performs iterative fuzzy matching on two columns across two DataFrames with decreasing similarity thresholds.

    Parameters:
    - df: DataFrame - The main DataFrame to match and update.
    - x: DataFrame - The lookup DataFrame with columns to match against.
    - df_control_col: str - Usually the SKU column name in `df` to match with `x.product_code`.
    - x_control_col: str - Usually the product_code column name in `x` to match with `df.SKU`.
    - df_match_col: str - The column name in `df` to perform fuzzy matching on.
    - x_match_col: str - The column name in `x` to match with `df_match_col`.
    - initial_similarity_threshold: float - The starting similarity threshold (0 to 1 scale).
    - iterations: int - The number of iterations to perform, reducing threshold each time.
    - final_pass: int - this is used to match NaN SKUs and NaN Vendors, as this is the last resort for matching
    Returns:
    - DataFrame: The final updated DataFrame.
    """
    
    
    if initial_similarity_threshold==1:
        iterations=1 #reset the iteration to 1
    if final_pass==0:
        filter_nans = ((col(f'df.computed_{df_match_col}') != lit('nan')) & 
                    (col(f'df.computed_{df_control_col}') != lit('nan')))
    else:
        filter_nans = lit(True)  # A filter that always evaluates to True

    
    print("######### permforming match iteration [",iterations,"] #######")  
    # Loop through each iteration with decreasing similarity threshold
    for i in range(iterations):
        # Adjust similarity threshold for each iteration
        similarity_threshold = initial_similarity_threshold - (i * 0.1)

        
        # Prepare `df` DataFrame by lowercasing the matching columns
        df = df.withColumn(f'computed_{df_match_col}', lower(col(df_match_col))) \
           .withColumn(f'computed_{df_control_col}', lower(col(df_control_col)))
          
        # Perform fuzzy matching and join
        if similarity_threshold < 1:
            result2 = df.alias('df').join(
            x.alias('x').withColumnRenamed('matched_arr_type', 'x_matched_arr_type'),
                (col(f'df.computed_{df_control_col}') == col(f'x.{x_control_col}')) &
                ((1 - (levenshtein(col(f'df.computed_{df_match_col}'), col(f'x.{x_match_col}')) / 
                    greatest(length(col(f'df.computed_{df_match_col}')), length(col(f'x.{x_match_col}'))))) 
                >= similarity_threshold) &
                (col('df.is_matched') == 0) &
                (col(f'df.computed_{df_match_col}') != 'nan'),
                how='left'
            ).withColumn(
                'levenshtein_distance',
                format_number(1 - (levenshtein(col(f'df.computed_{df_match_col}'), col(f'x.{x_match_col}')) / 
                    greatest(length(col(f'df.computed_{df_match_col}')), length(col(f'x.{x_match_col}')))),2)
            )
        else:
            # Perform exact matching without Levenshtein
            result2 = df.alias('df').join(
            x.alias('x').withColumnRenamed('matched_arr_type', 'x_matched_arr_type'),
                (col(f'df.computed_{df_control_col}') == col(f'x.{x_control_col}')) &
                (col(f'df.computed_{df_match_col}') == col(f'x.{x_match_col}')) &
                (col('df.is_matched') == 0) &
                filter_nans,
                how='left'
            ).withColumn(
                'levenshtein_distance',
                lit(1)  # Set to 1 as Levenshtein is not used in this case
            )
        # Define window by SKU to keep only the closest match
        # originally used business keys to partition but now create an Id for base transactions
                # business keys 'df.DocumentNo','df.TransactionDate', 'df.SKUInternal', 'df.VendorNameInternal', 'df.ResellerCode','df.EndCustomer','df.GL_Group','df.SalesOrderDate'
        window_spec = Window.partitionBy('df.Id').orderBy(col('levenshtein_distance'))
        
        # Add row number within each partition to rank by distance
        result2 = result2.withColumn('row_num', row_number().over(window_spec))

        matched_count = result2.filter(col(f'x.{x_control_col}').isNotNull() & (col('row_num') == 1)).count()
        # Log matched_count or use it in subsequent logic if needed.
        print("iteration ",i, ", for similarity_threshold ", similarity_threshold, ", result matched count:", matched_count)

        if matched_count>0:
            # Define the `is_matched` update logic with nested conditions
            is_matched_update = when(
                (lit(initial_similarity_threshold) == 1) & col('x.product_code').isNotNull(), 1  # Condition for threshold == 1 and a match
            ).when(
                col('x.product_code').isNotNull(), 2  # Condition for fuzzy matching used
            ).otherwise(col('df.is_matched'))  # Default to the original value

            # Define the `matched_type` update logic with nested conditions
            matched_type_update = when(
                (lit(initial_similarity_threshold) == 1) & col('x.product_code').isNotNull(),
                lit(f'mt={df_control_col}|{df_match_col}')  # Set to df_match_col if threshold == 1 and there’s a match
            ).when(
                col('x.product_code').isNotNull(),
                concat(lit(f'mt={df_control_col}|{df_match_col}:th={similarity_threshold:.1f}:lv='), col('levenshtein_distance'))  # Set to concatenated string for other matches
            ).otherwise(col('df.matched_type'))  # Default to original matched_type

            matched_arr_type_update = when(col('x.product_code').isNotNull(), col('x_matched_arr_type')).otherwise(col('df.matched_arr_type'))

            # Define dictionary for column updates if a match is found
            column_updates = {
                'is_matched': is_matched_update,
                'matched_type': matched_type_update,
                'matched_arr_type': matched_arr_type_update,
                'ProductTypeMaster': when(col('x.product_code').isNotNull(), col('x.product_type')).otherwise(col('df.ProductTypeMaster')),
                'CommitmentDuration1Master': when(col('x.product_code').isNotNull(), col('x.Commitment_Duration_in_months')).otherwise(col('df.CommitmentDuration1Master')),
                'CommitmentDuration2Master': when(col('x.product_code').isNotNull(), col('x.Commitment_Duration_Value')).otherwise(col('df.CommitmentDuration2Master')),
                'BillingFrequencyMaster': when(col('x.product_code').isNotNull(), col('x.Billing_Frequency')).otherwise(col('df.BillingFrequencyMaster')),
                'ConsumptionModelMaster': when(col('x.product_code').isNotNull(), col('x.Consumption_Model')).otherwise(col('df.ConsumptionModelMaster'))
            }

            # Apply column updates for this iteration
            for col_name, update_expr in column_updates.items():
                result2 = result2.withColumn(col_name, update_expr)

            # Filter to keep only the closest match
            result2 = result2.filter(col('row_num') == 1).drop('row_num', 'levenshtein_distance')
            
            # Update `df` DataFrame with the new values from `result2` for the next iteration
            df = result2

            # Select relevant columns to create the final output DataFrame
            selected_columns = [
                'is_matched','matched_arr_type', 'matched_type', 'VendorNameInternal', 'VendorNameMaster', 'SKUInternal', 'SKUMaster',
                'ProductTypeMaster', 'CommitmentDuration1Master', 'CommitmentDuration2Master', 'BillingFrequencyMaster', 
                'ConsumptionModelMaster', 'GroupEntityCode', 'Id','EntityCode', 'DocumentNo', 'TransactionDate', 'SalesOrderDate', 
                'SalesOrderID', 'SalesOrderItemID', 'Description', 'Technology', 'ProductTypeInternal', 'VendorCode', 
                'VendorGeography', 'VendorStartDate', 'ResellerCode', 'ResellerNameInternal', 'ResellerGeographyInternal', 
                'ResellerStartDate', 'ResellerGroupCode', 'ResellerGroupName', 'ResellerGroupStartDate', 'EndCustomer', 
                'IndustryVertical', 'CurrencyCode', 'RevenueAmount', 'Period_FX_rate', 'RevenueAmount_Euro', 'GP1', 'GP1_Euro', 
                'COGS', 'COGS_Euro', 'GL_Group', 'TopCostFlag'
            ]

            df = df.select(*selected_columns)

            # Save the resulting DataFrame as a table
            df.write \
            .mode("overwrite") \
            .format("delta") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{catalog}.{schema}.globaltransactions_auto")

    return df


# COMMAND ----------

df_trans = spark.table(f"{catalog}.{schema}.globaltransactions_auto")

#df_trans.display()

# COMMAND ----------

df = df_trans
df_control_col="SKUInternal"
x_control_col="product_code"
df_match_col="VendorNameMaster" 
x_match_col="vendor_code"
initial_similarity_threshold: float=1 
iterations=2
final_pass=0


if initial_similarity_threshold==1:
    iterations=1 #reset the iteration to 1
if final_pass==0:
    filter_nans = ((col(f'df.computed_{df_match_col}') != lit('nan')) & 
                (col(f'df.computed_{df_control_col}') != lit('nan')))
else:
    filter_nans = lit(True)  # A filter that always evaluates to True


print("######### permforming match iteration [",iterations,"] #######")  
# Loop through each iteration with decreasing similarity threshold
for i in range(iterations):
    # Adjust similarity threshold for each iteration
    similarity_threshold = initial_similarity_threshold - (i * 0.1)

    
    # Prepare `df` DataFrame by lowercasing the matching columns
    df = df.withColumn(f'computed_{df_match_col}', lower(col(df_match_col))) \
        .withColumn(f'computed_{df_control_col}', lower(col(df_control_col)))
        
    # Perform fuzzy matching and join
    if similarity_threshold < 1:
        result2 = df.alias('df').join(
            x.alias('x').withColumnRenamed('matched_arr_type', 'x_matched_arr_type'),
            (col(f'df.computed_{df_control_col}') == col(f'x.{x_control_col}')) &
            ((1 - (levenshtein(col(f'df.computed_{df_match_col}'), col(f'x.{x_match_col}')) / 
                greatest(length(col(f'df.computed_{df_match_col}')), length(col(f'x.{x_match_col}'))))) 
            >= similarity_threshold) &
            (col('df.is_matched') == 0) &
            (col(f'df.computed_{df_match_col}') != 'nan'),
            how='left'
        ).withColumn(
            'levenshtein_distance',
            format_number(1 - (levenshtein(col(f'df.computed_{df_match_col}'), col(f'x.{x_match_col}')) / 
                greatest(length(col(f'df.computed_{df_match_col}')), length(col(f'x.{x_match_col}')))),2)
        )
    else:
        # Perform exact matching without Levenshtein
        result2 = df.alias('df').join(
            x.alias('x').withColumnRenamed('matched_arr_type', 'x_matched_arr_type'),
            (col(f'df.computed_{df_control_col}') == col(f'x.{x_control_col}')) &
            (col(f'df.computed_{df_match_col}') == col(f'x.{x_match_col}')) &
            (col('df.is_matched') == 0) &
            filter_nans,
            how='left'
        ).withColumn(
            'levenshtein_distance',
            lit(1)  # Set to 1 as Levenshtein is not used in this case
        )
    # Define window by SKU to keep only the closest match
    # originally used business keys to partition but now create an Id for base transactions
            # business keys 'df.DocumentNo','df.TransactionDate', 'df.SKUInternal', 'df.VendorNameInternal', 'df.ResellerCode','df.EndCustomer','df.GL_Group','df.SalesOrderDate'
    window_spec = Window.partitionBy('df.Id').orderBy(col('levenshtein_distance'))
    
    # Add row number within each partition to rank by distance
    result2 = result2.withColumn('row_num', row_number().over(window_spec))

    matched_count = result2.filter(col(f'x.{x_control_col}').isNotNull() & (col('row_num') == 1)).count()
    # Log matched_count or use it in subsequent logic if needed.
    print("iteration ",i, ", for similarity_threshold ", similarity_threshold, ", result matched count:", matched_count)

    if matched_count>0:
        # Define the `is_matched` update logic with nested conditions
        is_matched_update = when(
            (lit(initial_similarity_threshold) == 1) & col('x.product_code').isNotNull(), 1  # Condition for threshold == 1 and a match
        ).when(
            col('x.product_code').isNotNull(), 2  # Condition for fuzzy matching used
        ).otherwise(col('df.is_matched'))  # Default to the original value

        # Define the `matched_type` update logic with nested conditions
        matched_type_update = when(
            (lit(initial_similarity_threshold) == 1) & col('x.product_code').isNotNull(),
            lit(f'mt={df_control_col}|{df_match_col}')  # Set to df_match_col if threshold == 1 and there’s a match
        ).when(
            col('x.product_code').isNotNull(),
            concat(lit(f'mt={df_control_col}|{df_match_col}:th={similarity_threshold:.1f}:lv='), col('levenshtein_distance'))  # Set to concatenated string for other matches
        ).otherwise(col('df.matched_type'))  # Default to original matched_type

        matched_arr_type_update = when(col('x.product_code').isNotNull(), col('x_matched_arr_type')).otherwise(col('df.matched_arr_type'))

        # Define dictionary for column updates if a match is found
        column_updates = {
            'is_matched': is_matched_update,
            'matched_type': matched_type_update,
            'matched_arr_type': matched_arr_type_update,
            'ProductTypeMaster': when(col('x.product_code').isNotNull(), col('x.product_type')).otherwise(col('df.ProductTypeMaster')),
            'CommitmentDuration1Master': when(col('x.product_code').isNotNull(), col('x.Commitment_Duration_in_months')).otherwise(col('df.CommitmentDuration1Master')),
            'CommitmentDuration2Master': when(col('x.product_code').isNotNull(), col('x.Commitment_Duration_Value')).otherwise(col('df.CommitmentDuration2Master')),
            'BillingFrequencyMaster': when(col('x.product_code').isNotNull(), col('x.Billing_Frequency')).otherwise(col('df.BillingFrequencyMaster')),
            'ConsumptionModelMaster': when(col('x.product_code').isNotNull(), col('x.Consumption_Model')).otherwise(col('df.ConsumptionModelMaster'))
        }

        # Apply column updates for this iteration
        for col_name, update_expr in column_updates.items():
            result2 = result2.withColumn(col_name, update_expr)
 
        # Filter to keep only the closest match
        result2 = result2.filter(col('row_num') == 1).drop('row_num', 'levenshtein_distance')
        
        # Update `df` DataFrame with the new values from `result2` for the next iteration
        df = result2

        # Select relevant columns to create the final output DataFrame
        selected_columns = [
            'is_matched','matched_arr_type', 'matched_type', 'VendorNameInternal', 'VendorNameMaster', 'SKUInternal', 'SKUMaster',
            'ProductTypeMaster', 'CommitmentDuration1Master', 'CommitmentDuration2Master', 'BillingFrequencyMaster', 
            'ConsumptionModelMaster', 'GroupEntityCode', 'Id','EntityCode', 'DocumentNo', 'TransactionDate', 'SalesOrderDate', 
            'SalesOrderID', 'SalesOrderItemID', 'Description', 'Technology', 'ProductTypeInternal', 'VendorCode', 
            'VendorGeography', 'VendorStartDate', 'ResellerCode', 'ResellerNameInternal', 'ResellerGeographyInternal', 
            'ResellerStartDate', 'ResellerGroupCode', 'ResellerGroupName', 'ResellerGroupStartDate', 'EndCustomer', 
            'IndustryVertical', 'CurrencyCode', 'RevenueAmount', 'Period_FX_rate', 'RevenueAmount_Euro', 'GP1', 'GP1_Euro', 
            'COGS', 'COGS_Euro', 'GL_Group', 'TopCostFlag'
        ]

        df = df.select(*selected_columns)

        # Save the resulting DataFrame as a table
        #df.display()


# COMMAND ----------

df.select(col('matched_arr_type'))

# COMMAND ----------

# DBTITLE 1,SKUInternal|VendorNameInternal|thres 1
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... control param is SKUInternal this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUInternal", 
    x_control_col="product_code",
    df_match_col="VendorNameInternal", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=1, 
    iterations=1
)


# COMMAND ----------

df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

# display(df_trans.where(col('is_matched')>0))

# COMMAND ----------

# DBTITLE 1,SKUInternal|VendorNameMaster|thres 1
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

## test the function with 100% similarity threshold against the VendorNameMaster code... this should find matches
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUInternal", 
    x_control_col="product_code",
    df_match_col="VendorNameMaster", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=1, 
    iterations=1,
    final_pass=0
)


# COMMAND ----------

df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

# display(df_trans.where(col('is_matched')>0))

# COMMAND ----------

# DBTITLE 1,SKUInternal|VendorNameMaster|thres 0.8
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUInternal", 
    x_control_col="product_code",
    df_match_col="VendorNameMaster", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=0.8, 
    iterations=3,
    final_pass=0
)


# COMMAND ----------

# DBTITLE 1,VendorNameMaster|SKUMaster|thres 1
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the SKUMaster code... this should find some matches
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="VendorNameMaster", 
    x_control_col="vendor_code",
    df_match_col="SKUMaster", 
    x_match_col="product_code", 
    initial_similarity_threshold=1, 
    iterations=1,
    final_pass=0
)


# COMMAND ----------

df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

display(df_trans.where(col('is_matched')>1))
##doesnt provide any matches

# COMMAND ----------

# DBTITLE 1,SKUMaster|VendorNameMaster|thres 0.8
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... control param is SKUMaster this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUMaster", 
    x_control_col="product_code",
    df_match_col="VendorNameMaster", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=0.8, 
    iterations=3
)


# COMMAND ----------

# DBTITLE 1,SKUMaster|VendorNameMaster|thres 0.5
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... control param is SKUMaster this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUMaster", 
    x_control_col="product_code",
    df_match_col="VendorNameMaster", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=0.5, 
    iterations=3
)


# COMMAND ----------

df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

display(df_trans.where(col('is_matched')>2))
##


# COMMAND ----------

# DBTITLE 1,SKUInternal|VendorNameInternal|thres 0.8
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... control param is SKUInternal this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUInternal", 
    x_control_col="product_code",
    df_match_col="VendorNameInternal", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=0.8, 
    iterations=3
)


# COMMAND ----------

# DBTITLE 1,SKUInternal|VendorNameMaster|thres 0.8
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... control param is SKUInternal this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUInternal", 
    x_control_col="product_code",
    df_match_col="VendorNameMaster", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=0.8, 
    iterations=3
)


# COMMAND ----------

# DBTITLE 1,SKUMaster|VendorNameInternal|thres 0.8
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the VendorNameMaster code... control param is SKUInternal this may find matches as the threshold has been lowered
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="SKUMaster", 
    x_control_col="product_code",
    df_match_col="VendorNameInternal", 
    x_match_col="vendor_code", 
    initial_similarity_threshold=0.8, 
    iterations=3
)


# COMMAND ----------

# Remove files older than 7 days (default is 7 days, adjust if needed)
spark.sql(f"VACUUM {catalog}.{schema}.globaltransactions_auto RETAIN 168 HOURS")


# COMMAND ----------

# DBTITLE 1,VendorNameMaster|SKUMaster|thres 1
df_trans=spark.table(f"{catalog}.{schema}.globaltransactions_auto")


## test the function with 100% similarity threshold againster the SKUMaster code... this should find some matches
result = udf_run_match(
    df=df_trans, 
    x=x, 
    df_control_col="VendorNameMaster", 
    x_control_col="vendor_code",
    df_match_col="SKUMaster", 
    x_match_col="product_code", 
    initial_similarity_threshold=1, 
    iterations=1,
    final_pass=1
)


# COMMAND ----------


df=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

display(df.where(col('is_matched')==2))

# COMMAND ----------

df_obt=spark.table(f"{catalog}.{schema}.globaltransactions_obt_v6")
df_orion=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

print(df_obt.count())
print(df_orion.count())


# COMMAND ----------


display(df_obt.filter(
    (col('GroupEntityCode') == 'VU') & 
    (date_format(col('TransactionDate'), 'yyyy-MM') == '2024-11') &
    (col('SKUInternal') == 'AVMB-SUB - Altaro VM Backup for MSPs')
).orderBy(col('RevenueAmount_Euro').desc()))



display(df_orion.filter(
    (col('GroupEntityCode') == 'VU') & 
    (date_format(col('TransactionDate'), 'yyyy-MM') == '2024-11') &
    (col('SKUInternal') == 'AVMB-SUB - Altaro VM Backup for MSPs')
).orderBy(col('RevenueAmount_Euro').desc()))

# COMMAND ----------


df_obt=spark.table(f"{catalog}.{schema}.globaltransactions_obt_v6")

df_orion=spark.table(f"{catalog}.{schema}.globaltransactions_auto")

# Rename the column 'id' to 'Id'
# df_orion_renamed = df_orion.withColumnRenamed("id", "Id")
# Define composite key
composite_key = [
    'Id'
]

# Define fields to compare - need to include mrrratio since we want to compare it
fields_to_compare = [
        'ProductTypeMaster', 'CommitmentDuration1Master', 'CommitmentDuration2Master', 
        'BillingFrequencyMaster', 'ConsumptionModelMaster'
]


# Define additional fields
additional_fields = [
    'is_matched', 'matched_type', 'matched_arr_type', 'GroupEntityCode', 'EntityCode', 'CurrencyCode',
    'RevenueAmount', 'Period_FX_rate', 'RevenueAmount_Euro'
    ,'DocumentNo', 'TransactionDate', 'SKUInternal', 'VendorNameInternal', 
    'ResellerCode', 'EndCustomer', 'GL_Group', 'SalesOrderDate'
]




comparison_df = compare_dataframes(                
    df1=df_obt, 
    df2=df_orion, 
    composite_key=composite_key, 
    fields_to_compare=fields_to_compare, 
    additional_fields=additional_fields)
#display(comparison_df)


# COMMAND ----------


comparison_df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.globaltransactions_comparison")

# COMMAND ----------

df_comparison=spark.table(f"{catalog}.{schema}.globaltransactions_comparison")

# COMMAND ----------

display(df_comparison.filter(col('SKUInternal')=='SS-WTP-ND-T9-C'))

# COMMAND ----------



df_comparison=spark.table(f"{catalog}.{schema}.globaltransactions_comparison")


df_significantdiff = df_comparison.filter(
        (col('is_matched') >= 2))
        # & 
        #(col('BillingFrequencyMaster_changes').like('NaN -> %')) & 
        #(~col('BillingFrequencyMaster_changes').like('NaN -> Not assigned'))
    

display(df_comparison)
# display(df_significantdiff.filter(
#     (col('GroupEntityCode') == 'VU') & 
#     (date_format(col('TransactionDate'), 'yyyy-MM') == '2024-11')
# ))

# COMMAND ----------



df_agg = df_significantdiff.groupBy(
    col('GroupEntityCode'), 
    date_format(col('TransactionDate'), 'yyyy-MM')
).agg(
    sum(col('RevenueAmount_Euro')).alias('TotalRevenue')
).where(date_format(col('TransactionDate'), 'yyyy-MM') == '2024-11') 
display(df_agg)


# COMMAND ----------

display(df_obt.filter(col('Id')==8590880627))

# COMMAND ----------

 

df_gt_auto=spark.table(f"{catalog}.{schema}.globaltransactions_comparison")


# First create the base aggregation
base_agg = (df_gt_auto
            .filter(col("is_matched") > 0)
            .groupBy("EntityCode", "VendorNameInternal")
            .agg(round(sum("RevenueAmount_Euro")).alias("sum_group_rev"))
)

# Main query with join and final aggregation
result = (df_gt_auto.alias("gt")
.filter(col("is_matched") > 0)
.join(
        base_agg,
        ["EntityCode", "VendorNameInternal"],
        "inner"
    )
    .groupBy(
        "EntityCode",
        "matched_type",
        "VendorNameInternal",
        "sum_group_rev"
    )
    .agg(
        round(sum("RevenueAmount_Euro")).alias("sum_rev")
    )
    .withColumn(
        "ratio",
        round(col("sum_rev") / col("sum_group_rev"), 4)
    )
)

# Display or use the result
result.display()
