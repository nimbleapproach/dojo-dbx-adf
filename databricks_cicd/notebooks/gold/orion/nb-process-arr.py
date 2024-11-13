# Databricks notebook source
# Importing Libraries
from databricks.sdk import WorkspaceClient
import re
import io
import base64
import os

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

df_arr= spark.table("gold_dev.orion.link_product_to_vendor_arr")

# COMMAND ----------

from pyspark.sql.functions import lower, when, col

df_trans = spark.table("platinum_dev.obt.globaltransactions")

# Lowercase `SKUInternal`
df_trans = df_trans.withColumn('SKUInternal', lower(col('SKUInternal')))

# Compute `vendor_code_computed` with conditional logic
df_trans = df_trans.withColumn('vendor_code_computed',
                               when(col('VendorNameMaster') == 'NaN', 
                                    lower(col('VendorNameInternal')))
                               .otherwise(lower(col('VendorNameMaster'))))
# Add `used_internal_flag` column to indicate if VendorNameInternal was used
df_trans = df_trans.withColumn('used_internal_flag', 
                               when(col('VendorNameMaster') == 'NaN', True).otherwise(False))


# Show the resulting DataFrame schema and sample data for verification
df_trans.select('VendorNameMaster','VendorNameInternal', 'SKUInternal', 'vendor_code_computed', 'used_internal_flag').show()


# COMMAND ----------

from pyspark.sql.functions import col, lower

# Create `x` DataFrame with distinct rows where `is_current` is 1 and columns are lowercased
x = df_arr.filter(col('is_current') == 1)
x = x.withColumn('product_vendor_code', lower(col('product_vendor_code')))
x = x.withColumn('product_code', lower(col('product_code')))
x = x.withColumn('vendor_code', lower(col('vendor_code')))
x = x.select('product_code', 'vendor_code'
             ,'product_type'
             ,'product_vendor_code'
        ,'Commitment_Duration_in_months'
        ,'Commitment_Duration_Value'
        ,'Billing_Frequency'
        ,'Consumption_Model').dropDuplicates()
# display(x)

# COMMAND ----------

# Perform the inner join
result = df_trans.join(
    x,
    (df_trans['SKUInternal'] == x['product_code']) & 
    (df_trans['vendor_code_computed'] == x['vendor_code']),
    how='inner'
)


# display(result)

# COMMAND ----------

df=spark.sql(
"""select 
  case when x.product_code is not null then 1 else 0 end  is_matched
, ''  matched_type
, x.product_vendor_code product_vendor_code_arr
, f.SKUInternal
, f.SKUMaster
, f.VendorNameInternal
, f.VendorNameMaster
, x.product_type ProductTypeMaster
, x.Commitment_Duration_in_months CommitmentDuration1Master
, x.Commitment_Duration_Value CommitmentDuration2Master
, x.Billing_Frequency BillingFrequencyMaster
, x.Consumption_Model ConsumptionModelMaster
, f.GroupEntityCode
, f.EntityCode
, f.DocumentNo
, f.TransactionDate
, f.SalesOrderDate
, f.SalesOrderID
, f.SalesOrderItemID
, f.Description
, f.Technology
, f.ProductTypeInternal
, f.VendorCode
, f.VendorGeography
, f.VendorStartDate
, f.ResellerCode
, f.ResellerNameInternal
, f.ResellerGeographyInternal
, f.ResellerStartDate
, f.ResellerGroupCode
, f.ResellerGroupName
, f.ResellerGroupStartDate
, f.EndCustomer
, f.IndustryVertical
, f.CurrencyCode
, f.RevenueAmount
, f.Period_FX_rate
, f.RevenueAmount_Euro
, f.GP1
, f.GP1_Euro
, f.COGS
, f.COGS_Euro
, f.GL_Group
, f.TopCostFlag
from platinum_dev.obt.globaltransactions f 
left join 
  (select distinct 
        lower(product_vendor_code) product_vendor_code
        ,lower(product_code) product_code
        ,lower(vendor_code)  vendor_code
        ,product_type
        ,Commitment_Duration_in_months
        ,Commitment_Duration_Value
        ,Billing_Frequency
        ,Consumption_Model
        from gold_dev.orion.link_product_to_vendor_arr
        where is_current=1 
  )x 
    on lower(x.product_code)= lower(SKUInternal) 
    --and lower(x.vendor_code)= case when VendorNameMaster ='NaN' then lower(VendorNameInternal) else lower(VendorNameMaster) end 
    and lower(x.vendor_code)=lower(VendorNameMaster)
    """)
    

# COMMAND ----------

df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.stage_trans")

# COMMAND ----------

from pyspark.sql.functions import col, lower, when, lit
df=spark.table(f"{catalog}.{schema}.stage_trans")
df = df.withColumn('computed_VendorNameInternal', lower(col('VendorNameInternal')))\
.withColumn('computed_SKUInternal', lower(col('SKUInternal')))

# Perform the left join with conditions
result = df.join(
    x,
    (df['computed_SKUInternal'] == x['product_code']) & 
    (df['computed_VendorNameInternal'] == x['vendor_code']) &
    (df['is_matched']==0),
    how='left'
)

# Define a dictionary to store column updates for `result` based on conditions
column_updates = {
    'is_matched': when(col('product_code').isNotNull(), lit(2)).otherwise(col('is_matched')),
    'matched_type': when(col('product_code').isNotNull(), 'SKUInternal').otherwise(col('matched_type')),
    'product_vendor_code_arr': when(col('product_code').isNotNull(), col('product_vendor_code')).otherwise(col('product_vendor_code_arr')),
    'ProductTypeMaster': when(col('product_code').isNotNull(), col('product_type')).otherwise(col('ProductTypeMaster')),
    'CommitmentDuration1Master': when(col('product_code').isNotNull(), col('Commitment_Duration_in_months')).otherwise(col('CommitmentDuration1Master')),
    'CommitmentDuration2Master': when(col('product_code').isNotNull(), col('Commitment_Duration_Value')).otherwise(col('CommitmentDuration2Master')),
    'BillingFrequencyMaster': when(col('product_code').isNotNull(), col('Billing_Frequency')).otherwise(col('BillingFrequencyMaster')),
    'ConsumptionModelMaster': when(col('product_code').isNotNull(), col('Consumption_Model')).otherwise(col('ConsumptionModelMaster'))
}


# Apply column updates
for col_name, update_expr in column_updates.items():
    result = result.withColumn(col_name, update_expr)

# Select and rename relevant columns in a single step
selected_columns = [
    'is_matched', 'matched_type','product_vendor_code_arr', 'VendorNameInternal', 'VendorNameMaster', 'SKUInternal', 'SKUMaster',
    'ProductTypeMaster', 'CommitmentDuration1Master', 'CommitmentDuration2Master', 'BillingFrequencyMaster', 
    'ConsumptionModelMaster','GroupEntityCode', 'EntityCode', 'DocumentNo', 'TransactionDate', 'SalesOrderDate', 'SalesOrderID', 
    'SalesOrderItemID', 'Description', 'Technology', 'ProductTypeInternal', 'VendorCode', 'VendorGeography', 
    'VendorStartDate', 'ResellerCode', 'ResellerNameInternal', 'ResellerGeographyInternal', 'ResellerStartDate', 
    'ResellerGroupCode', 'ResellerGroupName', 'ResellerGroupStartDate', 'EndCustomer', 'IndustryVertical', 
    'CurrencyCode', 'RevenueAmount', 'Period_FX_rate', 'RevenueAmount_Euro', 'GP1', 'GP1_Euro', 'COGS', 'COGS_Euro', 
    'GL_Group', 'TopCostFlag'
]
result = result.select(*selected_columns)

# Save the result as a table
result.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.stage_trans")

# COMMAND ----------

df=spark.table(f"{catalog}.{schema}.stage_trans")

display(df)

# COMMAND ----------

from pyspark.sql import Window

from pyspark.sql.functions import col, lower, when, levenshtein, lit, length, greatest, row_number
df = spark.table(f"{catalog}.{schema}.stage_trans")

# Prepare `df` DataFrame by lowercasing `computed_VendorNameMaster`
df = df.withColumn('computed_VendorNameMaster', lower(col('VendorNameMaster'))) \
       .withColumn('computed_SKUInternal', lower(col('SKUInternal')))

# Define initial decimal similarity threshold and number of iterations
initial_similarity_threshold = 0.9  # Adjust this as needed (0 to 1 scale)
iterations = 3

# Loop through each iteration with decreasing similarity threshold
for i in range(iterations):
    # Adjust similarity threshold for each iteration
    similarity_threshold = initial_similarity_threshold - (i * 0.1)
    
    # Calculate Levenshtein distance and normalized similarity
    result2 = df.alias('df').join(
        x.alias('x'),
        (col('df.computed_SKUInternal') == col('x.product_code')) & 
        ((1 - (levenshtein(col('df.computed_VendorNameMaster'), col('x.vendor_code')) / 
               greatest(length(col('df.computed_VendorNameMaster')), length(col('x.vendor_code'))))) >= similarity_threshold) &
        (col('df.is_matched') == 0) &    
        (col('df.computed_VendorNameMaster') != 'NaN'),
        how='left'
    ).withColumn(
        'levenshtein_distance',
        levenshtein(col('df.computed_VendorNameMaster'), col('x.vendor_code'))
    )

    # Define window by SKU to keep only the closest match
    window_spec = Window.partitionBy('df.SKUInternal').orderBy(col('levenshtein_distance'))

    # Add row number within each partition to rank by distance
    result2 = result2.withColumn('row_num', row_number().over(window_spec))

    # Filter to keep only the closest match
    result2 = result2.filter(col('row_num') == 1).drop('row_num', 'levenshtein_distance')

    # Define dictionary for column updates if a match is found
    column_updates = {
        'is_matched': when(col('x.product_code').isNotNull(), 3).otherwise(col('df.is_matched')),
        'matched_type': when(col('x.product_code').isNotNull(), lit(f'VendorNameMaster:{similarity_threshold:.1f}')).otherwise(col('df.matched_type')),
        'product_vendor_code_arr': when(col('x.product_code').isNotNull(), col('x.product_vendor_code')).otherwise(col('df.product_vendor_code_arr')),
        'ProductTypeMaster': when(col('x.product_code').isNotNull(), col('x.product_type')).otherwise(col('df.ProductTypeMaster')),
        'CommitmentDuration1Master': when(col('x.product_code').isNotNull(), col('x.Commitment_Duration_in_months')).otherwise(col('df.CommitmentDuration1Master')),
        'CommitmentDuration2Master': when(col('x.product_code').isNotNull(), col('x.Commitment_Duration_Value')).otherwise(col('df.CommitmentDuration2Master')),
        'BillingFrequencyMaster': when(col('x.product_code').isNotNull(), col('x.Billing_Frequency')).otherwise(col('df.BillingFrequencyMaster')),
        'ConsumptionModelMaster': when(col('x.product_code').isNotNull(), col('x.Consumption_Model')).otherwise(col('df.ConsumptionModelMaster'))
    }

    # Apply column updates for this iteration
    for col_name, update_expr in column_updates.items():
        result2 = result2.withColumn(col_name, update_expr)

    # Update `df` DataFrame with the new values from `result2` for the next iteration
    df = result2

# Select relevant columns to create the final output DataFrame
selected_columns = [
    'is_matched', 'matched_type', 'product_vendor_code_arr', 'VendorNameInternal', 'VendorNameMaster', 'SKUInternal', 'SKUMaster',
    'ProductTypeMaster', 'CommitmentDuration1Master', 'CommitmentDuration2Master', 'BillingFrequencyMaster', 
    'ConsumptionModelMaster', 'GroupEntityCode', 'EntityCode', 'DocumentNo', 'TransactionDate', 'SalesOrderDate', 'SalesOrderID', 
    'SalesOrderItemID', 'Description', 'Technology', 'ProductTypeInternal', 'VendorCode', 'VendorGeography', 
    'VendorStartDate', 'ResellerCode', 'ResellerNameInternal', 'ResellerGeographyInternal', 'ResellerStartDate', 
    'ResellerGroupCode', 'ResellerGroupName', 'ResellerGroupStartDate', 'EndCustomer', 'IndustryVertical', 
    'CurrencyCode', 'RevenueAmount', 'Period_FX_rate', 'RevenueAmount_Euro', 'GP1', 'GP1_Euro', 'COGS', 'COGS_Euro', 
    'GL_Group', 'TopCostFlag'
]

df = df.select(*selected_columns)

# Save the resulting DataFrame as a table
df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.stage_trans")


# COMMAND ----------

from pyspark.sql.functions import levenshtein, lit

# Example data for comparison
data = [("NaN", "Qnap")]
df = spark.createDataFrame(data, ["value1", "value2"])

# Compute Levenshtein distance between "NaN" and "Qnap"
df = df.withColumn("levenshtein_distance", levenshtein(lit("nan"), lit("qna-p")))

# Show the Levenshtein distance
df.select("levenshtein_distance").show()

# COMMAND ----------


df=spark.table(f"{catalog}.{schema}.stage_trans")

display(df.where(col('is_matched')==3))

# COMMAND ----------

# MAGIC %md
# MAGIC
