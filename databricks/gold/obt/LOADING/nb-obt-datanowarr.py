# Databricks notebook source
df_dn = spark.sql(
    """select 
    *
    from silver_dev.masterdata.datanowarr
    where Sys_Silver_IsCurrent=1
  """
)

# COMMAND ----------

from pyspark.sql.functions import *
dup_sku = df_dn.groupBy("SKU").agg(count("SKU").alias("num_SKU")).filter('num_SKU >1')

# COMMAND ----------

dup_sku_list = [s[0] for s in dup_sku.select('SKU').collect()]


# COMMAND ----------

dup_sku = (df_dn.filter(col("SKU").isin(dup_sku_list))
 .filter(col('Vendor_Name')!='Other vendors'))

# COMMAND ----------

df_non_dup_sku=(df_dn.filter(~col("SKU").isin(dup_sku_list)))

# COMMAND ----------

# dup_sku.agg({"SKU": "max"}).collect()[0]
df_dup_sku = (dup_sku.groupBy("SKU").agg(max('Commitment_Duration_in_months').alias('Commitment_Duration_in_months'),
                            max('Commitment_Duration2').alias('Commitment_Duration2'),
                            max('Billing_Frequency').alias('Billing_Frequency'),
                            max('Billing_Frequency2').alias('Billing_Frequency2'),
                            max('Consumption_Model').alias('Consumption_Model'),
                            max('Product_Type').alias('Product_Type'),
                            max('Mapping_Type_Duration').alias('Mapping_Type_Duration'),
                            max('Mapping_Type_Billing').alias('Mapping_Type_Billing'),
                            max('MRR_Ratio').alias('MRR_Ratio'),
                            max('Commitment_Duration_Value').alias('Commitment_Duration_Value'),
                            max('Billing_Frequency_Value2').alias('Billing_Frequency_Value2'),
                            max('MRR').alias('MRR'),
                            max('Vendor_Name').alias('Vendor_Name')
                           ))

# COMMAND ----------

cols = df_dup_sku.columns

df_result  = df_non_dup_sku.select(cols).union(df_dup_sku.select(cols))
df_result.createOrReplaceTempView('df')

# COMMAND ----------

spark.sql("""create or replace table gold_dev.obt.datanowarr
                    as
                    select * from df""")
