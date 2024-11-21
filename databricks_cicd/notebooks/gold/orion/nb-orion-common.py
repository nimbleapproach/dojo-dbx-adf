# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, coalesce, concat_ws, isnull
from typing import List, Optional
from functools import reduce

def compare_dataframes(
    df1: DataFrame, 
    df2: DataFrame, 
    composite_key: Optional[List[str]] = None,
    fields_to_compare: Optional[List[str]] = None,
    additional_fields: Optional[List[str]] = None
) -> DataFrame:
    """
    Compare two PySpark DataFrames and return differences and unmatched records.
    Excludes exact matches from the output.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        composite_key: List of columns that form the unique identifier
        fields_to_compare: List of columns to compare between DataFrames
        additional_fields: Additional fields to include in the output
    """
    # Default values if not provided
    composite_key = composite_key or [
        'Id'
    ]
    
    fields_to_compare = fields_to_compare or [
        'ProductTypeMaster', 'CommitmentDuration1Master', 'CommitmentDuration2Master', 
        'BillingFrequencyMaster', 'ConsumptionModelMaster'
    ]
    
    additional_fields = additional_fields or [
        'is_matched', 'matched_type', 'GroupEntityCode', 'EntityCode', 'CurrencyCode',
        'RevenueAmount', 'Period_FX_rate', 'RevenueAmount_Euro'
        ,'DocumentNo', 'TransactionDate', 'SKUInternal', 'VendorNameInternal', 
        'ResellerCode', 'EndCustomer', 'GL_Group', 'SalesOrderDate'
    ]
    
    def prepare_df_for_join(df: DataFrame, alias: str, null_placeholder: str) -> DataFrame:
        df = df.alias(alias)
        for key in composite_key:
            df = df.withColumn(
                f"{key}_join",
                when(col(f"{alias}.{key}").isNull(), lit(null_placeholder))
                .otherwise(col(f"{alias}.{key}").cast("string"))
            )
        # Fill nulls in fields to compare as well
        for field in fields_to_compare:
            df = df.withColumn(
                field,
                when(col(f"{alias}.{field}").isNull(), lit(null_placeholder))
                .otherwise(col(f"{alias}.{field}")).cast("string")
            )
        return df
    
    df1 = prepare_df_for_join(df1, 'df1', "___NULL___")
    df2 = prepare_df_for_join(df2, 'df2', "___NULL___")

    # Validate input DataFrames
    for field in composite_key + fields_to_compare:
        if field not in df1.columns or field not in df2.columns:
            raise ValueError(f"Column {field} not found in one or both DataFrames")
    
    # Alias the dataframes
    df1 = df1.alias('df1')
    df2 = df2.alias('df2')
    
    # Create the join condition
    join_condition = reduce(lambda x, y: x & y, [
        col(f'df1.{key}') == col(f'df2.{key}')
        for key in composite_key
    ])
    
    # Full outer join to get all records
    joined_df = df1.join(df2, join_condition, 'full_outer')
    
    # Create value difference condition
    has_differences = reduce(lambda x, y: x | y, [
        (col(f'df1.{field}').isNotNull() & 
         col(f'df2.{field}').isNotNull() & 
         (col(f'df1.{field}') != col(f'df2.{field}'))
        ) for field in fields_to_compare
    ])
    
    # Create comparison status column
    status_column = when(
        col(f'df1.{composite_key[0]}').isNull(),
        lit('ONLY_IN_DF2')
    ).when(
        col(f'df2.{composite_key[0]}').isNull(),
        lit('ONLY_IN_DF1')
    ).when(
        has_differences,
        lit('DIFFERENT_VALUES')
    ).otherwise(
        lit('EXACT_MATCH')
    ).alias('comparison_status')
    
    # Create the columns for differences
    diff_columns = []
    for field in fields_to_compare:
        # Only include the difference columns when there are actual differences
        diff_columns.extend([
            when(
                (col(f'df1.{field}').isNotNull() & 
                 col(f'df2.{field}').isNotNull() & 
                 (col(f'df1.{field}') != col(f'df2.{field}'))
                ),
                concat_ws(' -> ', 
                    coalesce(col(f'df1.{field}'), lit('')),
                    coalesce(col(f'df2.{field}'), lit(''))
                )
            ).otherwise(
                when(col(f'df1.{field}').isNull() & col(f'df2.{field}').isNotNull(),
                    concat_ws(' -> ', lit('NULL'), col(f'df2.{field}'))
                ).when(col(f'df1.{field}').isNotNull() & col(f'df2.{field}').isNull(),
                    concat_ws(' -> ', col(f'df1.{field}'), lit('NULL'))
                ).otherwise(None)
            ).alias(f'{field}_changes')
        ])
    
    # Select columns for final output
    selected_columns = [
        status_column,
        *[coalesce(col(f'df1.{key}'), col(f'df2.{key}')).alias(key) for key in composite_key],
        *diff_columns
    ]
    
    # Add additional fields if they exist
    for field in additional_fields:
        if field in df1.columns:
            selected_columns.append(
                coalesce(col(f'df2.{field}'), col(f'df1.{field}')).alias(field)
            )
    
    # Create final DataFrame and filter out exact matches
    result_df = (joined_df
                .select(*selected_columns)
                .filter(col('comparison_status') != 'EXACT_MATCH'))
    
    # Add summary statistics
    total_records = result_df.count()
    stats = result_df.groupBy('comparison_status').count()
    
    print("\nComparison Summary (Excluding Exact Matches):")
    stats.show()
    print(f"Total Records with Differences: {total_records}")
    
    return result_df

# COMMAND ----------

def case_insensitive_regex(pattern):
    return f"(?i){pattern}"

# COMMAND ----------


# For multiple column renames, here's an alternative function:
def rename_columns(df, columns_dict):
    """
    Rename multiple columns in a PySpark DataFrame using a dictionary
    
    Parameters:
    df (DataFrame): Input PySpark DataFrame
    columns_dict (dict): Dictionary with current names as keys and new names as values
    
    Returns:
    DataFrame: DataFrame with renamed columns
    """
    for old_name, new_name in columns_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

# COMMAND ----------

# Function to clean column names
def clean_column_names(df):
    # Create a mapping of old to new column names
    # 1. Convert to lowercase
    # 2. Replace special characters with underscore
    # 3. Remove leading and trailing underscores
    # 4. Replace multiple consecutive underscores with single underscore
    new_columns = [
        re.sub(r'^_+|_+$', '', # Remove leading/trailing underscores
               re.sub(r'_+', '_', # Replace multiple underscores with single
                     re.sub(r"[^a-zA-Z0-9_]", "_", col).lower() # Original cleaning
               )
        ) 
        for col in df.columns
    ]
    
    # Apply the new column names to the DataFrame
    return df.toDF(*new_columns)
