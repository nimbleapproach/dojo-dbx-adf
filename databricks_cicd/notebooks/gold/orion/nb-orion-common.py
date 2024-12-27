# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, coalesce, concat_ws, abs
from typing import List, Optional, Dict
from functools import reduce


def compare_dataframes(
    df1: DataFrame,
    df2: DataFrame,
    composite_key: Optional[List[str]] = None,
    fields_to_compare: Optional[List[str]] = None,
    additional_fields: Optional[List[str]] = None,
    numeric_tolerance: Optional[Dict[str, float]] = None,
) -> DataFrame:
    """
    Compare two PySpark DataFrames and return differences and unmatched records.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        composite_key: List of columns that form the unique identifier
        fields_to_compare: List of columns to compare between DataFrames
        additional_fields: Additional fields to include in the output
        numeric_tolerance: Dictionary of field names and their tolerance thresholds
                         e.g., {'RevenueAmount': 0.05} for 5% tolerance
    """
    # Default values if not provided
    composite_key = composite_key or ["Id"]
    fields_to_compare = fields_to_compare or [
        "ProductTypeMaster",
        "CommitmentDuration1Master",
        "CommitmentDuration2Master",
        "BillingFrequencyMaster",
        "ConsumptionModelMaster",
    ]

    numeric_tolerance = numeric_tolerance or {}

    def prepare_df_for_join(df: DataFrame, alias: str, null_placeholder: str) -> DataFrame:
        # Alias the DataFrame
        df = df.alias(alias)
        
        # First create individual join columns
        for key in composite_key:
            df = df.withColumn(
                f"{key}_join",
                when(col(f"{alias}.{key}").isNull(), lit(null_placeholder))
                .otherwise(col(f"{alias}.{key}").cast("string"))
            )
        
        # Then create composite join key from the _join columns
        join_columns = [col(f"{key}_join") for key in composite_key]
        df = df.withColumn("composite_join_key", concat_ws("||", *join_columns))
        
        # Handle fields to compare
        for field in fields_to_compare:
            if field in numeric_tolerance:
                df = df.withColumn(field, col(f"{alias}.{field}").cast("double"))
            else:
                df = df.withColumn(
                    field,
                    when(col(f"{alias}.{field}").isNull(), lit(null_placeholder))
                    .otherwise(col(f"{alias}.{field}"))
                    .cast("string")
                )
        return df


    df1 = prepare_df_for_join(df1, "df1", "___NULL___")
    df2 = prepare_df_for_join(df2, "df2", "___NULL___")

    # Alias the dataframes
    df1 = df1.alias("df1")
    df2 = df2.alias("df2")

    # Create the join condition
    join_condition = col("df1.composite_join_key") == col("df2.composite_join_key")

    # Full outer join
    joined_df = df1.join(df2, join_condition, "full_outer")

    # Create value difference condition with tolerance for numeric fields
    difference_conditions = []
    for field in fields_to_compare:
        if field in numeric_tolerance:
            # For numeric fields, use relative difference comparison
            tolerance = numeric_tolerance[field]
            diff_condition = (
                col(f"df1.{field}").isNotNull()
                & col(f"df2.{field}").isNotNull()
                & (
                    abs(col(f"df1.{field}") - col(f"df2.{field}"))
                    / abs(col(f"df1.{field}"))
                    > tolerance
                )
            )
        else:
            # For non-numeric fields, use exact comparison
            diff_condition = (
                col(f"df1.{field}").isNotNull()
                & col(f"df2.{field}").isNotNull()
                & (col(f"df1.{field}") != col(f"df2.{field}"))
            )
        difference_conditions.append(diff_condition)

    has_differences = reduce(lambda x, y: x | y, difference_conditions)

    # Create comparison status column
    status_column = when(
        col("df1.composite_join_key").isNull(),
        lit('ONLY_IN_DF2')
    ).when(
        col("df2.composite_join_key").isNull(),
        lit('ONLY_IN_DF1')
    ).when(
        has_differences,
        lit('DIFFERENT_VALUES')
    ).otherwise(
        lit('EXACT_MATCH')
    ).alias('comparison_status')

    # Create the columns for differences with tolerance handling
    diff_columns = []
    for field in fields_to_compare:
        if field in numeric_tolerance:
            tolerance = numeric_tolerance[field]
            diff_columns.append(
                when(
                    (
                        col(f"df1.{field}").isNotNull()
                        & col(f"df2.{field}").isNotNull()
                        & (
                            abs(col(f"df1.{field}") - col(f"df2.{field}"))
                            / abs(col(f"df1.{field}"))
                            > tolerance
                        )
                    ),
                    concat_ws(
                        " -> ",
                        coalesce(col(f"df1.{field}"), lit("")).cast("string"),
                        coalesce(col(f"df2.{field}"), lit("")).cast("string"),
                        lit(f"(diff: {(tolerance*100)}%)"),  # Add tolerance percentage
                    ),
                )
                .when(
                    (col(f"df1.{field}").isNotNull() & col(f"df2.{field}").isNotNull()),
                    concat_ws(
                        " ≈ ",
                        coalesce(col(f"df1.{field}"), lit("")).cast("string"),
                        coalesce(col(f"df2.{field}"), lit("")).cast("string"),
                        lit(f"(within {(tolerance*100)}%)"),  # Add tolerance percentage
                    ),
                )
                .otherwise(
                    when(
                        col(f"df1.{field}").isNull() & col(f"df2.{field}").isNotNull(),
                        concat_ws(
                            " -> ", lit("NULL"), col(f"df2.{field}").cast("string")
                        ),
                    )
                    .when(
                        col(f"df1.{field}").isNotNull() & col(f"df2.{field}").isNull(),
                        concat_ws(
                            " -> ", col(f"df1.{field}").cast("string"), lit("NULL")
                        ),
                    )
                    .otherwise(None)
                )
                .alias(f"{field}_changes")
            )
        else:
            # Original non-numeric comparison logic
            diff_columns.append(
                when(
                    (
                        col(f"df1.{field}").isNotNull()
                        & col(f"df2.{field}").isNotNull()
                        & (col(f"df1.{field}") != col(f"df2.{field}"))
                    ),
                    concat_ws(
                        " -> ",
                        coalesce(col(f"df1.{field}"), lit("")),
                        coalesce(col(f"df2.{field}"), lit("")),
                    ),
                )
                .when(
                    (
                        col(f"df1.{field}").isNotNull()
                        & col(f"df2.{field}").isNotNull()
                        & (col(f"df1.{field}") == col(f"df2.{field}"))
                    ),
                    concat_ws(
                        " == ",
                        coalesce(col(f"df1.{field}"), lit("")),
                        coalesce(col(f"df2.{field}"), lit("")),
                    ),
                )
                .otherwise(
                    when(
                        col(f"df1.{field}").isNull() & col(f"df2.{field}").isNotNull(),
                        concat_ws(" -> ", lit("NULL"), col(f"df2.{field}")),
                    )
                    .when(
                        col(f"df1.{field}").isNotNull() & col(f"df2.{field}").isNull(),
                        concat_ws(" -> ", col(f"df1.{field}"), lit("NULL")),
                    )
                    .otherwise(None)
                )
                .alias(f"{field}_changes")
            )

    # Select columns for final output
    selected_columns = [
    status_column,
    *[coalesce(col(f"df1.{key}"), col(f"df2.{key}")).alias(key) for key in composite_key],
    *diff_columns,
    col("df1.composite_join_key").alias("df1_join_key"),  # Add df1's join key
    col("df2.composite_join_key").alias("df2_join_key")  # Add df2's join key
    ]

    # Add tolerance columns for numeric fields
    for field in fields_to_compare:
        if field in numeric_tolerance:
            # Add actual difference percentage
            selected_columns.append(
                when(
                    (col(f"df1.{field}").isNotNull() & col(f"df2.{field}").isNotNull()),
                    abs(col(f"df1.{field}") - col(f"df2.{field}"))
                    / abs(col(f"df1.{field}"))
                    * 100,
                )
                .otherwise(lit(None))
                .alias(f"{field}_difference_pct")
            )
            # Add tolerance threshold
            selected_columns.append(
                lit(numeric_tolerance[field] * 100).alias(f"{field}_tolerance_pct")
            )

    # Add additional fields
    for field in additional_fields or []:
        if field in df1.columns:
            selected_columns.append(
                coalesce(col(f"df2.{field}"), col(f"df1.{field}")).alias(field)
            )

    # Create final DataFrame and filter out exact matches
    result_df = (
        joined_df.select(*selected_columns)
        # .filter(col('comparison_status') != 'EXACT_MATCH')
    )

    # Add summary statistics
    total_records = result_df.count()
    stats = result_df.groupBy("comparison_status").count()

    print("\nComparison Summary (Excluding Exact Matches):")
    stats.show()
    print(f"Total Records with Differences: {total_records}")

    return result_df

# COMMAND ----------

def get_primary_key_from_df(df):
    """
    Extract primary key information from a DataFrame's metadata
    Returns the primary key column name(s) if found, None otherwise
    """
    try:
        # Get the table's metadata if available
        catalog = df.sparkSession.catalog
        full_table_name = df._jdf.sparkSession().catalog().currentDatabase() + "." + df._jdf.queryExecution().analyzed().tableName()
        table = catalog.get(full_table_name)
        
        # Look for primary key in constraints/properties
        for prop in table.properties:
            if 'primary_key' in prop.lower():
                return table.properties[prop].strip('`() ').split(',')[0]
    except:
        pass
    
    # If no primary key found in metadata, look for identity columns
    for field in df.schema.fields:
        if 'identity' in str(field).lower() or 'pk' in field.name.lower():
            return field.name
            
    return None


# COMMAND ----------


def map_parent_child_keys(
    df,
    key_cols,
    order_cols=None,
    priority_col=None,
    priority_map=None
):
    """
    Generic function to map parent-child relationships in any DataFrame
    
    Parameters:
    df: DataFrame to process
    key_cols: List of columns that form the business key
    order_cols: Optional columns for ordering within groups
    priority_col: Column used for priority-based ordering
    priority_map: Dictionary mapping priority values to numeric priorities
    """
    # Get actual columns from DataFrame
    df_columns = df.columns
    
    # Filter key_cols to only include existing columns
    key_cols = [col for col in key_cols if col in df_columns]
    if not key_cols:
        raise ValueError("No valid key columns provided")
    
    # Try to get primary key, fall back to first key_col if none found
    id_column = get_primary_key_from_df(df) or key_cols[0]
    
    # Handle priority column and mapping
    if priority_col and priority_col in df_columns and priority_map:
        df = df.withColumn(
            "priority",
            F.expr(
                "CASE " +
                " ".join([f"WHEN lower({priority_col}) LIKE '%{k.lower()}%' THEN {v}" 
                         for k, v in priority_map.items()]) +
                " ELSE 999 END"
            )
        )
    else:
        df = df.withColumn("priority", F.lit(999))
    
    # Use order_cols if provided, otherwise use key_cols
    order_cols = [col for col in (order_cols or key_cols) if col in df_columns]
    
    # Define window specifications
    cnt_window = Window.partitionBy(*key_cols)
    row_window = Window.partitionBy(*key_cols).orderBy(F.col("priority"), *order_cols)
    
    # Create result DataFrame
    result_df = df \
        .withColumn("occurrence", F.count("*").over(cnt_window)) \
        .withColumn("row_number", F.row_number().over(row_window)) \
        .withColumn("is_parent", F.col("row_number") == 1) \
        .withColumn("group_id", F.col(id_column)) \
        .withColumn(
            "parent_id",
            F.when(F.col("row_number") == 1, None)
            .otherwise(F.first(id_column).over(cnt_window))
        )
    
    # Calculate summary statistics
    total_records = result_df.count()
    num_parents = result_df.filter(F.col("is_parent")).count()
    num_children = result_df.filter(~F.col("is_parent")).count()
    
    # Print summary
    print(f"\nParent-Child Mapping Summary:")
    print(f"Total Records: {total_records}")
    print(f"Parent Records: {num_parents}")
    print(f"Child Records: {num_children}")
    print(f"Key Columns: {key_cols}")
    print(f"ID Column used: {id_column}")
    
    return result_df

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def check_duplicate_keys(
    df, 
    key_cols=["Manufacturer Item No_", "Consolidated Vendor Name", "sys_databasename"], 
    order_cols=None, 
    priority_col="sys_databasename",
    priority_map = {
        "ReportsUK": 1,
        "ReportsDE": 2,
        "ReportsCH": 3,
        "ReportsNO": 4,
        "ReportsDN": 5,
        "ReportsSE": 6,
        "ReportsFI": 7,
        "ReportsNL": 8,
        "ReportsBE": 9,
        "ReportsFR": 10,
        "ReportsDK": 11
    }
):
    """
    Check for duplicate composite keys and provide detailed analysis with priority-based ordering.
    
    Parameters:
    df: DataFrame to check
    key_cols: List of column names that form the composite key
    order_cols: List of column names for ordering. If None, uses key_cols
    priority_col: Column to assign priorities
    priority_map: Dictionary mapping values of the priority column to their priorities
    """
    # Get actual columns from DataFrame
    df_columns = df.columns
    
    # Filter key_cols to only include columns that exist in the DataFrame
    key_cols = [col for col in key_cols if col in df_columns]
    
    # Adjust priority handling based on whether sys_databasename exists
    if priority_col not in df_columns:
        df = df.withColumn("priority", F.lit(999))
    else:
        df = df.withColumn(
            "priority",
            F.expr(
                "CASE " +
                " ".join([f"WHEN lower({priority_col}) LIKE '%{k.lower()}%' THEN {v}" for k, v in priority_map.items()]) +
                " ELSE 999 END"
            )
        )
    
    # Use order_cols if provided, otherwise fall back to key_cols
    order_cols = order_cols if order_cols is not None else key_cols
    # Filter order_cols to only include columns that exist
    order_cols = [col for col in order_cols if col in df_columns]
    
    # Get total record count
    total_records = df.count()
    
    # Define window specs
    cnt_window = Window.partitionBy(*key_cols)
    row_window = Window.partitionBy(*key_cols).orderBy(F.col("priority"), *order_cols)
    
    # Add occurrence and row_number using window functions
    occurrence_with_details = df \
        .withColumn("occurrence", F.count("*").over(cnt_window)) \
        .withColumn("row_number", F.row_number().over(row_window)) \
        .orderBy(F.col("priority"), *order_cols, "row_number")
    
    # Filter for duplicates only
    dupes_with_details = occurrence_with_details.filter(F.col("occurrence") > 1)
    
    # Get summary statistics
    num_unique_keys = df.select(*key_cols).distinct().count()
    num_duplicate_keys = dupes_with_details.select(*key_cols).distinct().count()
    
    # Print summary
    print(f"\nDuplicate Key Analysis:")
    print(f"Total Records: {total_records}")
    print(f"Unique Keys: {num_unique_keys}")
    print(f"Keys with Duplicates: {num_duplicate_keys}")
    print(f"Columns used as keys: {key_cols}")
    
    if num_duplicate_keys > 0:
        # Show distribution of duplicates
        print("\nDistribution of Duplicates:")
        dupe_distribution = dupes_with_details \
            .select("occurrence") \
            .filter(F.col("row_number") == 1) \
            .withColumn("frequency", F.count("*").over(Window.partitionBy("occurrence"))) \
            .distinct() \
            .orderBy("occurrence")
            
        print("\nDetailed Duplicate Records:")
        
    return occurrence_with_details

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, coalesce, concat_ws, isnull
from typing import List, Optional
from functools import reduce

def compare_dataframes_v1(
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
            ).when(
                (col(f'df1.{field}').isNotNull() & 
                 col(f'df2.{field}').isNotNull() & 
                 (col(f'df1.{field}') == col(f'df2.{field}'))
                ),
                concat_ws(' == ', 
                    coalesce(col(f'df1.{field}'), lit('')),
                    coalesce(col(f'df2.{field}'), lit(''))
                )
            ).otherwise(
                when(col(f'df1.{field}').isNull() 
                     & col(f'df2.{field}').isNotNull(),
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
                #.filter(col('comparison_status') != 'EXACT_MATCH')
                )
    
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

# COMMAND ----------

# Method 1: Simple optimization with ZORDER
def optimize_arr_table(catalog, schema, zorder_cols=None):
    """
    Apply optimization to pierre_arr_x table
    """
    if zorder_cols:
        zorder_by = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {catalog}.{schema}.pierre_arr_x ZORDER BY ({zorder_by})")
    else:
        spark.sql(f"OPTIMIZE {catalog}.{schema}.pierre_arr_x")


# COMMAND ----------

# Alternative version with more options:
def optimize_df_with_options(
    df, 
    zorder_columns, 
    partition_columns=None,
    table_name=None,
    write_mode="overwrite",
    cleanup=True
):
    """
    Optimize DataFrame with ZORDER and additional options
    
    Parameters:
    df: DataFrame to optimize
    zorder_columns: List of columns to ZORDER by
    partition_columns: Optional list of partition columns
    table_name: Optional temporary table name
    write_mode: Write mode for the DataFrame
    cleanup: Whether to clean up temporary table
    
    Returns:
    DataFrame: Optimized DataFrame
    """
    # Generate temporary table name if not provided
    if table_name is None:
        table_name = f"temp_zorder_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Initialize writer
    writer = df.write.format("delta").mode(write_mode)
    
    # Add partitioning if specified
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    
    # Write the table
    writer.saveAsTable(table_name)
    
    # Apply ZORDER optimization
    zorder_cols = ", ".join(zorder_columns)
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
    
    # Read back the optimized DataFrame
    optimized_df = spark.table(table_name)
    
    # Clean up if requested
    if cleanup:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    return optimized_df

