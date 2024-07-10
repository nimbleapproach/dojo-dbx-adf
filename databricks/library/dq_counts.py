from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_replace


def count_missing_string_values(df: DataFrame, column_name: str) -> int:
    """ Return count of null or empty or whitespace. """
    
    # handle missing column, type mismatch if needed
    return (df.where((col(column_name).isNull())
                     | (regexp_replace(col(column_name), r'\s', lit('')) == lit('')))
                     .count())

