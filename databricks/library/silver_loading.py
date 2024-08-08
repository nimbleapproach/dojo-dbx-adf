"""Reusable functions for loading in silver.

   This may be temporary home only until patterns are observed and better strategy is found.
   None of the below functions should rely on specific names, e.g. field names,
   unless they are shared across all silver layer.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, lit


def get_select_from_column_map(column_map: dict[str, str|None]) -> list[Column]:
    """Converts map of columns to list for select function argument.

       The keys are target columns, the values are either source columns, or None. 
       NOTE: The columns with None are assumed to be string type and will be converted
       to literal string value 'NaN'.  

    Args:
        column_map (dict[str, str|None]): the dictionary containing column map.

    Returns:
        list[Column]: Argument for select function.
    """
    cols_select = []
    for target_col in column_map:
        source_col = column_map[target_col]
        cols_select.append(col(source_col).alias(target_col) if source_col else lit('NaN').alias(target_col))
    return cols_select


def get_merge_condition_expr(keys_to_merge_on: list[str],
                                        source_alias: str ="source",
                                        target_alias: str="target") -> str:
    return " AND ".join([f"{source_alias}.{bk} = {target_alias}.{bk}" for bk in keys_to_merge_on])


def _make_aliased_column_map(columns, source_alias="source", target_alias="target"):
    return {f"{target_alias}.{c}":f"{source_alias}.{c}" for c in columns}

def get_insert_update_maps_from_columns(columns: list[str], 
                                        sys_insert_col_name: str,
                                        source_alias: str ="source",
                                        target_alias: str="target") -> tuple[dict,dict]:
    insert_cols = columns[:]
    insert_map = _make_aliased_column_map(insert_cols, source_alias, target_alias)
    update_cols = [c for c in insert_cols if c != sys_insert_col_name]
    update_map =  _make_aliased_column_map(update_cols, source_alias, target_alias)
    return insert_map, update_map


def get_current_silver_rows(silver_source: DataFrame, 
                            sys_is_current_col_name: str="Sys_Silver_IsCurrent"
                            ) -> DataFrame:
    # common condition to include only current records from source
    return silver_source.where(col(sys_is_current_col_name))