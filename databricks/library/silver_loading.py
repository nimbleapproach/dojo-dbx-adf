from typing import Dict, List, Union
from pyspark.sql import Column
from pyspark.sql.functions import col, lit

def get_select_from_column_map(column_map: Dict[str, Union[str, None]]) -> List[Column]:
    """Converts map of columns to list for select function argument.

       The keys are target columns, the values are either source columns, or None. 
       NOTE: The columns with None are assumed to be string type and will be converted
       to literal string value 'NaN'.  

    Args:
        column_map (Dict[str, Union[str, None]]): the dictionary containing column map.

    Returns:
        List[Column]: Argument for select function.
    """
    cols_select = []
    for target_col in column_map:
        source_col = column_map[target_col]
        cols_select.append(col(source_col).alias(target_col) if source_col else lit('NaN').alias(target_col))
    return cols_select