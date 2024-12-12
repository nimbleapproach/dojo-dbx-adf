# Databricks notebook source
# 
# Notebook to setup common variables and functions used by other notebooks
# which are ORION COMMON ONLY - such as a dict of dimension names and tables
# dict of fact names and tables etc.
#
import os
import json
from pyspark.sql.functions import levenshtein, regexp_extract, col, row_number, lit
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing.pool import ThreadPool
from pyspark.sql import Window
from delta.tables import DeltaTable
from collections import defaultdict

# set up some constants
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
catalog = f"gold_{ENVIRONMENT}"
orion_schema = "orion"
# this is number of notebooks to run in parallel
parallel_max = 4
processing_notebook = "nb-orion-process-dimension-v2"


def read_and_replace_json(file_path, replacements):
    """
    Reads a JSON file, replaces placeholders with specified values,
    and returns the updated data as a dictionary.
    
    :param file_path: Path to the JSON file
    :param replacements: Dictionary of placeholders and their replacement values
    :return: Dictionary containing the updated JSON data
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        def replace_placeholders(obj):
            if isinstance(obj, dict):
                return {k: replace_placeholders(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_placeholders(i) for i in obj]
            elif isinstance(obj, str):
                for placeholder, value in replacements.items():
                    obj = obj.replace(f"{{{placeholder}}}", value)
                return obj
            return obj
        
        updated_data = replace_placeholders(data)
        return updated_data
    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from the file at {file_path}.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
file_path = 'meta.json'
replacements = {
    "processing_notebook": processing_notebook,
    "ENVIRONMENT": ENVIRONMENT,
    "orion_schema": orion_schema
}
data = read_and_replace_json(file_path, replacements)
#print(data)



# COMMAND ----------

def check_type_run_group_consistency(data):
    # Mappings to track type-to-run_group and run_group-to-type relationships
    type_to_run_group = defaultdict(set)  # Allows multiple run_groups for a type
    run_group_to_types = defaultdict(set)  # Allows multiple types in a run_group
    
    # List to hold inconsistencies
    inconsistencies = []
    
    for entity, attributes in data.items():
        entity_type = attributes.get('type')
        run_group = attributes.get('run_group')
        
        if entity_type is not None and run_group is not None:
            # Update the mappings to track the type and run_group associations
            type_to_run_group[entity_type].add(run_group)
            run_group_to_types[run_group].add(entity_type)
    
    # Check for inconsistencies, except where dim and link coexist in the same run_group
    for entity_type, run_groups in type_to_run_group.items():
        # Allow multiple run_groups only for 'dim' and 'link'
        if entity_type not in {'dim', 'link'} and len(run_groups) > 1:
            inconsistencies.append(f"Type '{entity_type}' is assigned to multiple run_groups: {run_groups}")

    for run_group, types in run_group_to_types.items():
        # Allow multiple types in the same run_group only if it's 'dim' and 'link'
        if len(types) > 1 and not (types == {'dim', 'link'}):
            inconsistencies.append(f"Run_group {run_group} has multiple types: {types}")

    # Output the inconsistencies if any
    if inconsistencies:
        print("Inconsistencies found:")
        for inc in inconsistencies:
            print(f"- {inc}")
    else:
        print("No inconsistencies found. Each type is assigned to exactly one run_group and vice versa, except for 'dim' and 'link'.")

    return len(inconsistencies) == 0  # Returns True if no inconsistencies were found


# COMMAND ----------

def filter_and_sort_json(data, *filters):
    """
    Filters the JSON data based on the given filters and sorts the result by 'run_order'.
    Filters also ensure that only 'active=1' objects are included.

    Parameters:
    - data: dictionary containing the JSON data.
    - filters: multiple filters in the format 'attribute:value' (e.g., 'type:core').

    Returns:
    - A list of filtered and sorted objects, or a message if no matches are found.
    """
    
    # Parse filters
    filter_dict = {}
    for f in filters:
        attr, value = f.split(":")
        # Store the attribute with value as int if it's a digit
        filter_dict[attr] = value if not value.isdigit() else int(value)
    
    # Filter the data
    filtered_data = {
        key: value for key, value in data.items() 
        if all(str(value.get(attr)) == str(filter_dict[attr]) for attr in filter_dict)  and value.get("active") == 1
    }
    
    # Check if we are filtering by type
    if 'type' in filter_dict:
        type_filter = filter_dict['type']
        
        # Get all run_groups for the specified type
        run_groups_for_type = [value.get('run_group') for value in data.values() if value.get('type') == type_filter]

        # If no valid data is found for the specified type and run_group
        if not filtered_data and run_groups_for_type:
            min_run_group = min(run_groups_for_type)
            max_run_group = max(run_groups_for_type)
            return (f"No entries found for type '{type_filter}' and the specified filters. "
                    f"Minimum run_group for this type is: {min_run_group}, "
                    f"Maximum run_group for this type is: {max_run_group}")

    # Sort the filtered data by 'run_order', defaulting to 0 if it doesn't exist
    sorted_data = sorted(
        filtered_data.items(), 
        key=lambda item: item[1].get('run_order', 0)
    )
    
    return sorted_data

# COMMAND ----------

def filter_by_type(data, object_type):
    """
    Filters and returns all objects that match the specified type.

    Parameters:
    - data: dictionary containing the JSON data
    - object_type: string representing the type to filter on (e.g., 'core', 'dim')

    Returns:
    - Dictionary of filtered objects that match the specified type or a message if no matches are found.
    """
    # Filter the data based on the type
    filtered_objects = {key: value for key, value in data.items() if value.get("type") == object_type and value.get("active") == 1}
    
    if filtered_objects:
        return filtered_objects
    else:
        return f"No objects found with type '{object_type}'."



# COMMAND ----------


# Function to get details of an object from the data
def get_object_detail(data, attribute):
    """
    Returns the details of the given object (attribute) from the data.
    
    Parameters:
    - data: dictionary containing the JSON data
    - attribute: string representing the object name (e.g., 'source_system')
    
    Returns:
    - Dictionary of details for the specified attribute or a message if the attribute does not exist.
    """
    if attribute in data:
        return data[attribute]
    else:
        return f"Object '{attribute}' not found in the data."

# COMMAND ----------


def count_run_groups_by_type(data):
    type_run_groups = defaultdict(set)
    
    for entity, attributes in data.items():
        entity_type = attributes.get('type')
        run_group = attributes.get('run_group')
        
        if entity_type is not None and run_group is not None:
            type_run_groups[entity_type].add(run_group)
    
    result = {
        entity_type: len(run_groups)
        for entity_type, run_groups in type_run_groups.items()
    }
    
    return result

# COMMAND ----------


def count_objects_by_type(data):
    type_counts = defaultdict(int)
    
    for entity, attributes in data.items():
        entity_type = attributes.get('type')
        
        if entity_type is not None:
            type_counts[entity_type] += 1
    
    return dict(type_counts)

# COMMAND ----------

from collections import defaultdict

def count_objects_by_type_run_group_and_priority(data):
    type_group_priority_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    for entity, attributes in data.items():
        entity_type = attributes.get('type')
        run_group = attributes.get('run_group')
        priority = attributes.get('run_order')  # Note: using 'run_order' as in the original data
        
        if all(v is not None for v in (entity_type, run_group, priority)):
            type_group_priority_counts[entity_type][run_group][priority] += 1
    
    return dict(type_group_priority_counts)



# COMMAND ----------


def summarise_execution_order_with_layers(data):
    # Collect the relevant information from the input data
    execution_order = defaultdict(lambda: defaultdict(list))
    
    for entity, attributes in data.items():
        entity_type = attributes.get('type')
        run_group = attributes.get('run_group')
        run_order = attributes.get('run_order')
        
        if all(v is not None for v in (entity_type, run_group, run_order)):
            # Group `dim` and `link` types together in execution_order
            if entity_type in ["dim", "link"]:
                execution_order["dim & link"][(run_group, run_order)].append((entity, entity_type))
            else:
                execution_order[entity_type][(run_group, run_order)].append((entity, entity_type))
    
    # Define layer mapping
    layers = {
        "core": "Process layer: core",
        "dim & link": "layer: dim & link",
        "fact": "Process layer: fact"
    }

    # Output the sorted execution order in the desired format
    print("summarised Execution Order:")
    
    # Sort the layers to ensure the correct output order (core, dim & link, fact)
    for layer in ["core", "dim & link", "fact"]:
        if layer in execution_order:
            print(f"-------:{layers[layer]}----------")
            for (run_group, run_order), entities in sorted(execution_order[layer].items()):
                # Merge the entities and types for this run_group and run_order
                entity_list = ', '.join([f"{entity}" for entity, _ in entities])
                print(f"Run Group: {run_group}, Run Order: {run_order} [{entity_list}]")

    return execution_order


# COMMAND ----------


common_dimension_columns = [
    "start_datetime", 
    "end_datetime", 
    "is_current", 
    "Sys_Gold_InsertedDateTime_UTC", 
    "Sys_Gold_ModifiedDateTime_UTC"
    ]

# COMMAND ----------

def match_dimensions(df_data_to_match, df_shared_dimension_data):
    # stub function to run this process locally in the notebooks to simulate the matching process until it is written
    # each dataframe should have an "id" column and a "name" column - strict rules

    output_df = df_shared_dimension_data.join(df_data_to_match, df_data_to_match.name == df_shared_dimension_data.name, how="inner").drop(df_data_to_match.id)
    return output_df


