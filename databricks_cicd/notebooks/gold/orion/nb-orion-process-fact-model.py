# Databricks notebook source
# DBTITLE 1,Initialise global objects
# MAGIC %run ./nb-orion-meta

# COMMAND ----------

# DBTITLE 1,Get a list of the dimensions to process
# Example usage
file_path = 'meta.json'
replacements = {
    "processing_notebook": processing_notebook,
    "ENVIRONMENT": ENVIRONMENT,
    "orion_schema": orion_schema
}
data = read_and_replace_json(file_path, replacements)


# COMMAND ----------

# consistency check
is_consistent = check_type_run_group_consistency(data)
print(f"\nData consistency: {'Consistent' if is_consistent else 'Inconsistent'}")

# COMMAND ----------

filtered_sorted_data = filter_and_sort_json(data, "type:fact", "run_group:100")

if isinstance(filtered_sorted_data, str):  # If the result is a string, it's an error message
    print(filtered_sorted_data)
else:
    print(filtered_sorted_data)  # Output the filtered and sorted entries

# COMMAND ----------

# Group Count
run_group_counts = count_run_groups_by_type(data)
print("Number of run groups for each type:")
for entity_type, count in run_group_counts.items():
    print(f"{entity_type}: {count}")


type_summary = count_objects_by_type(data)
print("Number of objects for each type:")
for entity_type, count in type_summary.items():
    print(f"{entity_type}: {count}")

# COMMAND ----------


data = read_and_replace_json(file_path, replacements)

# count objects by type run group and priority usage
counts = count_objects_by_type_run_group_and_priority(data)

print("Number of objects for each type, run_group, and priority:")
for entity_type, group_counts in counts.items():
    for run_group, priority_counts in group_counts.items():
        print(f"\n{entity_type} (run_group {run_group}):")
        for priority, count in sorted(priority_counts.items()):
            print(f"  Priority {priority}: {count}")

# Optional: Calculate and print totals
type_totals = {t: sum(sum(p.values()) for p in g.values()) for t, g in counts.items()}
total_objects = sum(type_totals.values())

print("\nTotals by type:")
for entity_type, total in type_totals.items():
    print(f"{entity_type}: {total}")

print(f"\nTotal number of objects: {total_objects}")

# COMMAND ----------


# summarise usage
summarised_execution_order = summarise_execution_order_with_layers(data)

# COMMAND ----------

# now setup the run notebook function for the generic, where a dimension name is passed in as a paramters
def process_logic(notebook_name,dim_name):
    dbutils.notebook.run(path = f"./{notebook_name}",
                                        timeout_seconds = 600, 
                                        arguments = {"dimension_name":dim_name})
    
# function for calling a specific notebook, having no parmaters as it is specific
# not used
def process_nongeneric_dimension(notebook_name):
    dbutils.notebook.run(path = f"./{notebook_name}",
                                        timeout_seconds = 600)


# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
def process_object(object_name, object_details):
    """
    A function that processing the processing logic [process_generic_dimension].
    """
        
    process_logic(notebook_name= object_details['processing_notebook'], dim_name=object_name)

    return object_name

# COMMAND ----------


def run_objects_by_group(data, object_type, max_workers=4):
    """
    Runs objects by their run_group in parallel batches.
    Each run_group's objects are run in parallel, but run_groups are processed sequentially.

    Parameters:
    - object_type: filter json by type
    - max_workers: Maximum number of threads to use in the ThreadPoolExecutor.
    """
    filter_type=filter_by_type(data, object_type)
 
    # Step 1: Organize objects by their run_group
    run_groups = {}
    for key, value in filter_type.items():
        run_group = value['run_group']
        if run_group not in run_groups:
            run_groups[run_group] = []
        run_groups[run_group].append((key, value))

    # Step 2: Sort run_groups by the run_group number (to ensure sequential processing)
    sorted_run_groups = sorted(run_groups.items())

    # Step 3: Process each run_group sequentially
    for run_group, objects in sorted_run_groups:
        print(f"Processing run_group {run_group}...")

        # Step 4: Process all objects in this run_group in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_object, key, details): key for key, details in objects}

            for future in as_completed(futures):
                object_name = futures[future]
                try:
                    result = future.result()
                    print(f"Completed processing for {result}")
                except Exception as exc:
                    print(f"{object_name} generated an exception: {exc}")

# COMMAND ----------


# Run the objects by group, in parallel for each group
run_objects_by_group(data=data, object_type='core', max_workers=4)

# COMMAND ----------


# Run the objects by group, in parallel for each group
run_objects_by_group(data=data, object_type='dim', max_workers=4)
