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


ddl_deployment_df = get_ddl_deployment_df()


# COMMAND ----------

# consistency check
is_consistent = check_type_run_group_consistency(data)
print(f"\nData consistency: {'Consistent' if is_consistent else 'Inconsistent'}")

# COMMAND ----------

filtered_sorted_data = filter_and_sort_json(data, "type:dim", "run_group:4")

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
summarised_execution_order = summarize_execution_order_with_layers(data)

# COMMAND ----------

# now setup the run notebook function for the generic, where a dimension name is passed in as a paramters
def process_generic_dimension(notebook_name,dim_name):
    dbutils.notebook.run(path = f"./{notebook_name}",
                                        timeout_seconds = 600, 
                                        arguments = {"dimension_name":dim_name})
    
# function for calling a specific notebook, having no parmaters as it is specific
def process_nongeneric_dimension(notebook_name):
    dbutils.notebook.run(path = f"./{notebook_name}",
                                        timeout_seconds = 600)


# COMMAND ----------

# always run source_system first
filtered_sorted_data = filter_and_sort_json(data, "type:core", "run_group:0")

if isinstance(filtered_sorted_data, str):  # If the result is a string, it's an error message
    raise ValueError(filtered_sorted_data)
else:
    print(filtered_sorted_data)  # Output the filtered and sorted entries


if len(filtered_sorted_data) > 0:
  for key, entry in filtered_sorted_data:
    print(f"Key: {key}, Processing Notebook: {entry.get('processing_notebook', 'No notebook found')}, "
          f"Target table: {entry.get('destination_table_name', 'No table found')}")
    nb = {entry.get('processing_notebook')}
    dt = {entry.get('destination_table_name')}
    with ThreadPoolExecutor(parallel_max) as executor:
      results = executor.map(process_generic_dimension, nb, dt)



# COMMAND ----------

# Now populate the fact table
# left joins to dims 

