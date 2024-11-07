# Databricks notebook source
# DBTITLE 1,Initialise global objects
# MAGIC %run ./nb-orion-global

# COMMAND ----------

# DBTITLE 1,Get a list of the dimensions to process
dimension_names_generic = [] # empty list to hold the dim names to be processed with the generic notebook
dimension_names_nongeneric = [] # empty list to hold the dim names where a specific notebook must be run
# grab the dimension names from the dictionary and iterate through to create the above 2 lists

for dim_name in list(dimensions_dict.keys()):
    if dimensions_dict[dim_name]['processing_notebook'] == 'nb-orion-process-dimension':
        dimension_names_generic.append(dim_name)
    else:
        dimension_names_nongeneric.append(dim_name)
if "source_system" in dimension_names_generic:
    dimension_names_generic.remove("source_system")

specific_notebooks = []
# for the nongeneric, we need the notebooks names and so create a list of the specific notebooks to run
specific_notebooks = []
for dim_name in dimension_names_nongeneric:
    specific_notebooks.append(dimensions_dict[dim_name].get('processing_notebook'))


# COMMAND ----------

# now setup the run notebook function for the generic, where a dimension name is passed in as a paramters
def process_generic_dimension(dim_name):
    dbutils.notebook.run(path = f"./nb-orion-process-dimension",
                                        timeout_seconds = 600, 
                                        arguments = {"dimension_name":dim_name})
    
# function for calling a specific notebook, having no parmaters as it is specific
def process_nongeneric_dimension(notebook_name):
    dbutils.notebook.run(path = f"./{notebook_name}",
                                        timeout_seconds = 600)


# COMMAND ----------

# always run source_system first
dbutils.notebook.run(path = "./nb-orion-process-dimension",
                                        timeout_seconds = 600, 
                                        arguments = {"dimension_name":"source_system"})

# call the run notebook function in parallel based on parallel_max number of parallel threads
# for the generic notebook, passing in a list of the dimension names to process
if len(dimension_names_generic) > 0:
  with ThreadPoolExecutor(parallel_max) as executor:
    results = executor.map(process_generic_dimension, dimension_names_generic)

# now process in parallel the non generic notebooks, passing a list of notebook names (not dimension names)
if len(specific_notebooks) > 0:
  with ThreadPoolExecutor(parallel_max) as executor:
    results = [executor.submit(process_nongeneric_dimension, notebook) for notebook in specific_notebooks]


# COMMAND ----------

# Now populate the fact table
# left joins to dims 

