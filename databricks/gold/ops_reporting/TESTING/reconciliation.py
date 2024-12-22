# Databricks notebook source
import os
import json
from pyspark.sql.functions import *

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

HEAD_SIZE = 10
ROUND_POSITIONS = 4

current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
working_folder = '/Workspace' + current_notebook[0:current_notebook.rfind('/')]

with open(f"{working_folder}/recon.config.json", "r") as file:
    data = json.load(file)

try:
    REPORT = dbutils.widgets.get("wg_report")
except:
    dbutils.widgets.dropdown(name="wg_report", defaultValue=list(data.keys())[0], choices=sorted(data.keys()))
    REPORT = dbutils.widgets.get("wg_report")
print("Validating report {}".format(REPORT))

report_config = data.get(REPORT)

report_params = report_config.get("report-params")
input_params = {}

for report_param in report_params:
    try:
        PARAM_VALUE = dbutils.widgets.get(f"wg_inputParam_{report_param}")
    except:
        dbutils.widgets.text(name=f"wg_inputParam_{report_param}", defaultValue="")
        PARAM_VALUE = dbutils.widgets.get(f"wg_inputParam_{report_param}")
    input_params[report_param] = PARAM_VALUE

print(f"Input parameters: {input_params}")

try:
    MSSQL_USERNAME = dbutils.widgets.get("wg_msSqlUsername")
except:
    dbutils.widgets.text(name="wg_msSqlUsername", defaultValue="")
    MSSQL_USERNAME = dbutils.widgets.get("wg_msSqlUsername")

try:
    MSSQL_PASSWORD = dbutils.widgets.get("wg_msSqlPassword")
except:
    dbutils.widgets.text(name="wg_msSqlPassword", defaultValue="")
    MSSQL_PASSWORD = dbutils.widgets.get("wg_msSqlPassword")


def substitute_params(str, substitutions):
    result = str
    for bind_var, replacement in substitutions.items():
        result = result.replace(bind_var, replacement)
    return result


# COMMAND ----------

ssrs_config = report_config.get("ssrs-config")
report_folder = ssrs_config.get("folder")
report_main_query_file = ssrs_config.get("main-query-file")
report_cte_query_file = ssrs_config.get("cte-file")
ssrs_database = ssrs_config.get("database")
ssrs_param_customizations = ssrs_config.get("param-customizations")

databricks_config = report_config.get("databricks-config")
databricks_predicate = databricks_config.get("predicate")
databricks_object_layer = databricks_config.get("object-layer")
databricks_object_schema = databricks_config.get("object-schema")
databricks_object_name = databricks_config.get("object-name")

column_mapping = report_config.get("column-mapping")
# print(column_mapping)

ssrs_param_mapping = dict(zip(report_params, [ssrs_param_customizations.get(p) for p in report_params]))
# print(ssrs_param_mapping)

ssrs_effective_params = {}
for param, customized_param in ssrs_param_mapping.items():
    bind_var = f"@{param}"
    input_value = input_params.get(param)
    ssrs_effective_params[bind_var] = customized_param.replace(bind_var,
                                                               input_value) if customized_param is not None else input_value
# print(ssrs_effective_params)

report_folder_full_path = f"{working_folder}/reports/{report_folder}"

report_main_query_file_path = f"{report_folder_full_path}/{report_main_query_file}"
with open(report_main_query_file_path) as sql_file:
    main_query_sql = substitute_params(sql_file.read(), ssrs_effective_params)
# print(main_query_sql)

if report_cte_query_file:
    report_cte_query_file_path = f"{report_folder_full_path}/{report_cte_query_file}"
    with open(report_cte_query_file_path) as sql_file:
        cte_query_sql = substitute_params(sql_file.read(), ssrs_effective_params)
else:
    cte_query_sql = "with dummy_ as (select 1 dummy_)"
# print(cte_query_sql)

databricks_param_mapping = {}
for param_name, param_value in input_params.items():
    databricks_param_mapping[f"@{param_name}"] = param_value
# print(databricks_param_mapping)

effective_databricks_predicate = substitute_params(databricks_predicate, databricks_param_mapping)
# print(effective_databricks_predicate)

debug_predicate="1=1" 

# debug_predicate = '' #uncomment if necessarry. when finished, return filter back to '1=1'

columns_in_scope = [col(c) for c in column_mapping.values()]

databricks_table = (spark.read
                    .table(
    f"{databricks_object_layer}_{ENVIRONMENT}.{databricks_object_schema}.{databricks_object_name}")
                    .filter(effective_databricks_predicate)
                    .where(debug_predicate)
                    .select(columns_in_scope)
                    # .select(limited_columns_in_scope)
                    .fillna("").fillna(0).fillna(False)
                    # .withColumn("hash_", xxhash64(*columns_in_scope))
                    )
# print(databricks_table.dtypes)

rounding = {}
for col_name, col_type in databricks_table.dtypes:
    if col_type.startswith('decimal'):
        rounding[col_name] = round(col_name, ROUND_POSITIONS)

if len(rounding) > 0:
    databricks_table = databricks_table.withColumns(rounding)

print(f"DataBricks result (first {HEAD_SIZE}):")
databricks_table.toPandas().head(HEAD_SIZE)

# COMMAND ----------

databricks_table_count = databricks_table.count()
print(f"Row count in DataBricks: {databricks_table_count}")

# COMMAND ----------

remote_table = (spark.read
                .format("sqlserver")
                .option("host", "nuaz-sqlserver-01.database.windows.net")
                .option("port", "1433")
                .option("user", MSSQL_USERNAME)
                .option("password", MSSQL_PASSWORD)
                .option("database", ssrs_database)
                .option("prepareQuery", cte_query_sql)
                .option("query", main_query_sql)
                .load()
                .withColumnsRenamed(column_mapping)
                .where(debug_predicate)
                .select(columns_in_scope)
                # .select(limited_columns_in_scope)
                .fillna("").fillna(0).fillna(False)
                # .withColumn("hash_", xxhash64(*columns_in_scope))
                )

if len(rounding) > 0:
    remote_table = remote_table.withColumns(rounding)

print(f"SSRS result (first {HEAD_SIZE}):")
remote_table.toPandas().head(HEAD_SIZE)

# COMMAND ----------

remote_table_count = remote_table.count()
print(f"Row count in SSRS: {remote_table_count}")

# COMMAND ----------

if (remote_table_count != databricks_table_count):
    print(f"Row count mismatch: {remote_table_count} != {databricks_table_count}")
else:
    print("Counts match!")

# COMMAND ----------

print(f"Extra records in Databricks (first {HEAD_SIZE})")
databricks_table.subtract(remote_table).toPandas().head(HEAD_SIZE)

# COMMAND ----------

print(f"Missing records in SSRS (first {HEAD_SIZE})")
remote_table.subtract(databricks_table).toPandas().head(HEAD_SIZE)
