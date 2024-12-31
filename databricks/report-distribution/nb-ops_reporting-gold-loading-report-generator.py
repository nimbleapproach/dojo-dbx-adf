# Databricks notebook source
import json
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
working_folder = '/Workspace' + current_notebook[0:current_notebook.rfind('/')]

# COMMAND ----------

# Loading config
report_config = "report.config.json"
with open(f"{working_folder}/{report_config}", "r") as file:
    data = json.load(file)

# Prompting input parameters
try:
    REPORT_NAME = dbutils.widgets.get("wg_reportName")
except:
    dbutils.widgets.dropdown(name="wg_reportName", defaultValue=list(data.keys())[0], choices=sorted(data.keys()))
    REPORT_NAME = dbutils.widgets.get("wg_reportName")

# COMMAND ----------

# Reading config
report_config = data.get(REPORT_NAME)

report_params = report_config.get("report-params")

databricks_config = report_config.get("databricks-config")
databricks_predicate = databricks_config.get("predicate")
databricks_object_layer = databricks_config.get("object-layer")
databricks_object_schema = databricks_config.get("object-schema")
databricks_object_name = databricks_config.get("object-name")

subscription_schedule = report_config.get("subscription").get("schedule")
from_date_cron = subscription_schedule.get("from-date-cron")
to_date_cron = subscription_schedule.get("to-date-cron")

subscription_recipients = report_config.get("subscription").get("recipients")
subscription_to = subscription_recipients.get("to")
subscription_cc = subscription_recipients.get("cc")
subscription_bcc = subscription_recipients.get("bcc")

# COMMAND ----------

# MAGIC %run ../library/nb-cron-utils

# COMMAND ----------

from datetime import datetime, timedelta

date_format = "%Y-%m-%d"

try:
    REPORT_RUN_DATE = dbutils.widgets.get("wg_reportRunDate")
except:
    dbutils.widgets.text(name="wg_reportRunDate", defaultValue=datetime.strftime(datetime.now(), date_format))
    REPORT_RUN_DATE = dbutils.widgets.get("wg_reportRunDate")

report_run_date_as_datetime = datetime.strptime(REPORT_RUN_DATE, date_format)
report_date_to = get_previous_date(report_run_date_as_datetime, to_date_cron) - timedelta(days=1)
report_date_from = get_previous_date(report_date_to, from_date_cron)

try:
    REPORT_DATE_FROM = dbutils.widgets.get("wg_reportDateFrom")
except:
    dbutils.widgets.text(name="wg_reportDateFrom", defaultValue=datetime.strftime(report_date_from, date_format))
    REPORT_DATE_FROM = dbutils.widgets.get("wg_reportDateFrom")

try:
    REPORT_DATE_TO = dbutils.widgets.get("wg_reportDateTo")
except:
    dbutils.widgets.text(name="wg_reportDateTo", defaultValue=datetime.strftime(report_date_to, date_format))
    REPORT_DATE_TO = dbutils.widgets.get("wg_reportDateTo")



# COMMAND ----------

from pyspark.sql.functions import *

input_params = {"from": REPORT_DATE_FROM, "to": REPORT_DATE_TO}
column_mapping = dict((v,k) for k,v in report_config.get("column-mapping").items())
report_columns = column_mapping.values()
view_columns_in_scope = [col(c) for c in column_mapping.keys()]

databricks_param_mapping = {}
for param_name, param_value in input_params.items():
    databricks_param_mapping[f"@{param_name}"] = param_value
# print(databricks_param_mapping)

def substitute_params(str, substitutions):
    result = str
    for bind_var, replacement in substitutions.items():
        result = result.replace(bind_var, replacement)
    return result

effective_databricks_predicate = substitute_params(databricks_predicate, databricks_param_mapping)
# print(effective_databricks_predicate)

debug_predicate="1=1" 

databricks_table = (spark.read
                    .table(
    f"{databricks_object_layer}_{ENVIRONMENT}.{databricks_object_schema}.{databricks_object_name}")
                    .filter(effective_databricks_predicate)
                    #.where(debug_predicate)
                    .select(view_columns_in_scope)
                    .withColumnsRenamed(column_mapping)
                    )

report_data_types = dict(databricks_table.dtypes)
print(report_data_types)

dataset = databricks_table.collect()

# COMMAND ----------

!pip install openpyxl --quiet

from copy import copy
from datetime import datetime
from openpyxl.reader.excel import load_workbook
from openpyxl.styles import Border, Side

# Loading report template
template = "template.xlsx"
wb = load_workbook(working_folder + "/" + template)
sheet = wb.active

# Populating sheet name
sheet.title = REPORT_NAME

# Populating report name header
report_name_cell_name = "A2"
sheet[report_name_cell_name] = REPORT_NAME

# Populating date range header
date_range_cell_name = "A5"
date_range_input_format = "%Y-%m-%d"
date_range_print_format = "%d/%m/%Y"

formatted_date_from=datetime.strftime(datetime.strptime(REPORT_DATE_FROM, date_range_input_format), date_range_print_format)
formatted_date_to=datetime.strftime(datetime.strptime(REPORT_DATE_TO, date_range_input_format), date_range_print_format)

date_range_header_text = f"Report Period From {formatted_date_from} To {formatted_date_to}"
sheet[date_range_cell_name] = date_range_header_text

# Populating data column headers
header_template_cell_col = "A"
header_template_cell_row = 8
header_template_cell_name = header_template_cell_col + str(header_template_cell_row)
header_template_cell = sheet[header_template_cell_name]
header_cell_font = copy(header_template_cell.font)
header_cell_fill = copy(header_template_cell.fill)
header_cell_border = copy(header_template_cell.border)

for i, col in enumerate(report_columns):
    c = sheet.cell(row=header_template_cell_row, column=i + 1)
    c.value = col
    c.font = header_cell_font
    c.fill = header_cell_fill
    c.border = header_cell_border

# Populating data cells
data_template_cell_col = "A"
data_template_cell_row = 9
data_template_cell_name = data_template_cell_col + str(data_template_cell_row)
data_template_cell = sheet[data_template_cell_name]
data_cell_font = copy(data_template_cell.font)
data_cell_border = copy(data_template_cell.border)

cell_date_format = '[$-en-US,1]dd/mm/yyyy'
cell_number_format = '[$-en-US,1]#,##0.00'

for i, data_row in enumerate(dataset):
    for j, data_col in enumerate(report_columns):
        # print(data_row.asDict())
        c = sheet.cell(row=header_template_cell_row + i + 1, column=j + 1)
        c.value = data_row[data_col]
        c.border = data_cell_border
        c.font = data_cell_font

        data_type = report_data_types[data_col]
        if (data_type == "float") or (data_type == "double") or (data_type.startswith("decimal")):
            c.number_format = cell_number_format
        elif (data_type == "date") or (data_type == "timestamp") or (data_type == "timestampNTZ"):
            c.number_format = cell_date_format

# Saving result
report_folder = working_folder + "/reports"
os.makedirs(report_folder, exist_ok=True)

timestamp=datetime.strftime(datetime.today(), "%Y%m%d%H%M%S")
report_file_name = f"{REPORT_NAME}-{timestamp}.xlsx"

wb.save(report_folder + "/" + report_file_name)
