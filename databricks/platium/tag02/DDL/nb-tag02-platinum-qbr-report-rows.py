# Databricks notebook source
# MAGIC %md
# MAGIC ### Create view supporting row oriented tables in QBR report
# MAGIC This notebook generates sql based on the required tables configurations and creates single view, so the measures created in Power BI can be reused across tables. The view supports separate configs for each table (order of rows, descriptions, placeholders).

# COMMAND ----------

from collections import namedtuple
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA tag02;

# COMMAND ----------

# type to hold config
TableRowConfig = namedtuple(
    "TableRowConfig",
    ["description", "is_placeholder", "level_name", "level_value"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC Functions and variables for sql generation

# COMMAND ----------

actuals_cte = """actuals AS (
  SELECT hier.*, region.Country, trans.Date, trans.monthly_revenue_in_euros
  FROM qbr_account_description_hierarchy hier
    JOIN tagetik_revenue_actuals_intra_month trans
    ON hier.account_id = trans.account_code
    JOIN region
    ON trans.region_code = region.region_code
  WHERE trans.Cost_Centre_Code NOT LIKE 'E%'
    AND trans.Cost_Centre_Code NOT LIKE '%C%'
)"""

budget_cte = """budget AS (
  SELECT hier.*, region.Country, trans.Date, trans.monthly_revenue_in_euros
  FROM qbr_account_description_hierarchy hier
    JOIN tagetik_revenue_budget trans
    ON hier.account_id = trans.account_code
    JOIN region
    ON trans.region_code = region.region_code
  WHERE trans.Cost_Centre_Code NOT LIKE 'E%'
    AND trans.Cost_Centre_Code NOT LIKE '%C%'
)"""

group_by_sql = "GROUP BY Country, Date\n"

def compose_select_sql(report_type: str, description: str, order_id: int, is_revenue: bool) -> str:
    _agg = "SUM(monthly_revenue_in_euros)"
    _revenue = _agg if is_revenue else "0"
    _budget = _agg if not is_revenue else "0"
    return (
        f"SELECT '{report_type}' AS report_type, CAST({order_id} AS INT) AS order_id, "
        + f"Country, Date, '{description}' AS description, "
        + f"{_revenue} AS revenue, {_budget} AS budget\n"
    )

def compose_from_sql(is_revenue: bool) -> str:
    cte_source = "actuals" if is_revenue else "budget"
    return f"FROM {cte_source}\n"

def compose_where_sql(level_name: str, level_value: str) -> str:
    return f"WHERE {level_name} = '{level_value}'\n"

def compose_single_sql_string(
    report_type: str,
    descr: str,
    order_id: int,
    is_revenue: bool,
    level_name: str,
    level_value: str,
) -> str:
    select_ = compose_select_sql(report_type, descr, order_id, is_revenue)
    from_ = compose_from_sql(is_revenue)
    where_ = compose_where_sql(level_name, level_value)
    single_sql = select_ + from_ + where_ + group_by_sql
    return single_sql

def get_non_placeholder_sqls(
    report_type: str, descr: str, order_id: int, level_name: str, level_value: str
) -> list[str]:
    # one select for actuals and one for budget for each table row
    return [
        compose_single_sql_string(
            report_type, descr, order_id, True, level_name, level_value
        ),
        compose_single_sql_string(
            report_type, descr, order_id, False, level_name, level_value
        ),
    ]

def get_placeholder_sql(report_type: str, descr: str, order_id: int) -> str:
    return (
        f"SELECT '{report_type}' AS report_type, CAST({order_id} AS INT) AS order_id, '<placeholder>' AS Country, "
        + f"DATE('1900-01-01') AS Date, '{descr}' AS description, 0 AS revenue, 0 AS budget\n"
    )

def append_sqls_for_table(sqls: list[str], table: list[TableRowConfig], report_type: str) -> list[str]:
    """
    Generates sql statements based on each row config using the order of definition and appends to sqls list.

    :param sqls: list of single sql statements to append to
    :param table: ordered list of row configs for a table
    :param report_type: name of consuming report
    :return: result list of single sql statements
    """
    for i, config in enumerate(table):
        order_id = i + 1

        if config.is_placeholder:
            sqls.append(
                get_placeholder_sql(report_type=report_type,
                                    descr=config.description,
                                    order_id=order_id)
                )
        else:
            sqls += get_non_placeholder_sqls(
                report_type=report_type,
                descr=config.description,
                order_id=order_id,
                level_name=config.level_name,
                level_value=config.level_value,
            )
    return sqls

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC Table configurations

# COMMAND ----------

profit_loss_table = [
    TableRowConfig(
        description="Gross Revenue",
        level_name="level_5",
        level_value="Total Revenue",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Direct Profit (GP1)",
        level_name="level_4",
        level_value="GP1",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="% Margin (GP1)",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
    TableRowConfig(
        description="Net Distribution Profit (GP2)",
        level_name="level_3",
        level_value="GP2",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="% Margin (GP2)",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
    TableRowConfig(
        description="Gross Profit (GP3)",
        level_name="level_2",
        level_value="GP3",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="% Margin (GP3)",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
    TableRowConfig(
        description="Personnel Expenses",
        level_name="profit_loss_level_3",
        level_value="Personnel Expenses",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Non-FTE OPEX",
        level_name="profit_loss_level_3",
        level_value="Non-FTE OPEX",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Total OPEX",
        level_name="profit_loss_level_2",
        level_value="Total OPEX",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Adj. EBITDA",
        level_name="level_1",
        level_value="EBITDA (Adjusted)",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="% of Rev",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
    TableRowConfig(
        description="% Margin (GP3)",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
]

# COMMAND ----------

margin_table = [
    TableRowConfig(
        description="Gross Revenue",
        level_name="level_5",
        level_value="Total Revenue",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Direct Profit (GP1)",
        level_name="level_4",
        level_value="GP1",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="GP1 %",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
    TableRowConfig(
        description="Customer K&D",
        level_name="margin_level_3",
        level_value="Customer K&D",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Vendor K&D",
        level_name="margin_level_3",
        level_value="Vendor K&D",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Other GP2 Adj.",
        level_name="margin_level_3",
        level_value="Other GP2 Adjustments",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="GP2",
        level_name="level_3",
        level_value="GP2",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="GP2 %",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
    TableRowConfig(
        description="Freight Cost",
        level_name="margin_level_2",
        level_value="Freight Cost",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Change Inventory Allowances",
        level_name="margin_level_2",
        level_value="Change in Inventory Allowances",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Other GP3 Adj.",
        level_name="margin_level_2",
        level_value="Other GP3 Adjustments",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="Gross Profit (GP3)",
        level_name="level_2",
        level_value="GP3",
        is_placeholder=False,
    ),
    TableRowConfig(
        description="GP3 %",
        level_name=None,
        level_value=None,
        is_placeholder=True,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC Generate sql and create view

# COMMAND ----------

# create list of single sql statements and union them
# start with empty list and append for each required table
single_sqls = append_sqls_for_table(sqls=[], table=profit_loss_table, report_type="profit_loss")
single_sqls = append_sqls_for_table(sqls=single_sqls, table=margin_table, report_type="margin")
# union
row_sqls = "\nUNION\n".join(single_sqls)

# COMMAND ----------

# compose full sql
full_sql = f"""
CREATE OR REPLACE VIEW qbr_report_rows AS
WITH
{actuals_cte},
{budget_cte}
{row_sqls}"""

print(full_sql)

# COMMAND ----------

# create view
spark.sql(full_sql)
