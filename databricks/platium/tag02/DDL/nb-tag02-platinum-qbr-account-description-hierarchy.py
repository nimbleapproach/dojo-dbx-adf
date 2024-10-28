# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"platinum_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA tag02;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE qbr_account_description_hierarchy ( 
# MAGIC     account_id STRING
# MAGIC       COMMENT 'Account code'
# MAGIC     ,level_1 STRING
# MAGIC       COMMENT 'Level 1 general group'
# MAGIC     ,level_2 STRING
# MAGIC       COMMENT 'Level 2 general group'
# MAGIC     ,level_3 STRING
# MAGIC       COMMENT 'Level 3 general group'
# MAGIC     ,level_4 STRING
# MAGIC       COMMENT 'Level 4 general group'
# MAGIC     ,level_5 STRING
# MAGIC       COMMENT 'Level 5 general group'
# MAGIC     ,opex_level_1 STRING
# MAGIC       COMMENT 'Level 1 opex group'
# MAGIC     ,opex_level_2 STRING
# MAGIC       COMMENT 'Level 2 opex group'
# MAGIC     ,opex_level_3 STRING
# MAGIC       COMMENT 'Level 3 opex group'
# MAGIC     ,profit_loss_level_1 STRING
# MAGIC       COMMENT 'Level 1 P&L group'
# MAGIC     ,profit_loss_level_2 STRING
# MAGIC       COMMENT 'Level 2 P&L group'
# MAGIC     ,profit_loss_level_3 STRING
# MAGIC       COMMENT 'Level 3 P&L group'
# MAGIC     ,profit_loss_level_4 STRING
# MAGIC       COMMENT 'Level 4 P&L group'
# MAGIC     ,profit_loss_level_5 STRING
# MAGIC       COMMENT 'Level 5 P&L group'
# MAGIC     ,margin_level_1 STRING
# MAGIC       COMMENT 'Level 1 margin group'
# MAGIC     ,margin_level_2 STRING
# MAGIC       COMMENT 'Level 2 margin group'
# MAGIC     ,margin_level_3 STRING
# MAGIC       COMMENT 'Level 3 margin group'
# MAGIC     ,margin_level_4 STRING
# MAGIC       COMMENT 'Level 4 margin group'
# MAGIC   ,CONSTRAINT qbr_account_description_hierarchy_pk PRIMARY KEY(account_id)
# MAGIC )
# MAGIC COMMENT 'This table classifies accounts according to four reporting structures: general corresponds with categories found in SR024 report; opex, profit and loss, and margin are supporting QBR reporting requirements. The group levels go from most general to most specific.' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (account_id);

# COMMAND ----------

# MAGIC %md
# MAGIC **Populate the relevant meta data...**

# COMMAND ----------

spark.sql(f"""
INSERT INTO platinum_{ENVIRONMENT}.tag02.qbr_account_description_hierarchy (
      account_id, level_1, level_2, level_3, level_4, level_5,
      opex_level_1, opex_level_2, opex_level_3,
      profit_loss_level_1, profit_loss_level_2, profit_loss_level_3, profit_loss_level_4, profit_loss_level_5, 
      margin_level_1, margin_level_2, margin_level_3, margin_level_4
)
VALUES 
      ("309988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310188", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310288", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310388", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310488", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310588", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310688", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310788", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310888", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("310988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("311088", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("312088", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("313088", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("314088", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("320988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("322988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("370988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total Revenue", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total Revenue", "GP3", "GP2", "GP1", "Total Revenue"),
      ("350988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("351988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("371988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("391988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("400988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("401988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("402988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("420988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("421988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("422988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440188", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440288", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440388", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440488", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440588", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440688", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440788", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("440888", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("449988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("450888", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("450988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("451988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("452788", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("452888", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("452988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("468988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("469988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("471988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("499988", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("409999_REF", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("420999_REF", "EBITDA (Adjusted)", "GP3", "GP2", "GP1", "Total COGS", NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "GP1", "Total COGS", "GP3", "GP2", "GP1", "Total COGS"),
      ("390188", "EBITDA (Adjusted)", "GP3", "GP2", "Customer Kickbacks", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Customer K&D", NULL),
      ("390988", "EBITDA (Adjusted)", "GP3", "GP2", "Customer Kickbacks", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Customer K&D", NULL),
      ("470188", "EBITDA (Adjusted)", "GP3", "GP2", "Vendor Kickbacks", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Vendor K&D", NULL),
      ("390088", "EBITDA (Adjusted)", "GP3", "GP2", "Customer Payment Discounts", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Customer K&D", NULL),
      ("470988", "EBITDA (Adjusted)", "GP3", "GP2", "Customer Payment Discounts", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Customer K&D", NULL),
      ("470088", "EBITDA (Adjusted)", "GP3", "GP2", "Vendor Payment Discounts", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Vendor K&D", NULL),
      ("470888", "EBITDA (Adjusted)", "GP3", "GP2", "Other GP2 Adjustments", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "GP2", "Other", NULL, "GP3", "GP2", "Other GP2 Adjustments", NULL),
      ("480988", "EBITDA (Adjusted)", "GP3", "Total freight cost", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Freight Cost", NULL, NULL),
      ("481988", "EBITDA (Adjusted)", "GP3", "Total freight cost", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Freight Cost", NULL, NULL),
      ("551188", "EBITDA (Adjusted)", "GP3", "Total freight cost", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Freight Cost", NULL, NULL),
      ("551288", "EBITDA (Adjusted)", "GP3", "Total freight cost", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Freight Cost", NULL, NULL),
      ("552988", "EBITDA (Adjusted)", "GP3", "Total freight cost", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Freight Cost", NULL, NULL),
      ("470678", "EBITDA (Adjusted)", "GP3", "Change in Inventory Allowances", "Change in Obsolete Inventory Allowance MAIN", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Change in Inventory Allowances", NULL, NULL),
      ("470688", "EBITDA (Adjusted)", "GP3", "Change in Inventory Allowances", "Write-off - MAIN Inventory", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Change in Inventory Allowances", NULL, NULL),
      ("470778", "EBITDA (Adjusted)", "GP3", "Change in Inventory Allowances", "Change in Obsolete Inventory Allowance DEMO", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Change in Inventory Allowances", NULL, NULL),
      ("470788", "EBITDA (Adjusted)", "GP3", "Change in Inventory Allowances", "Write-off - DEMO Inventory", NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Change in Inventory Allowances", NULL, NULL),
      ("480188", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480189", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480197", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480198", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480288", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480289", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480388", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480488", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("480888", "EBITDA (Adjusted)", "GP3", " Total Other GP3 Adjustments", NULL, NULL, NULL, NULL, NULL, "Adj EBITDA", "GP3", "Other", "Other", NULL, "GP3", "Other GP3 Adjustments", NULL, NULL),
      ("500188", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "Base Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500190", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "Base Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("504988", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "Base Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500189", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "Capitalized Salary (Asset)", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500197", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "IC Shared Resources", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500198", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "IC Shared Resources", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502997", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "IC Shared Resources", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502998", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "IC Shared Resources", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500197_REF", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Salary", "IC Shared Resources", "Total Opex", "Net Personnel Expenses", "Salary", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("470288", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Vendor Fundings", NULL, "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500288", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Bonus Employee", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500295", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Bonus Employee", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500296", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Bonus Employee", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502489", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Bonus Employee", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500289", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Total Bonus", "Bonus Management", "Total Opex", "Net Personnel Expenses", "Bonus Management", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502490", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Total Bonus", "Bonus Management", "Total Opex", "Net Personnel Expenses", "Bonus Management", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500388", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Vacation / Holidays", NULL, "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500397", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Vacation / Holidays", NULL, "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500398", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Vacation / Holidays", "I/C Shared Resource – Vacation / Holidays (Income)", "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500488", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Severance", NULL, "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("500588", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Share / Option", NULL, "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502488", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Social Security", "National Contribution", "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502588", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Social Security", "Pension Benefits", "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502788", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Social Security", "Employee Insurances", "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("502988", "EBITDA (Adjusted)", "Total OPEX", "Net Personnel Expenses", "Social Security", "Other Social Security and Insurances", "Total Opex", "Net Personnel Expenses", "Other", "Adj EBITDA", "Total OPEX", "Personnel Expenses", NULL, NULL, NULL, NULL, NULL, NULL),
      ("585188", "EBITDA (Adjusted)", "Total OPEX", "Net Strategic Projects", NULL, NULL, "Total Opex", "Other OPEX", "Net Strategic Projects", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("505188", "EBITDA (Adjusted)", "Total OPEX", "Services Received", NULL, NULL, "Total Opex", "Other OPEX", "Services Received", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("505997", "EBITDA (Adjusted)", "Total OPEX", "Services Received", NULL, NULL, "Total Opex", "Other OPEX", "Services Received", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("505998", "EBITDA (Adjusted)", "Total OPEX", "Services Received", NULL, NULL, "Total Opex", "Other OPEX", "Services Received", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503288", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503297", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503298", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503188", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Training", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503997", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("503998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit", "Total Opex", "Net Other Expenses", "Personnel Related", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("510988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Office Rent", NULL, "Total Opex", "Net Other Expenses", "Office Rent / Costs", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("510998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Office Rent", NULL, "Total Opex", "Net Other Expenses", "Office Rent / Costs", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("520988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Office Cost", NULL, "Total Opex", "Net Other Expenses", "Office Rent / Costs", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("521188", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Logistics & Warehousing", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("521197", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Logistics & Warehousing", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("521988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Communication", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("521997", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Communication", "I/C Transfer Communication (Expense)", "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("521998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Communication", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("523688", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total IT Cost", "IT Cost", "Total Opex", "Net Other Expenses", "IT Costs", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("523697", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total IT Cost", "I/C Transfer IT Cost (Expense)", "Total Opex", "Net Other Expenses", "IT Costs", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("523698", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total IT Cost", "I/C Transfer IT Cost (Income)", "Total Opex", "Net Other Expenses", "IT Costs", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("524988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Company Insurance", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("524997", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Company Insurance", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("524998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Company Insurance", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("530988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Vehicles", NULL, "Total Opex", "Net Other Expenses", "Travel & Vehicle", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("530997", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Vehicles", NULL, "Total Opex", "Net Other Expenses", "Travel & Vehicle", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("530998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Vehicles", NULL, "Total Opex", "Net Other Expenses", "Travel & Vehicle", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("532988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Travel", NULL, "Total Opex", "Net Other Expenses", "Travel & Vehicle", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("532997", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Travel", NULL, "Total Opex", "Net Other Expenses", "Travel & Vehicle", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("532998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Travel", NULL, "Total Opex", "Net Other Expenses", "Travel & Vehicle", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("540988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Consulting", NULL, "Total Opex", "Net Other Expenses", "Consulting", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("541988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Audit Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("543988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Customer Insurance", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("561988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate", "Total Opex", "Net Other Expenses", "Marketing", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("561997", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate", "Total Opex", "Net Other Expenses", "Marketing", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("561998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate", "Total Opex", "Net Other Expenses", "Marketing", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("562288", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total Marketing Cost", "Marketing Refund Third Pary", "Total Opex", "Net Other Expenses", "Marketing", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("562988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Total Marketing Cost", "Marketing COS Third Party", "Total Opex", "Net Other Expenses", "Marketing", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("570188", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Non-Refundable VAT", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581788", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Change in Bad Debt Allowances", "Write-off - Receivables", "Total Opex", "Net Other Expenses", "Bad Debt", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581778", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Change in Bad Debt Allowances", "Change in Bad Debt Allowance", "Total Opex", "Net Other Expenses", "Bad Debt", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("582388", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("582988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583388", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583397", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583398", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583488", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583888", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("583998", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Financial Expense", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("544988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Administrative Services", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("570288", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Tax Other", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("570388", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Tax Other", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("542898", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("579988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("580988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581488", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581497", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581498", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581588", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581688", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("581988", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL),
      ("582888", "EBITDA (Adjusted)", "Total OPEX", "Net Other Expenses", "Other Cost", NULL, "Total Opex", "Net Other Expenses", "Other", "Adj EBITDA", "Total OPEX", "Non-FTE OPEX", NULL, NULL, NULL, NULL, NULL, NULL)
""")
