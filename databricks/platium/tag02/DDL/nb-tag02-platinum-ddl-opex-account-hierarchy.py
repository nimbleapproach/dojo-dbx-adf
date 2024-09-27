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
# MAGIC CREATE OR REPLACE TABLE opex_account_hierarchy ( 
# MAGIC     account_id STRING
# MAGIC       COMMENT 'Account code'
# MAGIC     ,category_description STRING
# MAGIC       COMMENT 'Most granular category description'
# MAGIC     ,opex_report_description STRING
# MAGIC       COMMENT 'Report display description'
# MAGIC     ,level_1 STRING
# MAGIC       COMMENT 'Level 1 - (most general) group of categories'
# MAGIC     ,level_2 STRING
# MAGIC       COMMENT 'Level 2 group of categories'
# MAGIC     ,level_3 STRING
# MAGIC       COMMENT 'Level 3 group of categories'
# MAGIC     ,level_4 STRING
# MAGIC       COMMENT 'Level 4 group of categories'
# MAGIC   ,CONSTRAINT opex_account_hierarchy_pk PRIMARY KEY(account_id)
# MAGIC )
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (account_id);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Populate the relevant meta data...**

# COMMAND ----------

spark.sql(f"""
INSERT INTO platinum_{ENVIRONMENT}.tag02.opex_account_hierarchy (
      account_id, category_description, opex_report_description, level_1, level_2, level_3, level_4
)
VALUES 
      ("500188", "Base Salary", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Base Salary"),
      ("500190", "Base Salary", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Base Salary"),
      ("504988", "Base Salary", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Base Salary"),
      ("500189", "Capitalized Salary (Asset)", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Capitalized Salary (Asset)"),
      ("500197", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources"),
      ("500198", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources"),
      ("502997", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources"),
      ("502998", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources"),
      ("500197_REF", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources"),
      ("470288", "Vendor Fundings", "Other", "Total Opex", "Net Personnel Expenses", "Vendor Fundings", "Vendor Fundings"),
      ("500288", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee"),
      ("500295", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee"),
      ("500296", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee"),
      ("502489", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee"),
      ("500289", "Bonus Management", "Bonus Management", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Management"),
      ("502490", "Bonus Management", "Bonus Management", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Management"),
      ("500388", "Vacation / Holidays", "Other", "Total Opex", "Net Personnel Expenses", "Vacation / Holidays", "Vacation / Holidays"),
      ("500397", "Vacation / Holidays", "Other", "Total Opex", "Net Personnel Expenses", "Vacation / Holidays", "Vacation / Holidays"),
      ("500488", "Severance", "Other", "Total Opex", "Net Personnel Expenses", "Severance", "Severance"),
      ("500588", "Share / Option", "Other", "Total Opex", "Net Personnel Expenses", "Share / Option", "Share / Option"),
      ("502488", "National Contribution", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "National Contribution"),
      ("502588", "Pension Benefits", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "Pension Benefits"),
      ("502788", "Employee Insurances", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "Employee Insurances"),
      ("502988", "Other Social Security and Insurances", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "Other Social Security and Insurances"),
      ("503288", "Personnel - Search Expense", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense"),
      ("503297", "Personnel - Search Expense", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense"),
      ("503298", "Personnel - Search Expense", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense"),
      ("503188", "Personnel - Training", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Training"),
      ("503988", "Personnel - Other Benefit", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit"),
      ("503997", "Personnel - Other Benefit", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit"),
      ("503998", "Personnel - Other Benefit", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit"),
      ("510988", "Office Rent", "Office Rent / Costs", "Total Opex", "Net Other Expenses", "Office Rent", NULL),
      ("510998", "Office Rent", "Office Rent / Costs", "Total Opex", "Net Other Expenses", "Office Rent", NULL),
      ("520988", "Office Cost", "Office Rent / Costs", "Total Opex", "Net Other Expenses", "Office Cost", NULL),
      ("521188", "Logistics & Warehousing", "Other", "Total Opex", "Net Other Expenses", "Logistics & Warehousing", NULL),
      ("521197", "Logistics & Warehousing", "Other", "Total Opex", "Net Other Expenses", "Logistics & Warehousing", NULL),
      ("521988", "Communication", "Other", "Total Opex", "Net Other Expenses", "Communication", NULL),
      ("521998", "Communication", "Other", "Total Opex", "Net Other Expenses", "Communication", NULL),
      ("523688", "IT Cost", "IT Costs", "Total Opex", "Net Other Expenses", "Total IT Cost", "IT Cost"),
      ("523697", "I/C Transfer IT Cost (Expense)", "IT Costs", "Total Opex", "Net Other Expenses", "Total IT Cost", "I/C Transfer IT Cost (Expense)"),
      ("523698", "I/C Transfer IT Cost (Income)", "IT Costs", "Total Opex", "Net Other Expenses", "Total IT Cost", "I/C Transfer IT Cost (Income)"),
      ("524988", "Company Insurance", "Other", "Total Opex", "Net Other Expenses", "Company Insurance", NULL),
      ("524997", "Company Insurance", "Other", "Total Opex", "Net Other Expenses", "Company Insurance", NULL),
      ("524998", "Company Insurance", "Other", "Total Opex", "Net Other Expenses", "Company Insurance", NULL),
      ("530988", "Vehicles", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Vehicles", NULL),
      ("530997", "Vehicles", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Vehicles", NULL),
      ("530998", "Vehicles", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Vehicles", NULL),
      ("532988", "Travel", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Travel", NULL),
      ("532997", "Travel", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Travel", NULL),
      ("532998", "Travel", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Travel", NULL),
      ("540988", "Consulting", "Consulting", "Total Opex", "Net Other Expenses", "Consulting", NULL),
      ("541988", "Audit Cost", "Other", "Total Opex", "Net Other Expenses", "Audit Cost", NULL),
      ("543988", "Customer Insurance", "Other", "Total Opex", "Net Other Expenses", "Customer Insurance", NULL),
      ("561988", "Marketing Cost Infinigate", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate"),
      ("561997", "Marketing Cost Infinigate", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate"),
      ("561998", "Marketing Cost Infinigate", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate"),
      ("562288", "Marketing Refund Third Pary", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Refund Third Pary"),
      ("562988", "Marketing COS Third Party", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing COS Third Party"),
      ("570188", "Non-Refundable VAT", "Other", "Total Opex", "Net Other Expenses", "Non-Refundable VAT", "Non-Refundable VAT"),
      ("581788", "Write-off - Receivables", "Bad Debt", "Total Opex", "Net Other Expenses", "Change in Bad Debt Allowances", "Write-off - Receivables"),
      ("581778", "Change in Bad Debt Allowance", "Bad Debt", "Total Opex", "Net Other Expenses", "Change in Bad Debt Allowances", "Change in Bad Debt Allowance"),
      ("582388", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("582988", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583388", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583397", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583398", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583488", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583888", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583988", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("583998", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL),
      ("544988", "Administrative Services", "Other", "Total Opex", "Net Other Expenses", "Administrative Services", NULL),
      ("570288", "Tax Other", "Other", "Total Opex", "Net Other Expenses", "Tax Other", NULL),
      ("570388", "Tax Other", "Other", "Total Opex", "Net Other Expenses", "Tax Other", NULL),
      ("542898", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("579988", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("580988", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("581488", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("581497", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("581498", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("581588", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("581688", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("581988", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("582888", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL),
      ("585188", "Net Strategic Projects", "Net Strategic Projects", "Total Opex", "Other OPEX", "Net Strategic Projects", "Net Strategic Projects"),
      ("505188", "Services Received", "Services Received", "Total Opex", "Other OPEX", "Services Received", "Services Received"),
      ("505997", "Services Received", "Services Received", "Total Opex", "Other OPEX", "Services Received", "Services Received"),
      ("505998", "Services Received", "Services Received", "Total Opex", "Other OPEX", "Services Received", "Services Received"),
      ("309988", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310188", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310288", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310388", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310488", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310588", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310688", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310788", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310888", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("310988", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("311088", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("312088", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("313088", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("314088", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("320988", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("322988", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("370988", "Total Revenue", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total Revenue"),
      ("350988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("351988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("371988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("391988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("400988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("401988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("402988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("420988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("421988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("422988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440188", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440288", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440388", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440488", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440588", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440688", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440788", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("440888", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("449988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("450888", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("450988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("451988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("452788", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("452888", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("452988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("468988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("469988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("471988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("499988", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("409999_REF", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("420999_REF", "Total COGS", "GP3", "GP3", "Net Distribution Profit (GP2)", "Direct Profit (GP1)", "Total COGS"),
      ("390188", "Customer Kickbacks", "GP3", "GP3", "Net Distribution Profit (GP2)", "Customer Kickbacks", NULL),
      ("470188", "Vendor Kickbacks", "GP3", "GP3", "Net Distribution Profit (GP2)", "Vendor Kickbacks", NULL),
      ("390088", "Customer Payment Discounts", "GP3", "GP3", "Net Distribution Profit (GP2)", "Customer Payment Discounts", NULL),
      ("470088", "Vendor Payment Discounts", "GP3", "GP3", "Net Distribution Profit (GP2)", "Vendor Payment Discounts", NULL),
      ("470888", "Other GP2 Adjustments", "GP3", "GP3", "Net Distribution Profit (GP2)", "Other GP2 Adjustments", NULL),
      ("480988", "Total freight cost", "GP3", "GP3", "Total freight cost", NULL, NULL),
      ("481988", "Total freight cost", "GP3", "GP3", "Total freight cost", NULL, NULL),
      ("551188", "Total freight cost", "GP3", "GP3", "Total freight cost", NULL, NULL),
      ("551288", "Total freight cost", "GP3", "GP3", "Total freight cost", NULL, NULL),
      ("552988", "Total freight cost", "GP3", "GP3", "Total freight cost", NULL, NULL),
      ("470678", "Change in Obsolete Inventory Allowance MAIN", "GP3", "GP3", "Change in Inventory Allowances", "Change in Obsolete Inventory Allowance MAIN", NULL),
      ("470688", "Write-off - MAIN Inventory", "GP3", "GP3", "Change in Inventory Allowances", "Write-off - MAIN Inventory", NULL),
      ("470778", "Change in Obsolete Inventory Allowance DEMO", "GP3", "GP3", "Change in Inventory Allowances", "Change in Obsolete Inventory Allowance DEMO", NULL),
      ("470788", "Write-off - DEMO Inventory", "GP3", "GP3", "Change in Inventory Allowances", "Write-off - DEMO Inventory", NULL),
      ("480188", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480189", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480197", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480198", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480288", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480289", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480388", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480488", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL),
      ("480888", "Total Other GP3 Adjustments", "GP3", "GP3", "Total Other GP3 Adjustments", NULL, NULL)
""")

# COMMAND ----------

# spark.table('opex_account_hierarchy').display()
