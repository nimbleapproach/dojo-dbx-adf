# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

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
# MAGIC     ,level_5 STRING
# MAGIC       COMMENT 'Level 5 group of categories'
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
INSERT INTO gold_{ENVIRONMENT}.tag02.opex_account_hierarchy (
      account_id, category_description, opex_report_description, level_1, level_2, level_3, level_4, level_5
)
VALUES 
      ("500188", "Base Salary", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Base Salary", NULL),
      ("500190", "Base Salary", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Base Salary", NULL),
      ("504988", "Base Salary", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Base Salary", NULL),
      ("500189", "Capitalized Salary (Asset)", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "Capitalized Salary (Asset)", NULL),
      ("500197", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources", NULL),
      ("500198", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources", NULL),
      ("502997", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources", NULL),
      ("502998", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources", NULL),
      ("500197_REF", "IC Shared Resources", "Salary", "Total Opex", "Net Personnel Expenses", "Salary", "IC Shared Resources", NULL),
      ("470288", "Vendor Fundings", "Other", "Total Opex", "Net Personnel Expenses", "Vendor Fundings", "Vendor Fundings", NULL),
      ("500288", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", NULL),
      ("500295", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", NULL),
      ("500296", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", NULL),
      ("502489", "Bonus Employee", "Bonus Employee", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Employee", NULL),
      ("500289", "Bonus Management", "Bonus Management", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Management", NULL),
      ("502490", "Bonus Management", "Bonus Management", "Total Opex", "Net Personnel Expenses", "Total Bonus", "Bonus Management", NULL),
      ("500388", "Vacation / Holidays", "Other", "Total Opex", "Net Personnel Expenses", "Vacation / Holidays", "Vacation / Holidays", NULL),
      ("500397", "Vacation / Holidays", "Other", "Total Opex", "Net Personnel Expenses", "Vacation / Holidays", "Vacation / Holidays", NULL),
      ("500488", "Severance", "Other", "Total Opex", "Net Personnel Expenses", "Severance", "Severance", NULL),
      ("500588", "Share / Option", "Other", "Total Opex", "Net Personnel Expenses", "Share / Option", "Share / Option", NULL),
      ("502488", "National Contribution", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "National Contribution", NULL),
      ("502588", "Pension Benefits", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "Pension Benefits", NULL),
      ("502788", "Employee Insurances", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "Employee Insurances", NULL),
      ("502988", "Other Social Security and Insurances", "Other", "Total Opex", "Net Personnel Expenses", "Social Security", "Other Social Security and Insurances", NULL),
      ("503288", "Personnel - Search Expense", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense", NULL),
      ("503297", "Personnel - Search Expense", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense", NULL),
      ("503298", "Personnel - Search Expense", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Search Expense", NULL),
      ("503188", "Personnel - Training", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Training", NULL),
      ("503988", "Personnel - Other Benefit", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit", NULL),
      ("503997", "Personnel - Other Benefit", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit", NULL),
      ("503998", "Personnel - Other Benefit", "Personnel Related", "Total Opex", "Net Other Expenses", "Total personnel related expense", "Personnel - Other Benefit", NULL),
      ("510988", "Office Rent", "Office Rent / Costs", "Total Opex", "Net Other Expenses", "Office Rent", NULL, NULL),
      ("510998", "Office Rent", "Office Rent / Costs", "Total Opex", "Net Other Expenses", "Office Rent", NULL, NULL),
      ("520988", "Office Cost", "Office Rent / Costs", "Total Opex", "Net Other Expenses", "Office Cost", NULL, NULL),
      ("521188", "Logistics & Warehousing", "Other", "Total Opex", "Net Other Expenses", "Logistics & Warehousing", NULL, NULL),
      ("521197", "Logistics & Warehousing", "Other", "Total Opex", "Net Other Expenses", "Logistics & Warehousing", NULL, NULL),
      ("521988", "Communication", "Other", "Total Opex", "Net Other Expenses", "Communication", NULL, NULL),
      ("521998", "Communication", "Other", "Total Opex", "Net Other Expenses", "Communication", NULL, NULL),
      ("523688", "IT Cost", "IT Costs", "Total Opex", "Net Other Expenses", "Total IT Cost", "IT Cost", NULL),
      ("523697", "I/C Transfer IT Cost (Expense)", "IT Costs", "Total Opex", "Net Other Expenses", "Total IT Cost", "I/C Transfer IT Cost (Expense)", NULL),
      ("523698", "I/C Transfer IT Cost (Income)", "IT Costs", "Total Opex", "Net Other Expenses", "Total IT Cost", "I/C Transfer IT Cost (Income)", NULL),
      ("524988", "Company Insurance", "Other", "Total Opex", "Net Other Expenses", "Company Insurance", NULL, NULL),
      ("524997", "Company Insurance", "Other", "Total Opex", "Net Other Expenses", "Company Insurance", NULL, NULL),
      ("524998", "Company Insurance", "Other", "Total Opex", "Net Other Expenses", "Company Insurance", NULL, NULL),
      ("530988", "Vehicles", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Vehicles", NULL, NULL),
      ("530997", "Vehicles", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Vehicles", NULL, NULL),
      ("530998", "Vehicles", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Vehicles", NULL, NULL),
      ("532988", "Travel", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Travel", NULL, NULL),
      ("532997", "Travel", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Travel", NULL, NULL),
      ("532998", "Travel", "Travel & Vehicle", "Total Opex", "Net Other Expenses", "Travel", NULL, NULL),
      ("540988", "Consulting", "Consulting", "Total Opex", "Net Other Expenses", "Consulting", NULL, NULL),
      ("541988", "Audit Cost", "Other", "Total Opex", "Net Other Expenses", "Audit Cost", NULL, NULL),
      ("543988", "Customer Insurance", "Other", "Total Opex", "Net Other Expenses", "Customer Insurance", NULL, NULL),
      ("561988", "Marketing Cost Infinigate", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate", NULL),
      ("561997", "Marketing Cost Infinigate", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate", NULL),
      ("561998", "Marketing Cost Infinigate", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Cost Infinigate", NULL),
      ("562288", "Marketing Refund Third Pary", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing Refund Third Pary", NULL),
      ("562988", "Marketing COS Third Party", "Marketing", "Total Opex", "Net Other Expenses", "Total Marketing Cost", "Marketing COS Third Party", NULL),
      ("570188", "Non-Refundable VAT", "Other", "Total Opex", "Net Other Expenses", "Non-Refundable VAT", "Non-Refundable VAT", NULL),
      ("581788", "Write-off - Receivables", "Bad Debt", "Total Opex", "Net Other Expenses", "Change in Bad Debt Allowances", "Write-off - Receivables", NULL),
      ("581778", "Change in Bad Debt Allowance", "Bad Debt", "Total Opex", "Net Other Expenses", "Change in Bad Debt Allowances", "Change in Bad Debt Allowance", NULL),
      ("582388", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("582988", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583388", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583397", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583398", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583488", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583888", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583988", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("583998", "Financial Expense", "Other", "Total Opex", "Net Other Expenses", "Financial Expense", NULL, NULL),
      ("544988", "Administrative Services", "Other", "Total Opex", "Net Other Expenses", "Administrative Services", NULL, NULL),
      ("570288", "Tax Other", "Other", "Total Opex", "Net Other Expenses", "Tax Other", NULL, NULL),
      ("570388", "Tax Other", "Other", "Total Opex", "Net Other Expenses", "Tax Other", NULL, NULL),
      ("542898", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("579988", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("580988", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("581488", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("581497", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("581498", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("581588", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("581688", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("581988", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("582888", "Other Cost", "Other", "Total Opex", "Net Other Expenses", "Other Cost", NULL, NULL),
      ("585188", "Net Strategic Projects", "Other OPEX", "Total Opex", "Net Strategic Projects", "Net Strategic Projects", "Net Strategic Projects", NULL),
      ("505188", "Services Received", "Other OPEX", "Total Opex", "Services Received", "Services Received", "Services Received", NULL),
      ("505997", "Services Received", "Other OPEX", "Total Opex", "Services Received", "Services Received", "Services Received", NULL),
      ("505998", "Services Received", "Other OPEX", "Total Opex", "Services Received", "Services Received", "Services Received", NULL)
""")

# COMMAND ----------

# spark.table('opex_account_hierarchy').display()
