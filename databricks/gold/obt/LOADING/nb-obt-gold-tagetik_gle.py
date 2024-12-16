# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW entity_code_map AS
# MAGIC
# MAGIC SELECT * FROM VALUES
# MAGIC ('NULL', 'NULL', 'BE', 'BE1'),
# MAGIC ('NULL', 'NULL', 'NP', 'CH2'),
# MAGIC ('IC-GRP-CH', 'HL3', 'GRP-CH', 'HL3'),
# MAGIC ('IC-FIN-DE', 'HL4', 'FIN-DE', 'HL4'),
# MAGIC ('IC-GRP-DE', 'HL5', 'GRP-DE', 'HL5'),
# MAGIC ('IC-HLD-CH', 'HL1', 'HLD-CH', 'HL1'),
# MAGIC ('IC-HLD-DE', 'HL2', 'HLD-DE', 'HL2'),
# MAGIC ('GRP-TDA-NEW', 'HL99', 'GRP-TDA-NEW', 'HL99'),
# MAGIC ('GRP-TDA', 'HL98', 'GRP-TDA', 'HL98'),
# MAGIC ('IC-HLD-CH-OLD', 'HL97', 'HLD-CH-OLD', 'HL97'),
# MAGIC ('IC-DE', 'DE1', 'DE', 'DE1'),
# MAGIC ('IC-ACM-DE', 'DE2', 'ACM-DE', 'DE2'),
# MAGIC ('IC-NAB-DE', 'DE3', 'NAB-DE', 'DE3'),
# MAGIC ('IC-CH', 'CH1', 'CH', 'CH1'),
# MAGIC ('IC-NET-CH', 'CH2', 'NET-CH', 'CH2'),
# MAGIC ('IC-AT', 'AT1', 'AT', 'AT1'),
# MAGIC ('IC-FI', 'FI1', 'FI', 'FI1'),
# MAGIC ('IC-FR', 'FR1', 'FR', 'FR1'),
# MAGIC ('IC-NL', 'NL1', 'NL', 'NL1'),
# MAGIC ('IC-NO', 'NO1', 'NO', 'NO1'),
# MAGIC ('IC-SE', 'SE1', 'SE', 'SE1'),
# MAGIC ('IC-DK', 'DK1', 'DK', 'DK1'),
# MAGIC ('NULL', 'NULL', 'AC', 'DE2'),
# MAGIC ('IC-UK', 'UK1', 'UK', 'UK1')
# MAGIC
# MAGIC as data 
# MAGIC   (		 old_code
# MAGIC       ,new_code
# MAGIC       ,old_region
# MAGIC       ,new_region)
# MAGIC

# COMMAND ----------

df = spark.sql(
  """
SELECT 
  Scenario                as SCENARIO,
COALESCE(en.new_code, gle.CompanyID)                as COMPANYID,
  CompanyName             as COMPANYNAME,
  FunctionalCur           as FUNCTIONALCUR,
  FiscalYear              as FISCALYEAR,
  FiscalMonth             as FISCALMONTH,
  IncomeBalanceDesc       as INCOMEBALANCEDESC,
  ConsolCreditAcc         as CONSOLCREDITACC,
  COALESCE(ic.new_code, gle.ICPartnerID)           as ICPARTNERID,
  ICPartnerDesc           as ICPARTNERDESC,
  TransactionalCur        as TRANSACTIONALCUR,
  CalendarYear            as CALENDARYEAR,
  CalendarMonth           as CALENDARMONTH,
  Category                as CATEGORY,
  cast(FunctionalAmount as decimal(20,4))          as FUNCTIONALAMOUNT,
  cast(TransactionalAmount as decimal(20,4))       as TRANSACTIONALAMOUNT,
  CostCenter              as COSTCENTER,
  Vendor                  as VENDOR,
  COALESCE(region.new_region, ReportingRegion)     as REPORTINGREGION,
  SpecialDeal             as SPECIALDEAL,
  ProjectDimension        as PROJECTDIMENSION
  'NULL'                    as COD_DEST5
FROM silver_dev.igsql03.igv_azure_general_tagetik_gle_sum_live_total_all as  gle
left join entity_code_map en
  on gle.CompanyID = en.old_code
left join entity_code_map ic
  on gle.ICPartnerID = ic.old_code
left join entity_code_map as region
  on gle.ReportingRegion = region.old_region
where FiscalYear >= year(getdate())
and gle.Sys_Silver_IsCurrent =1
"""
)

df = df.replace("NaN", 'NULL')

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("gold_dev.obt.tagetik_gle")


