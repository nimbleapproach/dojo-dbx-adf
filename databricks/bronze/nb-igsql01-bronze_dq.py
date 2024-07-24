# Databricks notebook source
# MAGIC %md
# MAGIC ##### Notebook to produce Basic DQ metrics on a list of igsql01 tables

# COMMAND ----------

# MAGIC %run ../library/nb-enable-imports

# COMMAND ----------

from library.dq_bronze_basic import dq_load_bronze_stats

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------

try:
    EXPLORE_BRONZE = bool(dbutils.widgets.get("wg_explorebronze") == 'True')
except:
    dbutils.widgets.dropdown(name = "wg_explorebronze", defaultValue = 'False', choices =  ['False','True'])
    EXPLORE_BRONZE = bool(dbutils.widgets.get("wg_explorebronze") == 'True')


# COMMAND ----------

database_name = "igsql01"
tables = spark.catalog.listTables(database_name)

# explore a database for all the tables that have been recently reloaded.
if EXPLORE_BRONZE:
    print(f"Recently Loaded at....")
    for table in tables:
        table_name = table.name
        df = spark.table(f"{database_name}.{table_name}")
        df = df.where("Sys_Bronze_InsertDateTime_UTC>'2024-07-01'")
        if df.count() > 0:
            print(f"{database_name}.{table_name}")
else:
    print('skip')

# COMMAND ----------

from collections import namedtuple
DqConf = namedtuple("DqConf", ["full_table_name", "key_column", "attribute_columns"])

# COMMAND ----------

# Define the source tables you wish to perform Basic DQ Check againsts
dq_checks = [
    DqConf(full_table_name="igsql01.accountbase", 
           key_column="AccountId", 
           attribute_columns=["OwningBusinessUnit","CreatedOn","ModifiedOn","AccountNumber","StatusCode",
                              "Name","inf_businessrel_VENDOR","Inf_LastModifiedOn","Inf_CountryId",
                              "inf_businessrel_NPP_COR", "inf_CurrencyCode","inf_businessrel_COMP",
                              "inf_businessrel_NPP","inf_businessrel_PP","inf_customerno",
                              "inf_invoicedispatchtype","inf_businessrelationinfo","inf_businessrel_CUSTOMER",
                              "inf_Name2", "inf_businessrel_endcust","inf_businessrel_SUPPLIER"]),
    DqConf(full_table_name="igsql01.appointmentbase", 
           key_column="ActivityId", 
           attribute_columns=["inf_audience","inf_AppointmentId","inf_followup_appointmentid","inf_TypeOfAppointment",
                              "inf_noofattendees","inf_appointment","Inf_ActiveIdentity","Inf_AppointmentType",
                              "inf_activitytypes","inf_Interest","pm_DatabaseName"]),
    DqConf(full_table_name="igsql01.businessunitbase", 
           key_column="BusinessUnitId", 
           attribute_columns=["Name","ModifiedOn","CreatedOn"]),
    DqConf(full_table_name="igsql01.campaignbase", 
           key_column="CampaignId", 
           attribute_columns=["StageId","CodeName","ExpectedResponse","TotalCampaignActivityActualCost_Base",
                              "ExpectedRevenue_Base","IsTemplate","ActualStart","ActualEnd","StatusCode","CreatedBy",
                              "OwningBusinessUnit","TotalActualCost","TypeCode","ProposedEnd","PriceListId",
                              "ExpectedRevenue","Objective","Description","BudgetedCost","PromotionCodeName",
                              "TotalCampaignActivityActualCost","ExchangeRate","OtherCost","ProcessId",
                              "pm_DatabaseName","ModifiedBy","OwnerId","Name"]),
    DqConf(full_table_name="igsql01.contactbase", 
           key_column="ContactId", 
           attribute_columns=["JobTitle","LastName","CustomerTypeCode","FirstName","MiddleName","NickName","StatusCode",
                              "FullName","ModifiedOn","Telephone1","StateCode","ParentCustomerId","ParentCustomerIdType",
                              "Inf_CountryId","Description","MasterId","ParentCustomerIdName","ProcessId","CreatedOnBehalfBy",
                              "GovernmentId","AnnualIncome","OnHoldTime","SLAId","pm_DatabaseName"]),
    DqConf(full_table_name="igsql01.inf_countrybase", 
           key_column="Inf_countryId", 
           attribute_columns=["CreatedOn","Inf_Description","Inf_name","ModifiedOn"]),
    DqConf(full_table_name="igsql01.inf_interestbase", 
           key_column="Inf_interestId", 
           attribute_columns=["CreatedOn","UTCConversionTimeZoneCode","Inf_name","Inf_Portfolio","inf_VendorDimension",
                              "statecode","pm_DatabaseName","TimeZoneRuleVersionNumber","ModifiedOn","ImportSequenceNumber",
                              "OverriddenCreatedOn","ModifiedOnBehalfBy","Inf_InterestShortcut","inf_ParentInterest","ModifiedBy",
                              "statuscode","OrganizationId","VersionNumber","CreatedOnBehalfBy","CreatedBy"]),
    DqConf(full_table_name="igsql01.inf_interestlinkbase", 
           key_column="CustomerNumber", 
           attribute_columns=["CustomerName"]),
    DqConf(full_table_name="igsql01.opportunitybase", 
           key_column="OpportunityId", 
           attribute_columns=["ModifiedOn","CustomerIdType","CreatedOn","CustomerIdName","ParentAccountId","CustomerId",
                              "New_BusinessCategory","New_ResellerAccountId"]),
    DqConf(full_table_name="igsql01.ownerbase", 
           key_column="OwnerId", 
           attribute_columns=["YomiName","OwnerIdType","Name","pm_DatabaseName","VersionNumber"]),
    DqConf(full_table_name="igsql01.systemuser", 
           key_column="SystemUserId", 
           attribute_columns=[
               "ModifiedByName","EntityImage","Address1_City","OrganizationIdName","Address1_Line2","Address2_Line3",
                              "Address1_Line3","Address1_PostalCode","Address2_Country","EmployeeId", "pm_DatabaseName",
                              "Address1_PostOfficeBox","Address1_Telephone2","Address1_Telephone3","Address2_County","Address2_Latitude",
                              "Address2_Line1","Address2_StateOrProvince","Address2_Telephone1","Address2_Telephone3","MobileAlertEMail",
                              "Address2_City","Address2_Longitude","Address2_ShippingMethodCode","NickName","PersonalEMailAddress",
                              "Address2_Name","Title","PreferredEmailCode","Address1_Name","Address2_PostalCode","JobTitle", "Skill",
                              "Address2_AddressId","FirstName","MiddleName","MobilePhone","Address1_Line1","Address1_ShippingMethodCode",
                              "Address2_AddressTypeCode","Address2_Telephone2","LastName","ModifiedBy","SiteId"
                              ]),
]

# COMMAND ----------

TARGET_TABLE_NAME = "bronze_dq"
TARGET_TABLE_SCHEMA = "dq"
TARGET = f"silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"
RECREATE = True

res = None

for check in dq_checks:
    print(f"\ndq_load_bronze_stats... \n\ttable: {check.full_table_name} \n\tkey: {check.key_column} \n\tattributes: {check.attribute_columns}")
    # execute the function to load the stats for the source table
    res = dq_load_bronze_stats(spark,
                                src_full_table_name=check.full_table_name,
                                column_id=check.key_column,
                                column_attr=check.attribute_columns,
                                recreate=RECREATE)
    print(res)

# COMMAND ----------

# Read the table into a DataFrame
df = spark.table(TARGET)

# Identify and keep the rows with the maximum `as_of` value for each combination of `schema`, `table`, and `col_name`
from pyspark.sql import functions as F
from pyspark.sql.window import Window
window_spec = Window.partitionBy("schema", "table", "col_name").orderBy(F.desc("as_of"))
df_cleaned_dupes = df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") > 1)
#check for dupes
display(df_cleaned_dupes)

if df_cleaned_dupes.count()>1:
# drop if dupes
    df_cleaned = df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1).drop("row_number")
    df_cleaned.write.mode("overwrite").saveAsTable(TARGET)
