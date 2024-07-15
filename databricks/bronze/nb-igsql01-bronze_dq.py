# Databricks notebook source
# MAGIC %md
# MAGIC ##### Notebook to produce Basic DQ metrics on a list of NUVIAS CRM tables

# COMMAND ----------

# MAGIC %run ../library/nb-dq-bronze-basic

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



# COMMAND ----------

# Define the source tables you wish to perform Basic DQ Chesk againsts
data = [
    ("igsql01.accountbase", "AccountId", ["OwningBusinessUnit","CreatedOn","ModifiedOn","AccountNumber","StatusCode","Name","inf_businessrel_VENDOR","Inf_LastModifiedOn","Inf_CountryId","inf_businessrel_NPP_COR","inf_CurrencyCode","inf_businessrel_COMP","inf_businessrel_NPP","inf_businessrel_PP","inf_customerno","inf_invoicedispatchtype","inf_businessrelationinfo","inf_businessrel_CUSTOMER","inf_Name2","inf_businessrel_endcust","inf_businessrel_SUPPLIER"]
),
    ("igsql01.appointmentbase", "ActivityId", ["inf_audience","inf_AppointmentId","inf_followup_appointmentid","inf_TypeOfAppointment","inf_noofattendees","inf_appointment","Inf_ActiveIdentity","Inf_AppointmentType","inf_activitytypes","inf_Interest","pm_DatabaseName"]
),
    ("igsql01.businessunitbase", "BusinessUnitId", ["Name","ModifiedOn","CreatedOn"]
),
    ("igsql01.campaignbase", "CampaignId", ["StageId","CodeName","ExpectedResponse","TotalCampaignActivityActualCost_Base","ExpectedRevenue_Base","IsTemplate","ActualStart","ActualEnd","StatusCode","CreatedBy","OwningBusinessUnit","TotalActualCost","TypeCode","ProposedEnd","PriceListId","ExpectedRevenue","Objective","Description","BudgetedCost","PromotionCodeName","TotalCampaignActivityActualCost","ExchangeRate","OtherCost","ProcessId","pm_DatabaseName","ModifiedBy","OwnerId","Name"]
),
    ("igsql01.contactbase", "ContactId", ["JobTitle","LastName","CustomerTypeCode","FirstName","MiddleName","NickName","StatusCode","FullName","ModifiedOn","Telephone1","StateCode","ParentCustomerId","ParentCustomerIdType","Inf_CountryId","Description","MasterId","ParentCustomerIdName","ProcessId","CreatedOnBehalfBy","GovernmentId","AnnualIncome","OnHoldTime","SLAId","pm_DatabaseName"]
),
    ("igsql01.inf_countrybase", "Inf_countryId", ["CreatedOn","Inf_Description","Inf_name","ModifiedOn"]
),
    ("igsql01.inf_interestbase", "Inf_interestId",["CreatedOn","UTCConversionTimeZoneCode","Inf_name","Inf_Portfolio","inf_VendorDimension","statecode","pm_DatabaseName","TimeZoneRuleVersionNumber","ModifiedOn","ImportSequenceNumber","OverriddenCreatedOn","ModifiedOnBehalfBy","Inf_InterestShortcut","inf_ParentInterest","ModifiedBy","statuscode","OrganizationId","VersionNumber","CreatedOnBehalfBy","CreatedBy"]
),
    ("igsql01.inf_interestlinkbase", "CustomerNumber",["CustomerName"]
),
    ("igsql01.opportunitybase", "OpportunityId", ["ModifiedOn","CustomerIdType","CreatedOn","CustomerIdName","ParentAccountId","CustomerId","New_BusinessCategory","New_ResellerAccountId"]
),
    ("igsql01.ownerbase", "OwnerId", ["YomiName","OwnerIdType","Name","pm_DatabaseName","VersionNumber"]
),
    ("igsql01.systemuser", "VersionNumber", ["ModifiedByName","EntityImage","Address1_City","OrganizationIdName","Address1_Line2","Address2_Line3","Address1_Line3","Address1_PostalCode","Address2_Country","EmployeeId","Skills","pm_DatabaseName","Address1_PostOfficeBox","Address1_Telephone2","Address1_Telephone3","Address2_County","Address2_Latitude","Address2_Line1","Address2_StateOrProvince","Address2_Telephone1","Address2_Telephone3","MobileAlertEMail","Address2_City","Address2_Longitude","Address2_ShippingMethodCode","NickName","PersonalEMailAddress","Address2_Name","Title","PreferredEmailCode","Address1_Name","Address2_PostalCode","JobTitle","Address2_AddressId","FirstName","MiddleName","MobilePhone","Address1_Line1","Address1_ShippingMethodCode","Address2_AddressTypeCode","Address2_Telephone2","LastName","ModifiedBy","SiteId"]
)
]


# Create a DataFrame
df_dq_list = spark.createDataFrame(
    data, ["full_table_name", "key_column", "attribute_columns"]
)

# COMMAND ----------


display(df_dq_list)

# COMMAND ----------



TARGET_TABLE_NAME = "bronze_dq"
TARGET_TABLE_SCHEMA = "dq"
TARGET = f"silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"
RECREATE = True
RECREATE_ALL = False

if RECREATE_ALL:
    # Verify that the table exists
    if spark.catalog.tableExists(TARGET):
        for row in df_dq_list.select("full_table_name").distinct().collect():
            # remove exists stats held within the bronze_dq table for the tables that are about to be processed
            table_name = row["full_table_name"]
            spark.sql(
                f"delete from {TARGET} where COALESCE(schema ,'.',table) ='{table_name}'"
            )


for row in df_dq_list.collect():
    # create paramaters that are being passed into the dq_load_bronze_stats function
    table_name = row["full_table_name"]
    key = row["key_column"]
    attributes = row["attribute_columns"]
    print(f"dq_load_bronze_stats... {table_name} key {key} attributes {attributes}")
    # execute the function to load the stats for the source table
    dq_load_bronze_stats(src_full_table_name=table_name, column_id=key, column_attr=attributes, recreate=RECREATE)

# COMMAND ----------

# Read the table into a DataFrame
df = spark.table("silver_dev.dq.bronze_dq")

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
    df_cleaned.write.mode("overwrite").saveAsTable("silver_dev.dq.bronze_dq")


