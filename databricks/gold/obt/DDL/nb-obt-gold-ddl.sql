-- Databricks notebook source
-- DBTITLE 1,Define managementreport at Gold
-- MAGIC %md
-- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
-- MAGIC If there is no widget defined, Data Factory will automatically create them.
-- MAGIC For us while developing we can use the try and except trick here.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The target catalog depens on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA obt;

-- COMMAND ----------

CREATE OR REPLACE TABLE globaltransactions
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,GroupEntityCode STRING NOT NULL 
      COMMENT 'Code to map from with Entity this Transactions came from.'
    ,EntityCode STRING
      COMMENT 'the lower level Entity code for the IG group company - from source ERP but should align to Azienda in Tagetik?'
    ,TransactionDate DATE NOT NULL
        Comment 'Date of the Transaction'
    ,SalesOrderDate DATE
      Comment 'The Date stamp of the SalesOrder (SO) in our ERP source system'
    ,SalesOrderID STRING
        Comment 'Business Key'
    ,SalesOrderItemID STRING
      Comment 'Line item number - The unique identifier of an individual ordered item in a Sales Order'
    ,SKUInternal STRING
      COMMENT 'SKU of the item sold. The Stock Keeping Unit code for the transaction row which uniquely defines the product that is the subject of the row'
    ,SKUMaster STRING
      COMMENT 'SKU of the item sold. [MASTERDATA]'
    ,Description STRING 
      COMMENT 'Description of the item sold.'
    ,ProductTypeInternal STRING 
      COMMENT 'Type of the Item. Originates from our ERP systems'
    ,ProductTypeMaster STRING 
      COMMENT 'TO DO - ? this is the Product Type description from the Data Now ARR Master - is this working for all x4 source views yet as lot of NaN .....?'
    ,CommitmentDuration1Master STRING 
      COMMENT 'TO DO - should be 1 of the 2 alternate names / values from the Datanow master'
    ,CommitmentDuration2Master STRING 
      COMMENT 'TO DO - should be 2 of the 2 alternate names / values from the Datanow master'
    ,BillingFrequencyMaster STRING 
      COMMENT 'TO DO - should be BillingFrequencyMaster from DataNow ARR'
    ,ConsumptionModelMaster STRING 
      COMMENT 'TO DO - should be ConsumptionModelMaster from DataNowARR'
    ,VendorCode STRING 
      COMMENT 'Code of the Vendor. From our ERP systems'
    ,VendorNameInternal STRING 
      COMMENT 'Name of the Vendor. From our ERP systems'
    ,VendorNameMaster STRING 
      COMMENT 'Name of the Vendor. [MASTERDATA]'
    ,VendorGeography STRING 
      COMMENT 'TO DO - SHOULD BE FROM OUR ERP systems and should reflect the ..... JM to confirm the business definition for KPI and YZ to look at technical options in Nav'
    ,VendorStartDate DATE 
      COMMENT 'TO DO - First Date a Vendor sold one item. As discussed this is ERP data and should be the earliest transaction date for any product sold by this vendor to the IG entity of the row (GroupEntityCode)'
    ,ResellerCode STRING 
      COMMENT 'Code of Reseller. From our ERP systems'
    ,ResellerNameInternal STRING 
      COMMENT 'TO DO - Name of Reseller. From our ERP systems - the NAV data column currently has BUG - each Reseller Name has "NaN" as a suffix !!! Fix'
    ,ResellerGeographyInternal STRING 
      COMMENT 'TO DO - the country code for the Reseller. From our ERP data - works for NAV - tbc if works for other ERP systems'
    ,ResellerStartDate DATE 
      COMMENT 'TO DO - should work same as VendorStartDate if we don"t have contract dates between Reseller and IG Entity which I don"t thnk we do. Clumn looks buggy - e.g. 1753-01-01 !!'
    ,ResellerGroupCode STRING 
      COMMENT 'From Reseller Master'
    ,ResellerGroupName STRING 
      COMMENT 'From Reseller Master'
    ,ResellerGroupStartDate DATE 
      COMMENT 'TO DO - Requires data from the Reseller Master source AND from the ERP systems. For a given Reseller Group as defined in the Reseller Master lookup the Group members in the ERP source data and take the earliest start date for that Group - Select Earliest from Aggregate function GROUP'
    ,CurrencyCode STRING
      COMMENT 'Code of the Currency.'
    ,RevenueAmount DECIMAL(10,2)
      COMMENT 'Amount of Revenue.'
    ,CostAmount DECIMAL(10,2)
      COMMENT 'Amount of Cost for calculate GP1.'
    ,GP1 DECIMAL(10,2)
      COMMENT 'Amount of Gross Margin at GP1 level.'
  )
COMMENT 'This table contains the global needed reports data for the management reports as one big table (obt). \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (GroupEntityCode)
