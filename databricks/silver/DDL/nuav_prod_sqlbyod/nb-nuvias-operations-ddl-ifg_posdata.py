# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuav_prod_sqlbyod;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ifg_posdata
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,ID BIGINT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,INFINIGATEENTITY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ORDERTYPE1 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ORDERTYPE2 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SALESORDERNUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,INVOICENUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CUSTOMERINVOICEDATE DATE
# MAGIC       COMMENT 'TODO'
# MAGIC     ,INVOICELINENUMBER INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,INFINIGATEITEMNUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MANUFACTURERITEMNUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,QUANTITY INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDORCLAIMID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDORRESELLERLEVEL STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDORADDITIONALDISCOUNT1 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDORADDITIONALDISCOUNT2 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PRODUCTLISTPRICE DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PURCHASECURRENCY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UNITSELLPRICE DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UNITSELLCURRENCY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,POBUYPRICE DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDORBUYPRICE DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERPONUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDORRESELLERREFERENCE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERADDRESS1 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERADDRESS2 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERADDRESS3 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERADDRESSCITY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERSTATE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERZIPCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERCOUNTRYCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTONAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOADDRESS1 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOADDRESS2 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOADDRESS3 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOCITY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOSTATE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOZIPCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPTOCOUNTRYCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERADDRESS1 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERADDRESS2 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERADDRESS3 STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERADDRESSCITY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERSTATE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERZIPCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDUSERCOUNTRYCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SHIPANDDEBIT STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PURCHASEORDERNUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CREATEDDATETIME TIMESTAMP
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SALESORDERLINENO INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SERIALNUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,IGSSHIPMENTNO STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VARID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERFIRSTNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERLASTNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESLLERTELEPHONENUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLEREMAILADDRESS STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDCUSTOMERFIRSTNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDCUSTOMERLASTNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDCUSTOMERTELEPHONENUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ENDCUSTOMEREMAILADDRESS STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RESELLERID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MANUFACTURERPARTNERNO STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VATREGISTRATIONNO STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC     ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC     ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC     ,Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys columns.'
# MAGIC     ,Sys_Silver_IsCurrent BOOLEAN
# MAGIC       COMMENT 'Flag if this is the current version.'
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC       COMMENT 'Flag if this is the deleted version.'
# MAGIC ,CONSTRAINT ifg_posdata_pk PRIMARY KEY(ID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC )
# MAGIC COMMENT 'This table contains the line data for ifg_posdata. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ifg_posdata ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ifg_posdata ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ifg_posdata ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ifg_posdata OWNER TO `az_edw_data_engineers_ext_db`
