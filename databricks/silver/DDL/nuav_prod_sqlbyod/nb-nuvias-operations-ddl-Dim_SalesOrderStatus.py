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
# MAGIC create or replace table Dim_SalesOrderStatus
# MAGIC   (
# MAGIC      SalesOrderStatusID                smallint                                        comment 'Unique identifier for each sales order status'
# MAGIC    , SalesOrderStatusName              string                                          comment 'Name of the sales order status'
# MAGIC    , constraint Dim_SalesOrderStatus primary key(SalesOrderStatusID,SalesOrderStatusName)
# MAGIC   )
# MAGIC comment 'This table represents the various statuses of sales orders. Formerly known as dbo_lu_salesorderstatus.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table Dim_SalesOrderStatus;
# MAGIC
# MAGIC insert into Dim_SalesOrderStatus (SalesOrderStatusID, SalesOrderStatusName) values
# MAGIC      (-1, 'Other')
# MAGIC    , ( 0, 'None')
# MAGIC    , ( 1, 'Back Order')
# MAGIC    , ( 2, 'Delivered')
# MAGIC    , ( 3, 'Invoiced')
# MAGIC    , ( 4, 'Cancelled');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_lu_salesorderstatus OWNER TO `az_edw_data_engineers_ext_db`
