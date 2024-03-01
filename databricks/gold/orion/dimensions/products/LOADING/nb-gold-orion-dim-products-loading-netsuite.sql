-- Databricks notebook source
-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW netsuite_products as (
  Select
    *
  from
    netsuite.masterdatasku
  where
    Sys_Silver_IsCurrent
)


-- COMMAND ----------

select
  *
from
  netsuite_products

-- COMMAND ----------

hash(sku_id,vendor_name, 'netsuite')
hash(sku_id,vendor_name, 'isql03')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table("silver_dev.netsuite.masterdatasku").display()

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW datanow_products as (
  Select
    *
  from
    masterdata.datanowarr
  where
    Sys_Silver_IsCurrent
)


-- COMMAND ----------


select
  *
from
  datanow_products
  where SKU = 'Roundoff - FireEye'

-- COMMAND ----------

Select
  *
from
  netsuite_products np
  left join datanow_products dp on np.sku_id = dp.sku
  and np.vendor_name = dp.vendor_name

-- COMMAND ----------

Select * from netsuite_products
where SKU_ID =  'CN-43504-GOLD- 31'

-- COMMAND ----------

Select SKU,Vendor_Name,Sys_Silver_ModifedDateTime_UTC,Sys_Bronze_InsertDateTime_UTC, first(Vendor_Name) OVER (PARTITION BY SKU ORDER BY Sys_Silver_ModifedDateTime_UTC desc, Sys_Bronze_InsertDateTime_UTC desc) AS Last_Vendor_Name from masterdata.datanowarr
where SKU = 'DADS-4001-6000MS'
and Sys_Silver_IsCurrent

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA orion

-- COMMAND ----------


