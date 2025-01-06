# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA if not EXISTS ops_reporting;
# MAGIC USE SCHEMA ops_reporting;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_inventory_on_hand_stock_report AS
# MAGIC with item_cte as 
# MAGIC (
# MAGIC SELECT 
# MAGIC     cp.ID
# MAGIC     ,cp.PartNumber
# MAGIC     ,cp.PriceType
# MAGIC     ,cp.Vendor
# MAGIC     ,cp.ItemType
# MAGIC     ,pb.list
# MAGIC     ,pb.price
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.cpq_customprice WHERE Sys_Silver_IsCurrent = 1) cp
# MAGIC     LEFT JOIN (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.cpq_pricebook WHERE Sys_Silver_IsCurrent = 1) pb
# MAGIC         ON pb.partcode = cp.PartNumber 
# MAGIC         AND pb.book = 'Nuvias North'
# MAGIC WHERE Vendor like 'SonicWall%'
# MAGIC ),
# MAGIC distinctitem_cte AS
# MAGIC (
# MAGIC SELECT
# MAGIC 	ROW_NUMBER() OVER(PARTITION BY it.itemid, it.dataareaid ORDER BY it.itemid) rn
# MAGIC 	,it.dataareaid				AS companyid
# MAGIC 	,it.itemid					AS itemid
# MAGIC 	,it.name					AS itemname
# MAGIC 	,it.description				AS itemdescription
# MAGIC 	,it.modelgroupid			AS itemmodelgroupid
# MAGIC 	,it.itemgroupid				AS itemgroupid
# MAGIC 	,it.practice				AS practice
# MAGIC 	,it.primaryvendorid			AS primaryvendorid
# MAGIC 	,ve.vendororganizationname	AS primaryvendorname
# MAGIC 	,ig.name					AS itemgroupname
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtablestaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_vendvendorv2staging WHERE Sys_Silver_IsCurrent = 1) ve
# MAGIC     ON (ve.vendoraccountnumber = it.primaryvendorid AND ve.dataareaid = it.dataareaid)
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventitemgroupstaging WHERE Sys_Silver_IsCurrent = 1) ig
# MAGIC     ON (ig.itemgroupid = it.itemgroupid AND ig.dataareaid = it.dataareaid)
# MAGIC WHERE LEFT(primaryvendorid,3) = 'VAC'
# MAGIC ),
# MAGIC v_distinctitems AS
# MAGIC (
# MAGIC   SELECT *
# MAGIC   FROM distinctitem_cte
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC (CASE 
# MAGIC     WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC     THEN (
# MAGIC         CASE WHEN oh.INVENTORYWAREHOUSEID LIKE '%1' 
# MAGIC         THEN 'Swindon'
# MAGIC         WHEN oh.INVENTORYWAREHOUSEID LIKE '%2'
# MAGIC         THEN 'Venlo' 
# MAGIC         WHEN oh.INVENTORYWAREHOUSEID LIKE '%3' 
# MAGIC         THEN 'Birmingham' 
# MAGIC         WHEN oh.INVENTORYWAREHOUSEID LIKE '%4' 
# MAGIC         THEN 'Gatwick'
# MAGIC         WHEN oh.INVENTORYWAREHOUSEID LIKE '%5'
# MAGIC         THEN 'Egham'
# MAGIC         ELSE oh.INVENTORYWAREHOUSEID END
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC END)                                                                                                                        AS warehouse_name
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.INVENTORYWAREHOUSEID
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS warehouse_id
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.ITEMNUMBER
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS item_number
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.PRODUCTNAME
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS product_name
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.ONHANDQUANTITY
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS on_hand_qty
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.RESERVEDONHANDQUANTITY
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS reserved_on_hand_qty
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.AVAILABLEONHANDQUANTITY
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS available_on_hand_qty
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.DATAAREAID
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS entity
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN di.itemdescription
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS item_description
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN di.PrimaryVendorName
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS vendor_name
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN 'Nuvias Global Services'
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS distribution_name
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN oh.ONORDERQUANTITY
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS on_order_qty
# MAGIC , (CASE 
# MAGIC      WHEN di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC      THEN date_format(current_date(), 'yyyyMMdd')
# MAGIC      ELSE NULL
# MAGIC   END)                                                                                                                       AS inventory_date
# MAGIC , 'TODO'                                                                                                                       AS id
# MAGIC , 'TODO'                                                                                                                       AS status
# MAGIC , 'TODO'                                                                                                                       AS sku
# MAGIC , 'TODO'                                                                                                                       AS search_name
# MAGIC , 'TODO'                                                                                                                       AS b2b
# MAGIC , 'TODO'                                                                                                                       AS coo
# MAGIC , 'TODO'                                                                                                                       AS coveragegroup
# MAGIC , 'TODO'                                                                                                                       AS eccn
# MAGIC , 'TODO'                                                                                                                       AS commoditycode
# MAGIC , 'TODO'                                                                                                                       AS availablephysical
# MAGIC , 'TODO'                                                                                                                       AS ordered
# MAGIC , 'TODO'                                                                                                                       AS onorder
# MAGIC , 'TODO'                                                                                                                       AS totalavailable
# MAGIC , 'TODO'                                                                                                                       AS item_name
# MAGIC , 'TODO'                                                                                                                       AS list_price
# MAGIC , 'TODO'                                                                                                                       AS disti_cost_unit
# MAGIC , 'TODO'                                                                                                                       AS total_inventory_value
# MAGIC , 'TODO'                                                                                                                       AS comments
# MAGIC , 'TODO'                                                                                                                       AS trans_qty
# MAGIC , 'TODO'                                                                                                                       AS serial_number
# MAGIC FROM v_distinctitems di
# MAGIC RIGHT JOIN (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.dbo_inventwarehouseinventorystatusonhandstaging WHERE Sys_Silver_IsCurrent = 1) oh
# MAGIC     ON 
# MAGIC     (
# MAGIC         (
# MAGIC             di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC         )
# MAGIC         AND 
# MAGIC         (
# MAGIC             di.ItemID = oh.ITEMNUMBER 
# MAGIC             AND di.CompanyID = oh.DATAAREAID
# MAGIC         )
# MAGIC     )
# MAGIC
# MAGIC WHERE 1=1
# MAGIC AND 
# MAGIC (
# MAGIC     (
# MAGIC         (
# MAGIC             di.primaryvendorid != 'VAC001400_NNL2' -- SonicWall
# MAGIC             OR di.primaryvendorid NOT IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa
# MAGIC         )
# MAGIC         AND
# MAGIC         (
# MAGIC             oh.DATAAREAID in( 'NGS1','NNL2')
# MAGIC             AND oh.ONHANDQUANTITY > 0
# MAGIC         )
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW gold_dev.ops_reporting.v_inventory_on_hand_stock_report OWNER TO `az_edw_data_engineers_ext_db`
