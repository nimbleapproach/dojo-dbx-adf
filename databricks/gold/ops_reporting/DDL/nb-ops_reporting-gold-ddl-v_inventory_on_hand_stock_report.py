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
# MAGIC WHERE UPPER(Vendor) like 'SONICWALL%'
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
# MAGIC ),
# MAGIC inventdim AS 
# MAGIC (
# MAGIC   SELECT 
# MAGIC     id.INVENTDIMID
# MAGIC 	  ,id.DATAAREAID
# MAGIC 	  ,id.INVENTSERIALID
# MAGIC 	  ,id.INVENTLOCATIONID
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventdimstaging WHERE Sys_Silver_IsCurrent = 1) id
# MAGIC   WHERE id.DATAAREAID IN ('NGS1','NNL2')
# MAGIC   AND id.inventlocationid LIKE 'DEMO%'
# MAGIC   GROUP BY id.DATAAREAID, id.INVENTDIMID, id.INVENTLOCATIONID, id.INVENTSERIALID
# MAGIC ),
# MAGIC inventory_on_hand_all AS 
# MAGIC (
# MAGIC   SELECT
# MAGIC     (CASE WHEN oh.INVENTORYWAREHOUSEID LIKE '%1' 
# MAGIC       THEN 'Swindon'
# MAGIC       WHEN oh.INVENTORYWAREHOUSEID LIKE '%2'
# MAGIC       THEN 'Venlo' 
# MAGIC       WHEN oh.INVENTORYWAREHOUSEID LIKE '%3' 
# MAGIC       THEN 'Birmingham' 
# MAGIC       WHEN oh.INVENTORYWAREHOUSEID LIKE '%4' 
# MAGIC       THEN 'Gatwick'
# MAGIC       WHEN oh.INVENTORYWAREHOUSEID LIKE '%5'
# MAGIC       THEN 'Egham'
# MAGIC       ELSE oh.INVENTORYWAREHOUSEID END)                                                                                          AS warehouse_name
# MAGIC     , oh.INVENTORYWAREHOUSEID                                                                                                    AS warehouse_id
# MAGIC     , oh.ITEMNUMBER                                                                                                              AS item_number
# MAGIC     , oh.PRODUCTNAME                                                                                                             AS product_name
# MAGIC     , oh.ONHANDQUANTITY                                                                                                          AS on_hand_qty
# MAGIC     , oh.RESERVEDONHANDQUANTITY                                                                                                  AS reserved_on_hand_qty
# MAGIC     , oh.AVAILABLEONHANDQUANTITY                                                                                                 AS available_on_hand_qty
# MAGIC     , oh.DATAAREAID                                                                                                              AS entity
# MAGIC     , di.itemname                                                                                                                AS item_name
# MAGIC     , di.itemdescription                                                                                                         AS item_description
# MAGIC     , di.companyid                                                                                                               AS company_id
# MAGIC     , di.PrimaryVendorName                                                                                                       AS vendor_name
# MAGIC     , (CASE 
# MAGIC         WHEN di.primaryvendorname LIKE 'Extreme%'
# MAGIC         THEN 'Nuvias Global Services'
# MAGIC         ELSE NULL
# MAGIC       END)                                                                                                                       AS distribution_name
# MAGIC     , oh.ONORDERQUANTITY                                                                                                         AS on_order_qty
# MAGIC     , di.itemmodelgroupid                                                                                                        AS item_model_group_id
# MAGIC     , date_format(current_date(), 'yyyyMMdd')                                                                                    AS inventory_date
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.dbo_inventwarehouseinventorystatusonhandstaging WHERE Sys_Silver_IsCurrent = 1) oh
# MAGIC LEFT JOIN v_distinctitems di
# MAGIC   ON 
# MAGIC   (
# MAGIC       (
# MAGIC           di.ItemID = oh.ITEMNUMBER 
# MAGIC           AND di.CompanyID = oh.DATAAREAID
# MAGIC       )
# MAGIC   )
# MAGIC WHERE
# MAGIC   oh.DATAAREAID in( 'NGS1','NNL2')
# MAGIC   AND oh.ONHANDQUANTITY > 0
# MAGIC ),
# MAGIC fortinet_stock_de AS 
# MAGIC (
# MAGIC   SELECT
# MAGIC     ft.id
# MAGIC     ,ft.status
# MAGIC     ,ft.itemid
# MAGIC     ,ft.sku
# MAGIC     ,ft.searchname
# MAGIC     ,ft.b2b
# MAGIC     ,ft.coo
# MAGIC     ,ft.coveragegroup
# MAGIC     ,ft.eccn
# MAGIC     ,ft.commoditycode
# MAGIC     ,ft.availablephysical
# MAGIC     ,ft.orderedintotal
# MAGIC     ,ft.onorder
# MAGIC     ,ft.totalavailable
# MAGIC     ,ft.entity
# MAGIC     ,di.primaryvendorname                           AS vendor_name
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.dbo_fortinetstockde WHERE Sys_Silver_IsCurrent = 1) ft
# MAGIC   LEFT JOIN v_distinctitems di
# MAGIC     ON
# MAGIC     (
# MAGIC       ft.itemid = di.itemid
# MAGIC       AND ft.entity = di.companyid
# MAGIC     )
# MAGIC ),
# MAGIC sonicwall_inventory_report AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     oh2.itemnumber                                                                                  AS item_number
# MAGIC     ,di.itemname                                                                                    AS item_name
# MAGIC     ,di.itemdescription                                                                             AS item_description
# MAGIC     ,di.primaryvendorname                                                                           AS vendor_name
# MAGIC     ,INT(oh2.ONHANDQUANTITY)                                                                        AS on_hand_qty
# MAGIC     ,CAST(itc.list AS DECIMAL(10,2))                                                                AS list_price
# MAGIC     ,CAST(itc.list - (itc.list * 0.30) AS DECIMAL(10,2))                                            AS disti_cost_unit
# MAGIC     ,CAST((itc.list - (itc.list * 0.30)) * oh2.ONHANDQUANTITY AS DECIMAL(10,2))                     AS total_inventory_value
# MAGIC     ,INT(oh2.ONORDERQUANTITY)                                                                       AS on_order_qty
# MAGIC     ,oh2.dataareaid                                                                                 AS entity
# MAGIC     ,''                                                                                             AS comments
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.dbo_inventwarehouseinventorystatusonhandv2staging WHERE Sys_Silver_IsCurrent = 1) oh2
# MAGIC   LEFT JOIN v_distinctitems di
# MAGIC     ON 
# MAGIC     (
# MAGIC       di.itemid = oh2.itemnumber
# MAGIC       AND di.companyid IN ('NGS1', 'NNL2')
# MAGIC     )
# MAGIC   LEFT JOIN item_cte itc
# MAGIC     ON 
# MAGIC     (
# MAGIC       itc.partnumber = di.itemname
# MAGIC     )
# MAGIC   WHERE
# MAGIC     di.primaryvendorid = 'VAC001400_NNL2'
# MAGIC     AND oh2.dataareaid = 'NNL2'
# MAGIC ),
# MAGIC versa_stock_report AS 
# MAGIC (
# MAGIC   SELECT
# MAGIC     st.companyid                                        AS entity
# MAGIC     ,st.fk_itemid                                       AS item_number
# MAGIC     ,di.itemname                                        AS sku
# MAGIC     ,INT(SUM(st.transqty))                              AS quantity
# MAGIC     ,st.serialno                                        AS serial_number
# MAGIC     ,st.warehouse                                       AS warehouse
# MAGIC     ,st.location                                        AS location
# MAGIC     ,di.primaryvendorid
# MAGIC     ,di.primaryvendorname                               AS vendor_name
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.ara_fact_stocktrans WHERE Sys_Silver_IsCurrent = 1) st
# MAGIC   LEFT JOIN v_distinctitems di
# MAGIC     ON
# MAGIC     (
# MAGIC       di.itemid = st.fk_itemid
# MAGIC       AND di.companyid = st.companyid
# MAGIC     )
# MAGIC   WHERE 
# MAGIC     st.companyid IN ('NGS1', 'NNL2')
# MAGIC     AND di.primaryvendorid IN ('VAC000850_NGS1', 'VAC000850_NNL2')
# MAGIC   GROUP BY 
# MAGIC     st.companyid
# MAGIC     ,st.fk_itemid
# MAGIC     ,di.itemname
# MAGIC     ,st.serialno
# MAGIC     ,st.warehouse
# MAGIC     ,st.location
# MAGIC     ,di.primaryvendorid
# MAGIC     ,di.primaryvendorname
# MAGIC   HAVING 
# MAGIC     INT(SUM(st.transqty)) > 0
# MAGIC ),
# MAGIC juniper_and_mist_demo_stock
# MAGIC (
# MAGIC   SELECT
# MAGIC     iv.itemid                               AS item_number
# MAGIC     ,di.itemname                            AS item_name
# MAGIC     ,iv.physicalinvent                      AS physical_inventory
# MAGIC     ,iv.reservphysical                      AS reserved_qty
# MAGIC     ,iv.availphysical                       AS available_physical
# MAGIC     ,iv.dataareaid                          AS entity
# MAGIC     ,id.inventlocationid                    AS warehouse_id
# MAGIC     ,id.inventserialid                      AS serial_number
# MAGIC     ,di.primaryvendorname                   AS vendor_name
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventsumstaging WHERE Sys_Silver_IsCurrent = 1) iv
# MAGIC   JOIN inventdim id 
# MAGIC   ON 
# MAGIC   (
# MAGIC     id.inventdimid = iv.inventdimid
# MAGIC   )
# MAGIC   LEFT JOIN v_distinctitems di 
# MAGIC   ON
# MAGIC   (
# MAGIC     di.itemid = iv.itemid
# MAGIC     AND di.companyid = iv.dataareaid
# MAGIC   )
# MAGIC   WHERE 
# MAGIC   iv.dataareaid IN ('NGS1','NNL2')
# MAGIC   AND id.inventlocationid IN ('DEMO5','DEMO2')
# MAGIC   AND di.primaryvendorname LIKE 'Juniper%'
# MAGIC   AND di.itemgroupid IN ('Hardware', 'Accessory')
# MAGIC   AND iv.physicalinvent > 0
# MAGIC )
# MAGIC SELECT 
# MAGIC   ioh.warehouse_name                                                                                                                          AS warehouse_name
# MAGIC   ,COALESCE(ioh.warehouse_id, vs.warehouse, jms.warehouse_id)                                                                                 AS warehouse_id
# MAGIC   ,COALESCE(ioh.item_number, ft.itemid, snwl.item_number, vs.item_number, jms.item_number)                                                    AS item_number
# MAGIC   ,ioh.product_name                                                                                                                           AS product_name
# MAGIC   ,ioh.on_hand_qty                                                                                                                            AS on_hand_qty
# MAGIC   ,snwl.on_hand_qty                                                                                                                           AS on_hand_qty2
# MAGIC   ,ioh.reserved_on_hand_qty                                                                                                                   AS reserved_on_hand_qty
# MAGIC   ,ioh.available_on_hand_qty                                                                                                                  AS available_on_hand_qty
# MAGIC   ,COALESCE(ioh.entity, ft.entity, snwl.entity, jms.entity)                                                                                   AS entity
# MAGIC   ,COALESCE(ioh.item_name, ft.sku, snwl.item_name, vs.sku, jms.item_name)                                                                     AS sku
# MAGIC   ,COALESCE(ioh.item_description, snwl.item_description)                                                                                      AS item_description
# MAGIC   ,ioh.company_id                                                                                                                             AS company_id
# MAGIC   ,COALESCE(ioh.vendor_name, ft.vendor_name, snwl.vendor_name, vs.vendor_name, jms.vendor_name)                                               AS vendor_name
# MAGIC   ,ioh.distribution_name                                                                                                                      AS distribution_name
# MAGIC   ,ioh.on_order_qty                                                                                                                           AS on_order_qty
# MAGIC   ,ioh.on_order_qty                                                                                                                           AS on_order_qty2
# MAGIC   ,ioh.item_model_group_id                                                                                                                    AS item_model_group_id
# MAGIC   ,ioh.inventory_date                                                                                                                         AS inventory_date
# MAGIC   ,ft.id                                                                                                                                      AS id
# MAGIC   ,ft.status                                                                                                                                  AS status
# MAGIC   ,ft.sku                                                                                                                                     AS sku
# MAGIC   ,ft.searchname                                                                                                                              AS searchname
# MAGIC   ,ft.b2b                                                                                                                                     AS b2b
# MAGIC   ,ft.coo                                                                                                                                     AS coo
# MAGIC   ,ft.coveragegroup                                                                                                                           AS coveragegroup
# MAGIC   ,ft.eccn                                                                                                                                    AS eccn
# MAGIC   ,ft.commoditycode                                                                                                                           AS commoditycode
# MAGIC   ,ft.availablephysical                                                                                                                       AS availablephysical
# MAGIC   ,ft.orderedintotal                                                                                                                          AS orderedintotal
# MAGIC   ,ft.onorder                                                                                                                                 AS onorder
# MAGIC   ,ft.totalavailable                                                                                                                          AS totalavailable
# MAGIC   ,list_price                                                                                                                                 AS list_price
# MAGIC   ,disti_cost_unit                                                                                                                            AS disti_cost_unit
# MAGIC   ,total_inventory_value                                                                                                                      AS total_inventory_value
# MAGIC   ,comments                                                                                                                                   AS comments
# MAGIC   ,vs.quantity                                                                                                                                AS quantity
# MAGIC   ,COALESCE(vs.serial_number, jms.serial_number)                                                                                              AS serial_number
# MAGIC   ,vs.location                                                                                                                                AS location
# MAGIC   ,jms.physical_inventory                                                                                                                     AS physical_inventory
# MAGIC   ,jms.reserved_qty                                                                                                                           AS reserved_qty
# MAGIC   ,jms.available_physical
# MAGIC FROM inventory_on_hand_all ioh
# MAGIC FULL JOIN fortinet_stock_de ft
# MAGIC   ON
# MAGIC   (
# MAGIC     ioh.vendor_name LIKE 'Fortinet%'
# MAGIC     AND ft.itemid = ioh.item_number
# MAGIC     AND ioh.entity = ft.entity
# MAGIC   )
# MAGIC FULL JOIN sonicwall_inventory_report snwl
# MAGIC   ON
# MAGIC   (
# MAGIC     UPPER(ioh.vendor_name) LIKE 'SONICWALL%'
# MAGIC     AND ioh.item_number = snwl.item_number
# MAGIC     AND ioh.entity = snwl.entity
# MAGIC   )
# MAGIC FULL JOIN versa_stock_report vs
# MAGIC   ON
# MAGIC   (
# MAGIC     vs.item_number = ioh.item_number
# MAGIC     AND vs.entity = ioh.entity
# MAGIC   )
# MAGIC FULL JOIN juniper_and_mist_demo_stock jms
# MAGIC   ON
# MAGIC   (
# MAGIC     jms.item_number = ioh.item_number
# MAGIC     AND jms.entity = ioh.entity
# MAGIC     AND jms.warehouse_id = ioh.warehouse_id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW gold_dev.ops_reporting.v_inventory_on_hand_stock_report OWNER TO `az_edw_data_engineers_ext_db`
