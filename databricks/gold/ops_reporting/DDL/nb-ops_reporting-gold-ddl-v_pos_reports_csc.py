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
# MAGIC CREATE OR REPLACE VIEW v_pos_reports_csc AS
# MAGIC with distinctitem_cte AS
# MAGIC (
# MAGIC SELECT
# MAGIC 	ROW_NUMBER() OVER (PARTITION BY it.itemid, it.dataareaid ORDER BY it.itemid) rn
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
# MAGIC --	,mg.name					AS itemmodelgroupname
# MAGIC --	,fd.description				AS practicedescr
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtablestaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_vendvendorv2staging WHERE Sys_Silver_IsCurrent = 1) ve
# MAGIC     ON (ve.vendoraccountnumber = it.primaryvendorid AND ve.dataareaid = it.dataareaid)
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventitemgroupstaging WHERE Sys_Silver_IsCurrent = 1) ig
# MAGIC     ON (ig.itemgroupid = it.itemgroupid AND ig.dataareaid = it.dataareaid)
# MAGIC --	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventmodelgroupstaging WHERE Sys_Silver_IsCurrent = 1) mg
# MAGIC --    ON (mg.modelgroupid = it.modelgroupid AND mg.dataareaid = it.dataareaid)
# MAGIC --	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_financialdimensionvalueentitystaging WHERE Sys_Silver_IsCurrent = 1) fd
# MAGIC --    ON (fd.dimensionvalue = it.PRACTICE AND fd.financialdimension = 'Practice')
# MAGIC WHERE LEFT(primaryvendorid,3) = 'VAC'
# MAGIC ),
# MAGIC v_distinctitems AS
# MAGIC (
# MAGIC   SELECT *
# MAGIC   FROM distinctitem_cte
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC local_id_list AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     st.salesid AS salestableid_local,
# MAGIC     sl.inventtransid AS saleslineid_local,
# MAGIC     pt.purchid AS purchtableid_local,
# MAGIC     pl.inventtransid AS purchlineid_local,
# MAGIC     '>> Inter Joins >>' AS hdr,
# MAGIC     upper(pt.intercompanycompanyid) as intercompanycompanyid,
# MAGIC     pl.intercompanyinventtransid
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) st
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
# MAGIC       ON st.salesid    = sl.salesid								--Sales Line Local
# MAGIC      AND st.dataareaid = sl.dataareaid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pl
# MAGIC     ON sl.inventreftransid = pl.inventtransid
# MAGIC     AND sl.dataareaid = pl.dataareaid				--Purchase Line Local
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchtablestaging WHERE Sys_Silver_IsCurrent = 1) pt
# MAGIC     ON pl.purchid = pt.purchid					--Purchase Header Local
# MAGIC     AND pl.dataareaid = pt.dataareaid
# MAGIC ),
# MAGIC so_po_id_list_csc AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     lil.salestableid_local,
# MAGIC     lil.saleslineid_local,
# MAGIC     lil.purchtableid_local,
# MAGIC     lil.purchlineid_local,
# MAGIC     '>> Inter Company Results >>' AS hdr,
# MAGIC     sli.salesid AS salestableid_intercomp,
# MAGIC     sli.inventtransid AS saleslineid_intercomp,
# MAGIC     pli.purchid AS purchtableid_intercomp,
# MAGIC     pli.inventtransid AS purchlineid_intercomp,
# MAGIC 	pli.purchprice as purchprice_intercomp,
# MAGIC 	pli.lineamount as purchlineamount_intercomp,
# MAGIC 	pli.purchqty   as purchqty_intercomp,
# MAGIC 	pli.dataareaid as purchline_dataareaid_intercomp
# MAGIC   FROM local_id_list lil
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sli
# MAGIC     ON lil.intercompanycompanyid = sli.dataareaid
# MAGIC     AND lil.intercompanyinventtransid = sli.inventtransid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pli
# MAGIC     ON sli.inventreftransid = pli.inventtransid
# MAGIC     AND sli.dataareaid = pli.dataareaid
# MAGIC ),
# MAGIC exchangerates AS
# MAGIC (
# MAGIC   SELECT 
# MAGIC     TO_DATE(startdate) AS startdate
# MAGIC     ,tocurrency
# MAGIC     ,fromcurrency
# MAGIC     ,rate
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_exchangerateentitystaging WHERE Sys_Silver_IsCurrent = 1)
# MAGIC     WHERE (fromcurrency = 'USD')
# MAGIC     OR (fromcurrency = 'GBP' AND tocurrency = 'USD')
# MAGIC     OR (fromcurrency = 'GBP' AND tocurrency = 'EUR')
# MAGIC ),
# MAGIC v_ncsc_nuvias_data AS
# MAGIC (
# MAGIC   SELECT 
# MAGIC 	sl.salesid
# MAGIC 	,(CASE
# MAGIC       WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC             THEN SUM(it.qty) OVER (PARTITION BY sl.SALESID, sh.CUSTOMERREF, sl.ITEMID, sh.CUSTACCOUNT, di.ItemName, sl.SAG_RESELLERVENDORID
# MAGIC 	                                      , it.INVENTTRANSID, sh.SAG_NAVSONUMBER, it.DATEPHYSICAL, it.INVENTSERIALID, di.PrimaryVendorName, sl.LINEAMOUNT
# MAGIC 	                                      , sh.SAG_NAVPONUMBER, sh.SAG_NAVSONUMBER, it.INVOICEID, it.STATUSISSUE, psh.PACKINGSLIPID, id.INVENTLOCATIONID
# MAGIC 	                                      , di.ItemGroupName, sl.SAG_SHIPANDDEBIT, pul.PURCHID, sl.SAG_PURCHPRICE, li.PurchTableID_Local, sh.DATAAREAID
# MAGIC 	                                      , id.INVENTLOCATIONID, psh.PACKINGSLIPID, pul.PURCHID, pul.PURCHPRICE, sl.SAG_VENDORREFERENCENUMBER,
# MAGIC                                         sl.SAG_PURCHPRICE, sl.SAG_NAVLINENUM)
# MAGIC       WHEN di.PrimaryVendorName LIKE 'Zyxel%'
# MAGIC             THEN SUM(-1 * it.qty) OVER (PARTITION BY sl.SALESID, sh.CUSTOMERREF, sl.ITEMID, sh.CUSTACCOUNT, di.ItemName, di.ItemDescription, sl.SAG_RESELLERVENDORID
# MAGIC 	                                                  ,it.INVENTTRANSID, sh.SAG_NAVSONUMBER, sl.LINEAMOUNT, sh.SAG_NAVPONUMBER, sh.SAG_NAVSONUMBER, it.INVOICEID, it.STATUSISSUE
# MAGIC                                                     ,psh.PACKINGSLIPID, id.INVENTLOCATIONID, di.ItemGroupName, sl.SAG_SHIPANDDEBIT, pul.PURCHID, sl.SAG_PURCHPRICE, li.PurchTableID_Local
# MAGIC 	                                                  ,sh.DATAAREAID, id.INVENTLOCATIONID, psh.PACKINGSLIPID, pul.PURCHID, pul.PURCHPRICE, sl.SAG_VENDORREFERENCENUMBER, sl.SAG_PURCHPRICE	
# MAGIC 	                                                  ,sl.SAG_NAVLINENUM, sl.SAG_VENDORSTANDARDCOST, sl.SAG_NGS1STANDARDBUYPRICE, sl.SAG_NGS1POBUYPRICE, sl.SAG_UNITCOSTINQUOTECURRENCY)
# MAGIC       ELSE INT(-1 * it.qty)
# MAGIC     END) AS qty
# MAGIC 	,sl.salesqty
# MAGIC 	,sl.itemid
# MAGIC 	,di.itemname
# MAGIC 	,sh.dataareaid
# MAGIC 	,id.inventlocationid
# MAGIC 	,sl.sag_resellervendorid
# MAGIC 	,it.inventtransid
# MAGIC 	,TO_DATE(it.datephysical) as datephysical
# MAGIC 	,sh.sag_createddatetime
# MAGIC 	,sh.sag_navsonumber
# MAGIC 	,sh.sag_navponumber
# MAGIC 	,it.inventserialid
# MAGIC 	,sl.sag_purchprice
# MAGIC 	,li.purchtableid_local
# MAGIC 	,di.primaryvendorid
# MAGIC 	,di.primaryvendorname
# MAGIC 	,di.itemgroupname
# MAGIC 	,di.itemdescription
# MAGIC 	,psh.packingslipid
# MAGIC 	,sh.custaccount
# MAGIC 	,sh.customerref
# MAGIC 	,sl.lineamount
# MAGIC 	,it.invoiceid
# MAGIC 	,it.statusissue
# MAGIC 	,it.statusreceipt
# MAGIC 	,sl.sag_shipanddebit
# MAGIC 	,sh.nuv_navcustomerref
# MAGIC 	,id.wmslocationid
# MAGIC 	,sl.sag_vendorreferencenumber
# MAGIC 	,pul.currencycode
# MAGIC 	,pul.purchid
# MAGIC 	,pul.purchprice
# MAGIC 	,sl.sag_navlinenum
# MAGIC 	,sl.sag_vendorstandardcost
# MAGIC 	,TO_DATE(it.datefinancial) as datefinacial 
# MAGIC 	,sl.sag_ngs1pobuyprice
# MAGIC 	,sl.sag_ngs1standardbuyprice
# MAGIC 	,sl.sag_unitcostinquotecurrency
# MAGIC 	,(CASE WHEN it.qty > 0 THEN BOOLEAN(1)
# MAGIC 		ELSE BOOLEAN(0) END) AS return
# MAGIC 	,sh.sag_reselleremailaddress
# MAGIC 	,sh.sag_euaddress_contact
# MAGIC 	,sh.sag_euaddress_email
# MAGIC 	,sh.nuv_rsaddress_contact
# MAGIC   ,it.recid
# MAGIC   ,(CASE 
# MAGIC       WHEN di.PrimaryVendorName LIKE 'WatchGuard%'
# MAGIC             THEN ROW_NUMBER() OVER(PARTITION BY sl.SALESID,sh.CUSTOMERREF,sl.ITEMID	,sh.CUSTACCOUNT,di.ItemName,di.ItemDescription,sl.SAG_RESELLERVENDORID
# MAGIC                                                   ,it.INVENTTRANSID,sh.SAG_NAVSONUMBER,it.DATEPHYSICAL,it.INVENTSERIALID,di.PrimaryVendorName,it.QTY,sl.LINEAMOUNT
# MAGIC                                                   ,sh.SAG_NAVPONUMBER,sh.SAG_NAVSONUMBER,it.INVOICEID,it.STATUSISSUE,psh.PACKINGSLIPID,id.INVENTLOCATIONID,di.ItemGroupName
# MAGIC 	                                                ,sl.SAG_SHIPANDDEBIT,pul.PURCHID,sl.SAG_PURCHPRICE,li.PurchTableID_Local,sh.DATAAREAID,id.INVENTLOCATIONID,psh.PACKINGSLIPID
# MAGIC 	                                                ,pul.PURCHID	,pul.PURCHPRICE	,sl.SAG_VENDORREFERENCENUMBER	,sl.SAG_PURCHPRICE	,sl.SAG_NAVLINENUM,sl.SAG_VENDORSTANDARDCOST
# MAGIC 	                                                ,sl.SAG_NGS1STANDARDBUYPRICE,sl.SAG_NGS1POBUYPRICE,sl.SAG_UNITCOSTINQUOTECURRENCY ORDER BY sl.SALESID)
# MAGIC       ELSE NULL
# MAGIC     END)      AS  row_number
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl 
# MAGIC     ON sl.INVENTTRANSID = it.INVENTTRANSID
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) sh 
# MAGIC     ON sh.SALESID = sl.SALESID
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventdimstaging WHERE Sys_Silver_IsCurrent = 1) id 
# MAGIC     ON id.INVENTDIMID = it.INVENTDIMID
# MAGIC 	LEFT JOIN v_distinctitems di 
# MAGIC     ON di.ItemID = it.ITEMID 
# MAGIC     AND di.CompanyID = 'NNL2'
# MAGIC 	LEFT JOIN so_po_id_list_csc li 
# MAGIC     ON li.SalesLineID_Local = it.INVENTTRANSID
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pul 
# MAGIC     ON pul.INVENTTRANSID = li.PurchLineID_Local
# MAGIC 	LEFT OUTER JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_custpackingsliptransstaging WHERE Sys_Silver_IsCurrent = 1) psl 
# MAGIC     ON psl.INVENTTRANSID = sl.INVENTTRANSID 
# MAGIC     AND psl.PACKINGSLIPID = it.PACKINGSLIPID
# MAGIC 	LEFT OUTER JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_custpackingslipjourstaging WHERE Sys_Silver_IsCurrent = 1) psh 
# MAGIC     ON psh.PACKINGSLIPID = psl.PACKINGSLIPID
# MAGIC WHERE 
# MAGIC 	it.dataareaid = 'NNL2'
# MAGIC 	AND sh.custaccount IN ('IBE1', 'IFR1', 'IDE1', 'IDK1', 'IFI1', 'INL1', 'INO1', 'ISW1')
# MAGIC 	AND ((it.statusissue = 1)
# MAGIC 		OR (it.statusreceipt = 1))
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC ncsc.PrimaryVendorId                                                                    AS nuvias_vendor_id
# MAGIC , (CASE 
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%'
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%'
# MAGIC           THEN di.PrimaryVendorName
# MAGIC           ELSE ncsc.PrimaryVendorName
# MAGIC   END)                                                                                  AS nuvias_vendor_name
# MAGIC , ncsc.CUSTACCOUNT                                                                      AS customer_account
# MAGIC ,  (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ncsc.SALESID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_sales_order_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.PURCHID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_purchase_order
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.SALESORDERNUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_sales_order_number
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ncsc.DATEPHYSICAL
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_invoice_date
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ifg.CUSTOMERINVOICEDATE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_invoice_date
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ifg.CUSTOMERINVOICEDATE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS invoice_date
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS nuvias_customer_account
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_customer_account
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.MANUFACTURERITEMNUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_part_code_id
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ncsc.ItemName
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_part_code
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.ItemDescription
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_part_code_description
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ncsc.ItemGroupName
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_part_code_category
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.VARID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_partner_id
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ncsc.SAG_RESELLERVENDORID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_partner_id
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ncsc.QTY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_quantity
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN SUM(ifg.QUANTITY) OVER (PARTITION BY ifg.INVOICENUMBER, ifg.CUSTOMERINVOICEDATE, ifg.RESELLERNAME, ifg.RESELLERADDRESS1, ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3,
# MAGIC                                         ifg.RESELLERADDRESSCITY, ifg.RESELLERSTATE, ifg.RESELLERCOUNTRYCODE, ifg.RESELLERZIPCODE, ifg.ENDUSERNAME, ifg.ENDUSERADDRESS1 
# MAGIC                                         ,ifg.ENDUSERADDRESS2, ifg.ENDUSERADDRESS3, ifg.ENDUSERADDRESSCITY, ifg.ENDUSERSTATE, ifg.ENDUSERCOUNTRYCODE, ifg.MANUFACTURERITEMNUMBER	
# MAGIC                                         ,ifg.UNITSELLCURRENCY, ifg.UNITSELLPRICE, ncsc.SAG_VENDORREFERENCENUMBER, ncsc.SALESID, ncsc.PURCHID, ncsc.SAG_PURCHPRICE, ifg.SALESORDERNUMBER
# MAGIC                                         , ncsc.SAG_VENDORSTANDARDCOST, ncsc.SAG_PURCHPRICE, ifg.QUANTITY)
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN ABS(ifg.QUANTITY)
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ifg.QUANTITY
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN (CASE WHEN (ifg.SERIALNUMBER IS NULL) OR (ifg.SERIALNUMBER = '')
# MAGIC                       THEN SUM(ifg.QUANTITY) OVER(PARTITION BY ncsc.CUSTACCOUNT, ifg.CUSTOMERINVOICEDATE, ifg.MANUFACTURERITEMNUMBER, ncsc.ItemDescription, ncsc.ItemGroupName,
# MAGIC                                                     ifg.QUANTITY, ncsc.SAG_RESELLERVENDORID, ifg.RESELLERNAME, ifg.RESELLERCOUNTRYCODE, ifg.RESELLERZIPCODE, ifg.SHIPTONAME,
# MAGIC                                                     ifg.SHIPTOCOUNTRYCODE, ifg.SHIPTOZIPCODE, ifg.SERIALNUMBER, ncsc.SAG_VENDORSTANDARDCOST, ncsc.SAG_VENDORREFERENCENUMBER,
# MAGIC                                                     ncsc.SALESID, ifg.SALESORDERNUMBER, ifg.ENDUSERNAME, ifg.ENDUSERZIPCODE, ifg.ENDUSERCOUNTRYCODE, ncsc.SAG_UNITCOSTINQUOTECURRENCY,
# MAGIC                                                     ncsc.QTY, ifg.RESELLERPONUMBER, ncsc.PURCHID, ncsc.SAG_VENDORREFERENCENUMBER)
# MAGIC                       ELSE ifg.QUANTITY
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_quantity
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN UPPER(ncsc.INVENTSERIALID)
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ncsc.INVENTSERIALID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_serial_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN UPPER(ifg.SERIALNUMBER)
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.SERIALNUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_serial_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
# MAGIC           THEN ncsc.SAG_VENDORREFERENCENUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_vendor_promotion
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ncsc.SAG_VENDORREFERENCENUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_vendor_reference_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN ifg.VENDORADDITIONALDISCOUNT1
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_vendor_additional_discount1
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.SAG_VENDORSTANDARDCOST
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS mspunit_cost
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ifg.UNITSELLCURRENCY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_sell_currency
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ncsc.SAG_UNITCOSTINQUOTECURRENCY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_unit_cost_in_quote_currency
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ncsc.SAG_UNITCOSTINQUOTECURRENCY * ncsc.QTY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_unit_cost_in_quote_currency_total
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.SAG_VENDORSTANDARDCOST * ifg.QUANTITY
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ncsc.SAG_VENDORSTANDARDCOST * ncsc.QTY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS mspunit_total_cost
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.SAG_PURCHPRICE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_purchase_price
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.PURCHPRICE
# MAGIC   ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_po_buy_price
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ncsc.PURCHPRICE * ncsc.QTY
# MAGIC   ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_po_buy_price_total
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN ifg.VENDORBUYPRICE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_vendor_buy_price
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS navision_reseller_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.VATREGISTRATIONNO
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_vat_registration_no
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ifg.RESELLERNAME
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_name
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN CONCAT_WS(' ', ifg.RESELLERADDRESS1,ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3)
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ifg.RESELLERADDRESS1 + ifg.RESELLERADDRESS2 + ifg.RESELLERADDRESSCITY
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN CONCAT_WS(', ', ifg.RESELLERADDRESS1, ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3)
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_address
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ResellerAddress1
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_address1
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ResellerAddress2
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_address2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ifg.ResellerAddress2
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_address3
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.RESELLERADDRESSCITY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_city
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.RESELLERSTATE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_state
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.RESELLERZIPCODE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_postal_code
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.RESELLERCOUNTRYCODE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_country
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ifg.SHIPTONAME
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_name
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ifg.SHIPTOCOUNTRYCODE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_country
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ifg.SHIPTOZIPCODE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_postal_code
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.SHIPTOADDRESS1
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_address1
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.SHIPTOADDRESS2
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_address2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ifg.SHIPTOADDRESS3
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_address3
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.SHIPTOCITY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_city
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.SHIPTOSTATE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_ship_to_state
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ifg.ENDUSERNAME
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_name
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN CONCAT_WS(' ',ifg.ENDUSERADDRESS1, ifg.ENDUSERADDRESS2, ifg.ENDUSERADDRESS3)
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ifg.ENDUSERADDRESS1 + ' ' + ifg.ENDUSERADDRESS2  + ifg.ENDUSERADDRESS3
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_address
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDUSERADDRESS1
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_address1
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDUSERADDRESS2
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_address2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ifg.ENDUSERADDRESS3
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_address3
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDUSERADDRESSCITY
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_city
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDUSERSTATE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_state
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ifg.ENDUSERZIPCODE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_postal_code
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           THEN ifg.ENDUSERCOUNTRYCODE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_end_customer_country
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDCUSTOMERFIRSTNAME
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS navision_end_customer_first_name
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDCUSTOMERLASTNAME
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS navision_end_customer_last_name
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDCUSTOMEREMAILADDRESS
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS navision_end_customer_email
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.ENDCUSTOMERTELEPHONENUMBER
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS navision_end_customer_phone_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ifg.RESELLERPONUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_reseller_po_to_infinigate
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN (CASE 
# MAGIC                     WHEN ncsc.SAG_SHIPANDDEBIT = 0 THEN 'No' 
# MAGIC 	                  WHEN ncsc.SAG_SHIPANDDEBIT = 1 THEN 'Yes'
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_ship_and_debit
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN ncsc.PACKINGSLIPID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_packing_slip_id
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.VENDORCLAIMID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_vendor_claim_id
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ifg.VENDORRESELLERLEVEL
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_vendor_reseller_level
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN (ncsc.SAG_VENDORSTANDARDCOST - ncsc.SAG_PURCHPRICE) * ifg.QUANTITY
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ncsc.PURCHPRICE - ncsc.SAG_PURCHPRICE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS claim_amount
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN 'Infinigate Global Services Ltd'
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS distributor_name
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN 'Nuvias Global Services'
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS distributor_account_name
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ifg.INVOICENUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_invoice_number
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.INVOICELINENUMBER
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_invoice_line_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ifg.UNITSELLPRICE
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_sales_price
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS sales_price_usd
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN (CASE 
# MAGIC                   WHEN ncsc.DATAAREAID= 'NNL2' AND ncsc.INVENTLOCATIONID LIKE '%2' 
# MAGIC 			            THEN '101421538' 
# MAGIC 		              WHEN ncsc.DATAAREAID= 'NGS1' AND ncsc.INVENTLOCATIONID LIKE '%5' OR ncsc.DATAAREAID= 'NGS1'  AND ncsc.INVENTLOCATIONID = 'CORR'
# MAGIC 			            THEN '101417609'
# MAGIC 	              END)
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN (CASE 
# MAGIC                   WHEN ncsc.INVENTLOCATIONID  = 'MAIN2' AND ncsc.DATAAREAID = 'NNL2'
# MAGIC 		              THEN '101421538' -- Venlo ID
# MAGIC 		              ELSE '' 
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS distributer_id_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           THEN (CASE WHEN ifg.ORDERTYPE1 = 'Credit Memo' THEN 'RD' ELSE 'POS' END)
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN (CASE 
# MAGIC                   WHEN 1 * ncsc.QTY > 0 THEN 'POS' 
# MAGIC 		              WHEN -1 * ncsc.QTY <0 THEN 'RD' 
# MAGIC 		              ELSE '' 
# MAGIC 	              END)
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS distributer_transaction_type
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS export_licence_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS business_model1
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS business_model2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
# MAGIC           OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS juniper_var_id2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ncsc.INVENTLOCATIONID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_warehouse
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS distributer_id_no2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS blank1
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS blank2
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS blank3
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS blank4
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS exchange_rate
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nokia_ref
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ncsc.SALESID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_intercompany_sales_order
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS sales_order_date
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ncsc.SAG_PURCHPRICE * ex1.rate
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nuvias_product_cost_eur
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ncsc.recid
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS transaction_record_id
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
# MAGIC           THEN ifg.MANUFACTURERITEMNUMBER
# MAGIC     ELSE NULL 
# MAGIC   END)                                                                                  AS snwl_pn_sku
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN 'Nothing Needed After'
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS nothing_needed_after
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS po_number
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS check
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN '€'
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS local_currency
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC           THEN ncsc.SALESID
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS d365_so
# MAGIC , (CASE
# MAGIC     WHEN ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                                                AS product_unit_price_usd
# MAGIC , (CASE
# MAGIC     WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
# MAGIC           THEN ifg.SALESORDERLINENO
# MAGIC     ELSE NULL
# MAGIC   END)                                                                                  AS navision_sales_order_line_no
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.ifg_posdata WHERE Sys_Silver_IsCurrent = 1) ifg
# MAGIC LEFT JOIN v_DistinctItems di 
# MAGIC       ON (
# MAGIC             (
# MAGIC               UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
# MAGIC               OR di.PrimaryVendorName LIKE 'Zyxel%'
# MAGIC               OR di.PrimaryVendorName LIKE 'WatchGuard%'
# MAGIC             )
# MAGIC             AND di.ItemName = ifg.MANUFACTURERITEMNUMBER 
# MAGIC             AND di.CompanyID = 'NNL2'
# MAGIC          )
# MAGIC FULL JOIN v_ncsc_nuvias_data ncsc
# MAGIC    ON (
# MAGIC         (
# MAGIC             (ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')) -- Cambium
# MAGIC             OR (ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2')) -- Juniper
# MAGIC             OR (ncsc.PrimaryVendorID = 'VAC000904_NNL2') -- Mist
# MAGIC             OR (ncsc.PrimaryVendorName LIKE 'Nokia%') -- Nokia
# MAGIC             OR (ncsc.PrimaryVendorName LIKE 'Prolabs%') -- AddOn
# MAGIC             OR (di.PrimaryVendorName LIKE 'Zyxel%') -- Zyxel
# MAGIC             OR (ncsc.PrimaryVendorName  LIKE 'Smart Optics%') -- SmartOptics
# MAGIC         )
# MAGIC       AND ncsc.SAG_NAVSONUMBER = ifg.SALESORDERNUMBER
# MAGIC       AND ncsc.SAG_NAVLINENUM = ifg.SALESORDERLINENO
# MAGIC       AND ncsc.PACKINGSLIPID = ifg.IGSSHIPMENTNO)
# MAGIC   OR  (
# MAGIC         (UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' AND UPPER(ncsc.PrimaryVendorName) LIKE 'SONICWALL%')
# MAGIC         AND ncsc.SAG_NAVSONUMBER = ifg.SALESORDERNUMBER
# MAGIC         AND ncsc.SAG_NAVLINENUM = ifg.SALESORDERLINENO
# MAGIC         -- AND (ncsc.DATEPHYSICAL BETWEEN DATEADD(day, -31, ifg.CUSTOMERINVOICEDATE) AND DATEADD(day, +1, ifg.CUSTOMERINVOICEDATE))
# MAGIC       )
# MAGIC   OR  (
# MAGIC         (di.PrimaryVendorName LIKE 'WatchGuard%') -- WatchGuard
# MAGIC         AND ncsc.SAG_NAVSONUMBER = ifg.SALESORDERNUMBER
# MAGIC         AND ncsc.SAG_NAVLINENUM = ifg.SALESORDERLINENO
# MAGIC         AND ncsc.PACKINGSLIPID = ifg.IGSSHIPMENTNO
# MAGIC         -- AND (ncsc.DATEPHYSICAL BETWEEN DATEADD(day, -31, ifg.CUSTOMERINVOICEDATE) AND DATEADD(day, +1, ifg.CUSTOMERINVOICEDATE))
# MAGIC       )
# MAGIC LEFT JOIN exchangerates ex1
# MAGIC       ON ex1.fromcurrency = 'GBP'
# MAGIC       AND ex1.tocurrency = 'EUR'
# MAGIC       AND ex1.StartDate = ifg.CUSTOMERINVOICEDATE
# MAGIC
# MAGIC WHERE 1 = 1
# MAGIC AND (
# MAGIC       -- Cambium
# MAGIC       (
# MAGIC         ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
# MAGIC       )
# MAGIC       OR
# MAGIC       -- Juniper
# MAGIC       (
# MAGIC         (ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2'))
# MAGIC         AND ifg.VENDORRESELLERLEVEL LIKE 'Juniper%'
# MAGIC         AND ncsc.INVENTLOCATIONID <> 'DD'
# MAGIC         AND ifg.SHIPANDDEBIT = 1
# MAGIC       )
# MAGIC       OR
# MAGIC       -- Mist
# MAGIC       (
# MAGIC         (ncsc.PrimaryVendorID = 'VAC000904_NNL2')
# MAGIC         AND ((ncsc.INVENTSERIALID IS NOT NULL) OR (ncsc.INVENTSERIALID != 'NaN'))
# MAGIC       )
# MAGIC       OR
# MAGIC       -- Nokia
# MAGIC       (
# MAGIC         (ncsc.PrimaryVendorName LIKE 'Nokia%')
# MAGIC         AND ifg.VENDORRESELLERLEVEL LIKE 'Nokia%'
# MAGIC       )
# MAGIC       OR
# MAGIC       -- SonicWall
# MAGIC       (
# MAGIC         (UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' OR UPPER(ncsc.PrimaryVendorName) LIKE 'SONICWALL%')
# MAGIC         AND ((ifg.RESELLERID IS NOT NULL) OR (ifg.RESELLERID != 'NaN'))
# MAGIC       )
# MAGIC       OR
# MAGIC       -- WatchGuard
# MAGIC       (
# MAGIC         di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC         AND (
# MAGIC               (ncsc.row_number = 1 OR ncsc.row_number IS NULL)
# MAGIC               AND
# MAGIC               (
# MAGIC                 ( 
# MAGIC                   (DATEADD(day, -1, TO_DATE(ifg.CUSTOMERINVOICEDATE)) >= TO_DATE(ncsc.DATEPHYSICAL))
# MAGIC                   OR (DATEADD(day, 0, TO_DATE(ifg.CUSTOMERINVOICEDATE)) = TO_DATE(ncsc.DATEPHYSICAL))
# MAGIC                   OR (DATEADD(day, +1, TO_DATE(ifg.CUSTOMERINVOICEDATE)) <= TO_DATE(ncsc.DATEPHYSICAL))
# MAGIC                 )
# MAGIC                 OR
# MAGIC                 (ncsc.CUSTACCOUNT IS NULL)
# MAGIC               )
# MAGIC             )
# MAGIC       )
# MAGIC       OR
# MAGIC       -- AddOn
# MAGIC       (
# MAGIC         ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC       )
# MAGIC       OR
# MAGIC       -- Zyxel
# MAGIC       (
# MAGIC         di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
# MAGIC         AND ((ifg.VATREGISTRATIONNO IS NOT NULL) OR (ifg.VATREGISTRATIONNO != 'NaN'))
# MAGIC       )
# MAGIC       OR
# MAGIC       -- SmartOptics
# MAGIC       (
# MAGIC         ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
# MAGIC         AND ((ncsc.ItemName IS NOT NULL) OR (ncsc.ItemName != 'NaN'))
# MAGIC       )
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW gold_dev.ops_reporting.v_pos_reports_csc OWNER TO `az_edw_data_engineers_ext_db`
