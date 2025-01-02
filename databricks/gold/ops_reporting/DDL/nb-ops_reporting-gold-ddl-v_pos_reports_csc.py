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

spark.sql(f"""
CREATE OR REPLACE VIEW v_pos_reports_csc AS
with distinctitem_cte AS
(
SELECT
	ROW_NUMBER() OVER (PARTITION BY it.itemid, it.dataareaid ORDER BY it.itemid) rn
	,it.dataareaid				AS companyid
	,it.itemid					AS itemid
	,it.name					AS itemname
	,it.description				AS itemdescription
	,it.modelgroupid			AS itemmodelgroupid
	,it.itemgroupid				AS itemgroupid
	,it.practice				AS practice
	,it.primaryvendorid			AS primaryvendorid
	,ve.vendororganizationname	AS primaryvendorname
	,ig.name					AS itemgroupname
--	,mg.name					AS itemmodelgroupname
--	,fd.description				AS practicedescr
FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventtablestaging WHERE Sys_Silver_IsCurrent = 1) it
	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_vendvendorv2staging WHERE Sys_Silver_IsCurrent = 1) ve
    ON (ve.vendoraccountnumber = it.primaryvendorid AND ve.dataareaid = it.dataareaid)
	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventitemgroupstaging WHERE Sys_Silver_IsCurrent = 1) ig
    ON (ig.itemgroupid = it.itemgroupid AND ig.dataareaid = it.dataareaid)
--	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventmodelgroupstaging WHERE Sys_Silver_IsCurrent = 1) mg
--    ON (mg.modelgroupid = it.modelgroupid AND mg.dataareaid = it.dataareaid)
--	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_financialdimensionvalueentitystaging WHERE Sys_Silver_IsCurrent = 1) fd
--    ON (fd.dimensionvalue = it.PRACTICE AND fd.financialdimension = 'Practice')
WHERE LEFT(primaryvendorid,3) = 'VAC'
),
v_distinctitems AS
(
  SELECT *
  FROM distinctitem_cte
  WHERE rn = 1
),
local_id_list AS
(
  SELECT
    st.salesid AS salestableid_local,
    sl.inventtransid AS saleslineid_local,
    pt.purchid AS purchtableid_local,
    pl.inventtransid AS purchlineid_local,
    '>> Inter Joins >>' AS hdr,
    upper(pt.intercompanycompanyid) as intercompanycompanyid,
    pl.intercompanyinventtransid
  FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) st
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
      ON st.salesid    = sl.salesid								--Sales Line Local
     AND st.dataareaid = sl.dataareaid
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pl
    ON sl.inventreftransid = pl.inventtransid
    AND sl.dataareaid = pl.dataareaid				--Purchase Line Local
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_purchtablestaging WHERE Sys_Silver_IsCurrent = 1) pt
    ON pl.purchid = pt.purchid					--Purchase Header Local
    AND pl.dataareaid = pt.dataareaid
),
so_po_id_list_csc AS
(
  SELECT
    lil.salestableid_local,
    lil.saleslineid_local,
    lil.purchtableid_local,
    lil.purchlineid_local,
    '>> Inter Company Results >>' AS hdr,
    sli.salesid AS salestableid_intercomp,
    sli.inventtransid AS saleslineid_intercomp,
    pli.purchid AS purchtableid_intercomp,
    pli.inventtransid AS purchlineid_intercomp,
	pli.purchprice as purchprice_intercomp,
	pli.lineamount as purchlineamount_intercomp,
	pli.purchqty   as purchqty_intercomp,
	pli.dataareaid as purchline_dataareaid_intercomp
  FROM local_id_list lil
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sli
    ON lil.intercompanycompanyid = sli.dataareaid
    AND lil.intercompanyinventtransid = sli.inventtransid
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pli
    ON sli.inventreftransid = pli.inventtransid
    AND sli.dataareaid = pli.dataareaid
),
exchangerates AS
(
  SELECT 
    TO_DATE(startdate) AS startdate
    ,tocurrency
    ,fromcurrency
    ,rate
  FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_exchangerateentitystaging WHERE Sys_Silver_IsCurrent = 1)
    WHERE (fromcurrency = 'USD')
    OR (fromcurrency = 'GBP' AND tocurrency = 'USD')
    OR (fromcurrency = 'GBP' AND tocurrency = 'EUR')
),
v_ncsc_nuvias_data AS
(
  SELECT 
	sl.salesid
	,(CASE
      WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
            THEN SUM(it.qty) OVER (PARTITION BY sl.SALESID, sh.CUSTOMERREF, sl.ITEMID, sh.CUSTACCOUNT, di.ItemName, sl.SAG_RESELLERVENDORID
	                                      , it.INVENTTRANSID, sh.SAG_NAVSONUMBER, it.DATEPHYSICAL, it.INVENTSERIALID, di.PrimaryVendorName, sl.LINEAMOUNT
	                                      , sh.SAG_NAVPONUMBER, sh.SAG_NAVSONUMBER, it.INVOICEID, it.STATUSISSUE, psh.PACKINGSLIPID, id.INVENTLOCATIONID
	                                      , di.ItemGroupName, sl.SAG_SHIPANDDEBIT, pul.PURCHID, sl.SAG_PURCHPRICE, li.PurchTableID_Local, sh.DATAAREAID
	                                      , id.INVENTLOCATIONID, psh.PACKINGSLIPID, pul.PURCHID, pul.PURCHPRICE, sl.SAG_VENDORREFERENCENUMBER,
                                        sl.SAG_PURCHPRICE, sl.SAG_NAVLINENUM)
      WHEN di.PrimaryVendorName LIKE 'Zyxel%'
            THEN SUM(-1 * it.qty) OVER (PARTITION BY sl.SALESID, sh.CUSTOMERREF, sl.ITEMID, sh.CUSTACCOUNT, di.ItemName, di.ItemDescription, sl.SAG_RESELLERVENDORID
	                                                  ,it.INVENTTRANSID, sh.SAG_NAVSONUMBER, sl.LINEAMOUNT, sh.SAG_NAVPONUMBER, sh.SAG_NAVSONUMBER, it.INVOICEID, it.STATUSISSUE
                                                    ,psh.PACKINGSLIPID, id.INVENTLOCATIONID, di.ItemGroupName, sl.SAG_SHIPANDDEBIT, pul.PURCHID, sl.SAG_PURCHPRICE, li.PurchTableID_Local
	                                                  ,sh.DATAAREAID, id.INVENTLOCATIONID, psh.PACKINGSLIPID, pul.PURCHID, pul.PURCHPRICE, sl.SAG_VENDORREFERENCENUMBER, sl.SAG_PURCHPRICE	
	                                                  ,sl.SAG_NAVLINENUM, sl.SAG_VENDORSTANDARDCOST, sl.SAG_NGS1STANDARDBUYPRICE, sl.SAG_NGS1POBUYPRICE, sl.SAG_UNITCOSTINQUOTECURRENCY)
      ELSE INT(-1 * it.qty)
    END) AS qty
	,sl.salesqty
	,sl.itemid
	,di.itemname
	,sh.dataareaid
	,id.inventlocationid
	,sl.sag_resellervendorid
	,it.inventtransid
	,TO_DATE(it.datephysical) as datephysical
	,sh.sag_createddatetime
	,sh.sag_navsonumber
	,sh.sag_navponumber
	,it.inventserialid
	,sl.sag_purchprice
	,li.purchtableid_local
	,di.primaryvendorid
	,di.primaryvendorname
	,di.itemgroupname
	,di.itemdescription
	,psh.packingslipid
	,sh.custaccount
	,sh.customerref
	,sl.lineamount
	,it.invoiceid
	,it.statusissue
	,it.statusreceipt
	,sl.sag_shipanddebit
	,sh.nuv_navcustomerref
	,id.wmslocationid
	,sl.sag_vendorreferencenumber
	,pul.currencycode
	,pul.purchid
	,pul.purchprice
	,sl.sag_navlinenum
	,sl.sag_vendorstandardcost
	,TO_DATE(it.datefinancial) as datefinacial 
	,sl.sag_ngs1pobuyprice
	,sl.sag_ngs1standardbuyprice
	,sl.sag_unitcostinquotecurrency
	,(CASE WHEN it.qty > 0 THEN BOOLEAN(1)
		ELSE BOOLEAN(0) END) AS return
	,sh.sag_reselleremailaddress
	,sh.sag_euaddress_contact
	,sh.sag_euaddress_email
	,sh.nuv_rsaddress_contact
  ,it.recid
  ,(CASE 
      WHEN di.PrimaryVendorName LIKE 'WatchGuard%'
            THEN ROW_NUMBER() OVER(PARTITION BY sl.SALESID,sh.CUSTOMERREF,sl.ITEMID	,sh.CUSTACCOUNT,di.ItemName,di.ItemDescription,sl.SAG_RESELLERVENDORID
                                                  ,it.INVENTTRANSID,sh.SAG_NAVSONUMBER,it.DATEPHYSICAL,it.INVENTSERIALID,di.PrimaryVendorName,it.QTY,sl.LINEAMOUNT
                                                  ,sh.SAG_NAVPONUMBER,sh.SAG_NAVSONUMBER,it.INVOICEID,it.STATUSISSUE,psh.PACKINGSLIPID,id.INVENTLOCATIONID,di.ItemGroupName
	                                                ,sl.SAG_SHIPANDDEBIT,pul.PURCHID,sl.SAG_PURCHPRICE,li.PurchTableID_Local,sh.DATAAREAID,id.INVENTLOCATIONID,psh.PACKINGSLIPID
	                                                ,pul.PURCHID	,pul.PURCHPRICE	,sl.SAG_VENDORREFERENCENUMBER	,sl.SAG_PURCHPRICE	,sl.SAG_NAVLINENUM,sl.SAG_VENDORSTANDARDCOST
	                                                ,sl.SAG_NGS1STANDARDBUYPRICE,sl.SAG_NGS1POBUYPRICE,sl.SAG_UNITCOSTINQUOTECURRENCY ORDER BY sl.SALESID)
      ELSE NULL
    END)      AS  row_number
FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl 
    ON sl.INVENTTRANSID = it.INVENTTRANSID
	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) sh 
    ON sh.SALESID = sl.SALESID
	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventdimstaging WHERE Sys_Silver_IsCurrent = 1) id 
    ON id.INVENTDIMID = it.INVENTDIMID
	LEFT JOIN v_distinctitems di 
    ON di.ItemID = it.ITEMID 
    AND di.CompanyID = 'NNL2'
	LEFT JOIN so_po_id_list_csc li 
    ON li.SalesLineID_Local = it.INVENTTRANSID
	LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pul 
    ON pul.INVENTTRANSID = li.PurchLineID_Local
	LEFT OUTER JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingsliptransstaging WHERE Sys_Silver_IsCurrent = 1) psl 
    ON psl.INVENTTRANSID = sl.INVENTTRANSID 
    AND psl.PACKINGSLIPID = it.PACKINGSLIPID
	LEFT OUTER JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingslipjourstaging WHERE Sys_Silver_IsCurrent = 1) psh 
    ON psh.PACKINGSLIPID = psl.PACKINGSLIPID
WHERE 
	it.dataareaid = 'NNL2'
	AND sh.custaccount IN ('IBE1', 'IFR1', 'IDE1', 'IDK1', 'IFI1', 'INL1', 'INO1', 'ISW1')
	AND ((it.statusissue = 1)
		OR (it.statusreceipt = 1))
)
SELECT DISTINCT
ncsc.PrimaryVendorId                                                                    AS nuvias_vendor_id
, (CASE 
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
          OR di.PrimaryVendorName LIKE 'Zyxel%'
          OR di.PrimaryVendorName LIKE 'WatchGuard%'
          THEN di.PrimaryVendorName
          ELSE ncsc.PrimaryVendorName
  END)                                                                                  AS nuvias_vendor_name
, ncsc.CUSTACCOUNT                                                                      AS customer_account
,  (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ncsc.SALESID
    ELSE NULL
  END)                                                                                  AS nuvias_sales_order_number
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.PURCHID
    ELSE NULL
  END)                                                                                  AS nuvias_purchase_order
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.SALESORDERNUMBER
    ELSE NULL
  END)                                                                                  AS navision_sales_order_number
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.DATEPHYSICAL
    ELSE NULL
  END)                                                                                  AS nuvias_invoice_date
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ifg.CUSTOMERINVOICEDATE
    ELSE NULL
  END)                                                                                  AS navision_invoice_date
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN date_format(ifg.CUSTOMERINVOICEDATE, 'dd/MM/yyyy')
    ELSE NULL
  END)                                                                                  AS navision_invoice_date2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ifg.CUSTOMERINVOICEDATE
    ELSE NULL
  END)                                                                                  AS invoice_date
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ''
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.CUSTACCOUNT
    ELSE NULL
    END)                                                                                AS nuvias_customer_account
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ''
    ELSE NULL
  END)                                                                                  AS navision_customer_account
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.MANUFACTURERITEMNUMBER
    ELSE NULL
  END)                                                                                  AS navision_part_code_id
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ncsc.ItemName
    ELSE NULL
  END)                                                                                  AS nuvias_part_code
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.ItemDescription
    ELSE NULL
  END)                                                                                  AS nuvias_part_code_description
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.ItemGroupName
    ELSE NULL
  END)                                                                                  AS nuvias_part_code_category
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.VARID
    ELSE NULL
  END)                                                                                  AS navision_partner_id
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.SAG_RESELLERVENDORID
    ELSE NULL
  END)                                                                                  AS nuvias_partner_id
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ncsc.QTY
    ELSE NULL
  END)                                                                                  AS nuvias_quantity
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN SUM(ifg.QUANTITY) OVER (PARTITION BY ifg.INVOICENUMBER, ifg.CUSTOMERINVOICEDATE, ifg.RESELLERNAME, ifg.RESELLERADDRESS1, ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3,
                                        ifg.RESELLERADDRESSCITY, ifg.RESELLERSTATE, ifg.RESELLERCOUNTRYCODE, ifg.RESELLERZIPCODE, ifg.ENDUSERNAME, ifg.ENDUSERADDRESS1 
                                        ,ifg.ENDUSERADDRESS2, ifg.ENDUSERADDRESS3, ifg.ENDUSERADDRESSCITY, ifg.ENDUSERSTATE, ifg.ENDUSERCOUNTRYCODE, ifg.MANUFACTURERITEMNUMBER	
                                        ,ifg.UNITSELLCURRENCY, ifg.UNITSELLPRICE, ncsc.SAG_VENDORREFERENCENUMBER, ncsc.SALESID, ncsc.PURCHID, ncsc.SAG_PURCHPRICE, ifg.SALESORDERNUMBER
                                        , ncsc.SAG_VENDORSTANDARDCOST, ncsc.SAG_PURCHPRICE, ifg.QUANTITY)
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN ABS(ifg.QUANTITY)
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ifg.QUANTITY
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN (CASE WHEN (ifg.SERIALNUMBER IS NULL) OR (ifg.SERIALNUMBER = '')
                      THEN SUM(ifg.QUANTITY) OVER(PARTITION BY ncsc.CUSTACCOUNT, ifg.CUSTOMERINVOICEDATE, ifg.MANUFACTURERITEMNUMBER, ncsc.ItemDescription, ncsc.ItemGroupName,
                                                    ifg.QUANTITY, ncsc.SAG_RESELLERVENDORID, ifg.RESELLERNAME, ifg.RESELLERCOUNTRYCODE, ifg.RESELLERZIPCODE, ifg.SHIPTONAME,
                                                    ifg.SHIPTOCOUNTRYCODE, ifg.SHIPTOZIPCODE, ifg.SERIALNUMBER, ncsc.SAG_VENDORSTANDARDCOST, ncsc.SAG_VENDORREFERENCENUMBER,
                                                    ncsc.SALESID, ifg.SALESORDERNUMBER, ifg.ENDUSERNAME, ifg.ENDUSERZIPCODE, ifg.ENDUSERCOUNTRYCODE, ncsc.SAG_UNITCOSTINQUOTECURRENCY,
                                                    ncsc.QTY, ifg.RESELLERPONUMBER, ncsc.PURCHID, ncsc.SAG_VENDORREFERENCENUMBER)
                      ELSE ifg.QUANTITY
                END)
    ELSE NULL
  END)                                                                                  AS navision_quantity
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN UPPER(ncsc.INVENTSERIALID)
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ncsc.INVENTSERIALID
    ELSE NULL
  END)                                                                                  AS nuvias_serial_number
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN UPPER(ifg.SERIALNUMBER)
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.SERIALNUMBER
    ELSE NULL
  END)                                                                                  AS navision_serial_number
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
          THEN ncsc.SAG_VENDORREFERENCENUMBER
    ELSE NULL
  END)                                                                                  AS nuvias_vendor_promotion
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ncsc.SAG_VENDORREFERENCENUMBER
    ELSE NULL
  END)                                                                                  AS nuvias_vendor_reference_number
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN ifg.VENDORADDITIONALDISCOUNT1
    ELSE NULL
  END)                                                                                  AS navision_vendor_additional_discount1
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.SAG_VENDORSTANDARDCOST
    ELSE NULL
  END)                                                                                  AS mspunit_cost
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ifg.UNITSELLCURRENCY
    ELSE NULL
  END)                                                                                  AS navision_sell_currency
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.SAG_UNITCOSTINQUOTECURRENCY
    ELSE NULL
  END)                                                                                  AS nuvias_unit_cost_in_quote_currency
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.SAG_UNITCOSTINQUOTECURRENCY * ncsc.QTY
    ELSE NULL
  END)                                                                                  AS nuvias_unit_cost_in_quote_currency_total
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.SAG_VENDORSTANDARDCOST * ifg.QUANTITY
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ncsc.SAG_VENDORSTANDARDCOST * ncsc.QTY
    ELSE NULL
  END)                                                                                  AS mspunit_total_cost
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.SAG_PURCHPRICE
    ELSE NULL
  END)                                                                                  AS nuvias_purchase_price
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.PURCHPRICE
  ELSE NULL
  END)                                                                                  AS nuvias_po_buy_price
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ncsc.PURCHPRICE * ncsc.QTY
  ELSE NULL
  END)                                                                                  AS nuvias_po_buy_price_total
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN ifg.VENDORBUYPRICE
    ELSE NULL
  END)                                                                                  AS navision_vendor_buy_price
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ''
    ELSE NULL
    END)                                                                                AS navision_reseller_number
, (CASE
    WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.VATREGISTRATIONNO
    ELSE NULL
  END)                                                                                  AS navision_vat_registration_no
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ifg.RESELLERNAME
    ELSE NULL
  END)                                                                                  AS navision_reseller_name
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN CONCAT_WS(' ', ifg.RESELLERADDRESS1,ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3)
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ifg.RESELLERADDRESS1 + ifg.RESELLERADDRESS2 + ifg.RESELLERADDRESSCITY
    WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN CONCAT_WS(', ', ifg.RESELLERADDRESS1, ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3)
    ELSE NULL
  END)                                                                                  AS navision_reseller_address
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ResellerAddress1
    ELSE NULL
  END)                                                                                  AS navision_reseller_address1
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ResellerAddress2
    ELSE NULL
  END)                                                                                  AS navision_reseller_address2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ifg.ResellerAddress3
    ELSE NULL
  END)                                                                                  AS navision_reseller_address3
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.RESELLERADDRESSCITY
    ELSE NULL
  END)                                                                                  AS navision_reseller_city
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.RESELLERSTATE
    ELSE NULL
  END)                                                                                  AS navision_reseller_state
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.RESELLERZIPCODE
    ELSE NULL
  END)                                                                                  AS navision_reseller_postal_code
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.RESELLERCOUNTRYCODE
    ELSE NULL
  END)                                                                                  AS navision_reseller_country
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ifg.SHIPTONAME
    ELSE NULL
  END)                                                                                  AS navision_ship_to_name
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ifg.SHIPTOCOUNTRYCODE
    ELSE NULL
  END)                                                                                  AS navision_ship_to_country
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ifg.SHIPTOZIPCODE
    ELSE NULL
  END)                                                                                  AS navision_ship_to_postal_code
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.SHIPTOADDRESS1
    ELSE NULL
  END)                                                                                  AS navision_ship_to_address1
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.SHIPTOADDRESS2
    ELSE NULL
  END)                                                                                  AS navision_ship_to_address2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ifg.SHIPTOADDRESS3
    ELSE NULL
  END)                                                                                  AS navision_ship_to_address3
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.SHIPTOCITY
    ELSE NULL
  END)                                                                                  AS navision_ship_to_city
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.SHIPTOSTATE
    ELSE NULL
  END)                                                                                  AS navision_ship_to_state
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ifg.ENDUSERNAME
    ELSE NULL
  END)                                                                                  AS navision_end_customer_name
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN CONCAT_WS(' ',ifg.ENDUSERADDRESS1, ifg.ENDUSERADDRESS2, ifg.ENDUSERADDRESS3)
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ifg.ENDUSERADDRESS1 + ' ' + ifg.ENDUSERADDRESS2  + ifg.ENDUSERADDRESS3
    ELSE NULL
  END)                                                                                  AS navision_end_customer_address
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDUSERADDRESS1
    ELSE NULL
  END)                                                                                  AS navision_end_customer_address1
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDUSERADDRESS2
    ELSE NULL
  END)                                                                                  AS navision_end_customer_address2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ifg.ENDUSERADDRESS3
    ELSE NULL
  END)                                                                                  AS navision_end_customer_address3
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDUSERADDRESSCITY
    ELSE NULL
  END)                                                                                  AS navision_end_customer_city
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDUSERSTATE
    ELSE NULL
  END)                                                                                  AS navision_end_customer_state
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ifg.ENDUSERZIPCODE
    ELSE NULL
  END)                                                                                  AS navision_end_customer_postal_code
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          THEN ifg.ENDUSERCOUNTRYCODE
    ELSE NULL
  END)                                                                                  AS navision_end_customer_country
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDCUSTOMERFIRSTNAME
    ELSE NULL
    END)                                                                                AS navision_end_customer_first_name
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDCUSTOMERLASTNAME
    ELSE NULL
    END)                                                                                AS navision_end_customer_last_name
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDCUSTOMEREMAILADDRESS
    ELSE NULL
    END)                                                                                AS navision_end_customer_email
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.ENDCUSTOMERTELEPHONENUMBER
    ELSE NULL
    END)                                                                                AS navision_end_customer_phone_number
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ifg.RESELLERPONUMBER
    ELSE NULL
  END)                                                                                  AS navision_reseller_po_to_infinigate
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN (CASE 
                    WHEN ncsc.SAG_SHIPANDDEBIT = 0 THEN 'No' 
	                  WHEN ncsc.SAG_SHIPANDDEBIT = 1 THEN 'Yes'
                END)
    ELSE NULL
  END)                                                                                  AS nuvias_ship_and_debit
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN ncsc.PACKINGSLIPID
    ELSE NULL
  END)                                                                                  AS nuvias_packing_slip_id
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.VENDORCLAIMID
    ELSE NULL
  END)                                                                                  AS navision_vendor_claim_id
, (CASE
    WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ifg.VENDORRESELLERLEVEL
    ELSE NULL
  END)                                                                                  AS navision_vendor_reseller_level
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN (ncsc.SAG_VENDORSTANDARDCOST - ncsc.SAG_PURCHPRICE) * ifg.QUANTITY
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ncsc.PURCHPRICE - ncsc.SAG_PURCHPRICE
    ELSE NULL
  END)                                                                                  AS claim_amount
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN 'Infinigate Global Services Ltd'
    ELSE NULL
  END)                                                                                  AS distributor_name
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN 'Nuvias Global Services'
    ELSE NULL
  END)                                                                                  AS distributor_account_name
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          OR ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ifg.INVOICENUMBER
    ELSE NULL
  END)                                                                                  AS navision_invoice_number
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.INVOICELINENUMBER
    ELSE NULL
  END)                                                                                  AS navision_invoice_line_number
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ifg.UNITSELLPRICE
    ELSE NULL
  END)                                                                                  AS navision_sales_price
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN ''
    ELSE NULL
  END)                                                                                  AS sales_price_usd
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN (CASE 
                  WHEN ncsc.DATAAREAID= 'NNL2' AND ncsc.INVENTLOCATIONID LIKE '%2' 
			            THEN '101421538' 
		              WHEN ncsc.DATAAREAID= 'NGS1' AND ncsc.INVENTLOCATIONID LIKE '%5' OR ncsc.DATAAREAID= 'NGS1'  AND ncsc.INVENTLOCATIONID = 'CORR'
			            THEN '101417609'
	              END)
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN (CASE 
                  WHEN ncsc.INVENTLOCATIONID  = 'MAIN2' AND ncsc.DATAAREAID = 'NNL2'
		              THEN '101421538' -- Venlo ID
		              ELSE '' 
                END)
    ELSE NULL
  END)                                                                                  AS distributer_id_number
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          THEN (CASE WHEN ifg.ORDERTYPE1 = 'Credit Memo' THEN 'RD' ELSE 'POS' END)
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN (CASE 
                  WHEN 1 * ncsc.QTY > 0 THEN 'POS' 
		              WHEN -1 * ncsc.QTY <0 THEN 'RD' 
		              ELSE '' 
	              END)
    ELSE NULL
  END)                                                                                  AS distributer_transaction_type
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ''
    ELSE NULL
  END)                                                                                  AS export_licence_number
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ''
    ELSE NULL
  END)                                                                                  AS business_model1
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ''
    ELSE NULL
  END)                                                                                  AS business_model2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2') -- Juniper
          OR ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ''
    ELSE NULL
  END)                                                                                  AS juniper_var_id2
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ncsc.INVENTLOCATIONID
    ELSE NULL
  END)                                                                                  AS nuvias_warehouse
, (CASE
    WHEN ncsc.PrimaryVendorID = 'VAC000904_NNL2' -- Mist
          THEN ''
    ELSE NULL
  END)                                                                                  AS distributer_id_no2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                                                  AS blank1
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                                                  AS blank2
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                                                  AS blank3
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                                                  AS blank4
, (CASE
    WHEN ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ''
    ELSE NULL
  END)                                                                                  AS exchange_rate
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                                                  AS nokia_ref
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ncsc.SALESID
    ELSE NULL
  END)                                                                                  AS nuvias_intercompany_sales_order
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                                                  AS sales_order_date
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ncsc.SAG_PURCHPRICE * ex1.rate
    ELSE NULL
  END)                                                                                  AS nuvias_product_cost_eur
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ncsc.recid
    ELSE NULL
  END)                                                                                  AS transaction_record_id
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' -- SonicWall
          THEN ifg.MANUFACTURERITEMNUMBER
    ELSE NULL 
  END)                                                                                  AS snwl_pn_sku
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN 'Nothing Needed After'
    ELSE NULL
  END)                                                                                  AS nothing_needed_after
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ''
    ELSE NULL
  END)                                                                                  AS po_number
, (CASE
    WHEN ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          THEN ''
    ELSE NULL
  END)                                                                                  AS check
, (CASE
    WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN '€'
    ELSE NULL
  END)                                                                                  AS local_currency
, (CASE
    WHEN di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
          THEN ncsc.SALESID
    ELSE NULL
  END)                                                                                  AS d365_so
, (CASE
    WHEN ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
          THEN ''
    ELSE NULL
    END)                                                                                AS product_unit_price_usd
, (CASE
    WHEN UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
          THEN ifg.SALESORDERLINENO
    ELSE NULL
  END)                                                                                  AS navision_sales_order_line_no
FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.ifg_posdata WHERE Sys_Silver_IsCurrent = 1) ifg
LEFT JOIN v_DistinctItems di 
      ON (
            (
              UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%'
              OR di.PrimaryVendorName LIKE 'Zyxel%'
              OR di.PrimaryVendorName LIKE 'WatchGuard%'
            )
            AND di.ItemName = ifg.MANUFACTURERITEMNUMBER 
            AND di.CompanyID = 'NNL2'
         )
FULL JOIN v_ncsc_nuvias_data ncsc
   ON (
        (
            (ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')) -- Cambium
            OR (ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2')) -- Juniper
            OR (ncsc.PrimaryVendorID = 'VAC000904_NNL2') -- Mist
            OR (ncsc.PrimaryVendorName LIKE 'Nokia%') -- Nokia
            OR (ncsc.PrimaryVendorName LIKE 'Prolabs%') -- AddOn
            OR (di.PrimaryVendorName LIKE 'Zyxel%') -- Zyxel
            OR (ncsc.PrimaryVendorName  LIKE 'Smart Optics%') -- SmartOptics
        )
      AND ncsc.SAG_NAVSONUMBER = ifg.SALESORDERNUMBER
      AND ncsc.SAG_NAVLINENUM = ifg.SALESORDERLINENO
      AND ncsc.PACKINGSLIPID = ifg.IGSSHIPMENTNO)
  OR  (
        (UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' AND UPPER(ncsc.PrimaryVendorName) LIKE 'SONICWALL%')
        AND ncsc.SAG_NAVSONUMBER = ifg.SALESORDERNUMBER
        AND ncsc.SAG_NAVLINENUM = ifg.SALESORDERLINENO
        -- AND (ncsc.DATEPHYSICAL BETWEEN DATEADD(day, -31, ifg.CUSTOMERINVOICEDATE) AND DATEADD(day, +1, ifg.CUSTOMERINVOICEDATE))
      )
  OR  (
        (di.PrimaryVendorName LIKE 'WatchGuard%') -- WatchGuard
        AND ncsc.SAG_NAVSONUMBER = ifg.SALESORDERNUMBER
        AND ncsc.SAG_NAVLINENUM = ifg.SALESORDERLINENO
        AND ncsc.PACKINGSLIPID = ifg.IGSSHIPMENTNO
        -- AND (ncsc.DATEPHYSICAL BETWEEN DATEADD(day, -31, ifg.CUSTOMERINVOICEDATE) AND DATEADD(day, +1, ifg.CUSTOMERINVOICEDATE))
      )
LEFT JOIN exchangerates ex1
      ON ex1.fromcurrency = 'GBP'
      AND ex1.tocurrency = 'EUR'
      AND ex1.StartDate = ifg.CUSTOMERINVOICEDATE

WHERE 1 = 1
AND (
      -- Cambium
      (
        ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
      )
      OR
      -- Juniper
      (
        (ncsc.PrimaryVendorName LIKE 'Juniper%' AND ncsc.PrimaryVendorID NOT IN ('VAC000904_NGS1', 'VAC000904_NNL2', 'VAC001110_NGS1', 'VAC001110_NNL2'))
        AND ifg.VENDORRESELLERLEVEL LIKE 'Juniper%'
        AND ncsc.INVENTLOCATIONID <> 'DD'
        AND ifg.SHIPANDDEBIT = 1
      )
      OR
      -- Mist
      (
        (ncsc.PrimaryVendorID = 'VAC000904_NNL2')
        AND ((ncsc.INVENTSERIALID IS NOT NULL) OR (ncsc.INVENTSERIALID != 'NaN'))
      )
      OR
      -- Nokia
      (
        (ncsc.PrimaryVendorName LIKE 'Nokia%')
        AND ifg.VENDORRESELLERLEVEL LIKE 'Nokia%'
      )
      OR
      -- SonicWall
      (
        (UPPER(di.PrimaryVendorName) LIKE 'SONICWALL%' OR UPPER(ncsc.PrimaryVendorName) LIKE 'SONICWALL%')
        AND ((ifg.RESELLERID IS NOT NULL) OR (ifg.RESELLERID != 'NaN'))
      )
      OR
      -- WatchGuard
      (
        di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
        AND (
              (ncsc.row_number = 1 OR ncsc.row_number IS NULL)
              AND
              (
                ( 
                  (DATEADD(day, -1, TO_DATE(ifg.CUSTOMERINVOICEDATE)) >= TO_DATE(ncsc.DATEPHYSICAL))
                  OR (DATEADD(day, 0, TO_DATE(ifg.CUSTOMERINVOICEDATE)) = TO_DATE(ncsc.DATEPHYSICAL))
                  OR (DATEADD(day, +1, TO_DATE(ifg.CUSTOMERINVOICEDATE)) <= TO_DATE(ncsc.DATEPHYSICAL))
                )
                OR
                (ncsc.CUSTACCOUNT IS NULL)
              )
            )
      )
      OR
      -- AddOn
      (
        ncsc.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
      )
      OR
      -- Zyxel
      (
        di.PrimaryVendorName LIKE 'Zyxel%' -- Zyxel
        AND ((ifg.VATREGISTRATIONNO IS NOT NULL) OR (ifg.VATREGISTRATIONNO != 'NaN'))
      )
      OR
      -- SmartOptics
      (
        ncsc.PrimaryVendorName  LIKE 'Smart Optics%' -- SmartOptics
        AND ((ncsc.ItemName IS NOT NULL) OR (ncsc.ItemName != 'NaN'))
      )
    )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW v_pos_reports_csc OWNER TO `az_edw_data_engineers_ext_db`
