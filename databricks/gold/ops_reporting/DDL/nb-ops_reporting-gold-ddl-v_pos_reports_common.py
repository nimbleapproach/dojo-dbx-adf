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
# MAGIC CREATE OR REPLACE VIEW v_pos_reports_common AS
# MAGIC WITH sales_data as (
# MAGIC   SELECT DISTINCT
# MAGIC     sl.SALESID
# MAGIC   , sl.DATAAREAID
# MAGIC   , sl.inventtransid
# MAGIC   , sl.inventreftransid
# MAGIC   , sl.sag_resellervendorid
# MAGIC   , sl.sag_vendorreferencenumber
# MAGIC   , sl.sag_purchprice
# MAGIC   , sl.sag_vendorstandardcost
# MAGIC   , sl.salesstatus
# MAGIC   , sl.sag_shipanddebit
# MAGIC   , sl.currencycode
# MAGIC   , sl.salesprice
# MAGIC   , sl.itemid
# MAGIC   , sl.sag_ngs1pobuyprice
# MAGIC   , sl.linenum
# MAGIC   , st.purchorderformnum
# MAGIC   , st.sag_euaddress_name
# MAGIC   , st.sag_euaddress_street1
# MAGIC   , st.sag_euaddress_street2
# MAGIC   , st.sag_euaddress_city
# MAGIC   , st.sag_euaddress_county
# MAGIC   , st.sag_euaddress_postcode
# MAGIC   , st.sag_euaddress_country
# MAGIC   , st.sag_euaddress_contact
# MAGIC   , st.sag_euaddress_email
# MAGIC   , st.salesname
# MAGIC   , st.customerref
# MAGIC   , st.custaccount
# MAGIC   , st.salesordertype
# MAGIC   , st.INVOICEACCOUNT
# MAGIC   , st.DELIVERYPOSTALADDRESS
# MAGIC   , st.sag_reselleremailaddress
# MAGIC   , st.sag_createddatetime
# MAGIC     FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
# MAGIC     JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) st
# MAGIC       ON st.salesid    = sl.salesid								--Sales Line Local
# MAGIC      AND st.dataareaid = sl.dataareaid
# MAGIC )
# MAGIC , oracle_data as (
# MAGIC   SELECT
# MAGIC     op.sales_order
# MAGIC   , op.contact_name
# MAGIC   , co.firstName
# MAGIC   , co.lastName
# MAGIC   , co.emailaddress
# MAGIC   , co.contactisprimaryforaccount
# MAGIC   , co.creationdate
# MAGIC     FROM (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.ora_oracle_opportunities WHERE Sys_Silver_IsCurrent = 1) op
# MAGIC     LEFT JOIN (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.ora_oracle_contacts WHERE Sys_Silver_IsCurrent = 1) co
# MAGIC       ON CONCAT_WS(' ', STRING(co.firstName), STRING(co.lastName)) = STRING(op.contact_name)
# MAGIC )
# MAGIC , distinctitem_cte AS
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
# MAGIC     s.salesid AS salestableid_local,
# MAGIC     s.inventtransid AS saleslineid_local,
# MAGIC     pt.purchid AS purchtableid_local,
# MAGIC     pl.inventtransid AS purchlineid_local,
# MAGIC     '>> Inter Joins >>' AS hdr,
# MAGIC     upper(pt.intercompanycompanyid) as intercompanycompanyid,
# MAGIC     pl.intercompanyinventtransid
# MAGIC   FROM sales_data s
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pl
# MAGIC     ON s.inventreftransid = pl.inventtransid
# MAGIC     AND s.dataareaid = pl.dataareaid				--Purchase Line Local
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchtablestaging WHERE Sys_Silver_IsCurrent = 1) pt
# MAGIC     ON pl.purchid = pt.purchid					--Purchase Header Local
# MAGIC     AND pl.dataareaid = pt.dataareaid
# MAGIC ),
# MAGIC so_po_id_list AS
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
# MAGIC row_gen_ AS
# MAGIC (
# MAGIC     select explode(sequence(1,10000)) as row_id
# MAGIC ),
# MAGIC ngs_inventory as
# MAGIC (
# MAGIC   SELECT DISTINCT
# MAGIC     it.inventtransid
# MAGIC   , id.inventlocationid
# MAGIC   , id.dataareaid
# MAGIC     FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC     JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventdimstaging WHERE Sys_Silver_IsCurrent = 1) id
# MAGIC       ON it.inventdimid = id.inventdimid
# MAGIC      AND it.DATAAREAID in ('NGS1','NNL2')
# MAGIC 	 AND (it.STATUSISSUE IN ('1', '3') OR (it.STATUSRECEIPT LIKE '1' AND it.INVOICERETURNED = 1))
# MAGIC      AND id.inventlocationid NOT LIKE 'DD'
# MAGIC ),
# MAGIC support_items AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     it.inventserialid AS serialnumber,
# MAGIC     s.itemid AS itemid,
# MAGIC     sp.PurchTableID_InterComp AS ponumber,
# MAGIC     di.itemname AS sku
# MAGIC   FROM sales_data s
# MAGIC   JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC     ON it.inventtransid = s.inventtransid
# MAGIC    AND it.dataareaid <> 'NGS1'
# MAGIC    AND it.INVENTSERIALID != 'NaN'
# MAGIC   JOIN so_po_id_list sp
# MAGIC     ON sp.SalesLineID_Local = s.inventtransid
# MAGIC   JOIN v_distinctitems di
# MAGIC     ON di.itemid = s.itemid
# MAGIC     AND di.companyid = RIGHT(sp.SalesTableID_InterComp,4)
# MAGIC   WHERE
# MAGIC     s.dataareaid <> 'NGS1'
# MAGIC     AND it.STATUSISSUE = '1'
# MAGIC     AND di.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC000895_NNL2', 'VAC000850_NGS1') -- Versa
# MAGIC     AND di.ItemGroupID = 'Support'
# MAGIC     AND s.SALESSTATUS = '3'
# MAGIC     AND s.SALESORDERTYPE = 'New'
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
# MAGIC     ELSE it.datephysical
# MAGIC     END)                                                              AS report_date
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN NULL -- WatchGuard
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') THEN ct.invoicedate -- Cambium
# MAGIC     ELSE it.datephysical
# MAGIC     END)                                                              AS invoice_date
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                             AS financial_date
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC     THEN TO_DATE(it.datephysical)
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN it.datephysical
# MAGIC     ELSE NULL
# MAGIC     END)                                                             AS shipping_date
# MAGIC , s.dataareaid                                                       AS entity
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN s.itemid
# MAGIC     ELSE NULL
# MAGIC     END)                                                             AS part_code_id
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC             OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC             OR di.PrimaryVendorID IN('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC             OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC             OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC             THEN di.itemname
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC             THEN di_ic.itemname
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN (CASE WHEN di_ic.itemname = 'FORTICARE' THEN 'RENEWAL' ELSE di_ic.itemname END)
# MAGIC     ELSE NULL
# MAGIC     END)                                         AS part_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di.PrimaryVendorID IN('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN di.itemdescription
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN di_ic.itemdescription
# MAGIC     ELSE NULL
# MAGIC   END)                                                                AS part_code_description
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC     THEN di.itemgroupname
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC     THEN di_ic.itemgroupname
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS part_code_category
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             THEN s.sag_resellervendorid
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN ''   -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS partner_id
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SUM(-1* it.qty) OVER(
# MAGIC       PARTITION BY s.salesid, it.datephysical, di.itemname, s.inventtransid, s.dataareaid,
# MAGIC       s.purchorderformnum, s.currencycode, pa.addressdescription, CONCAT_WS(', ',SPLIT(pa.addressstreet,'\n')[0],SPLIT(pa.addressstreet,'\n')[1]),
# MAGIC       pa.addresscity, pa.addresszipcode, pa.addresscountryregionisocode,SPLIT(o.contact_name, ' +')[0], SPLIT(o.contact_name, ' +')[1],
# MAGIC       o.emailaddress, s.sag_euaddress_name, CONCAT_WS(', ', s.sag_euaddress_street1, s.sag_euaddress_street2),
# MAGIC       s.sag_euaddress_city, s.sag_euaddress_county, s.sag_euaddress_postcode, s.sag_euaddress_country, s.sag_euaddress_contact, s.sag_euaddress_email,
# MAGIC       o.contactisprimaryforaccount, o.creationdate) -- Sophos
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN ct.qty
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' THEN 1 -- Fortinet
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN ABS(-1 * it.qty)
# MAGIC     WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN SUM(-1* it.qty) OVER (PARTITION BY s.salesname, pa.ADDRESSCOUNTRYREGIONISOCODE, s.SAG_EUADDRESS_NAME, s.SAG_EUADDRESS_COUNTRY, s.CUSTOMERREF, di.ItemName
# MAGIC           /* s.SAG_CREATEDDATETIME should be added also when it will be added to view*/
# MAGIC           )
# MAGIC     ELSE (-1* it.qty)
# MAGIC     END)                                                              AS quantity
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC     THEN collect_set(it.INVENTSERIALID) over (partition by s.SALESID, it.ItemID, it.INVENTTRANSID, s.DATAAREAID) -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS serial_numbers --- TO DO
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.purchorderformnum -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS special_pricing_identifier
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.sag_vendorreferencenumber
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS vendor_promotion
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.sag_vendorstandardcost
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS mspunit_cost
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.sag_vendorstandardcost * (-1 * it.qty)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS mspunit_total_cost
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_number
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.salesname
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.organizationname -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC             THEN pa.ADDRESSDESCRIPTION
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.salesname
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_name  -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC             THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1]) --  Sophos
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1')
# MAGIC             THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1],SPLIT(pa.ADDRESSSTREET,'\n')[2]) --  Extreme
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN pa.ADDRESSSTREET
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             THEN CONCAT_WS(' ', pa.ADDRESSSTREET, pa.ADDRESSCITY)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN SPLIT(pa.ADDRESSSTREET,'\n')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address1
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN SPLIT(pa.ADDRESSSTREET,'\n')[1]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address2
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(pa.ADDRESSSTREET,'\n')[2]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address3
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN pa.ADDRESSCITY
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_city
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           THEN ''
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN 'NA'
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN pa.ADDRESSSTATE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_state
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.addresszipcode -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_postal_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN pa.ADDRESSZIPCODE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_postal_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.addresscountryregionisocode -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN pa.ADDRESSCOUNTRYREGIONISOCODE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(o.contact_name, ' +')[0] -- Sophos
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_first_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(o.contact_name, ' +')[1] -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_last_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_phone_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN o.emailaddress -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_email -- ora.Oracle_Contacts not ingested
# MAGIC --, 'To Be Done'                                                        AS reseller_email -- probably added by mistake
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN ad.DESCRIPTION
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'   -- WatchGuard
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN b.ISOCODE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN ad.ZIPCODE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_postal_code
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN SPLIT(ad.STREET,'\n')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_address1
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN SPLIT(ad.STREET,'\n')[1]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_address2
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(ad.STREET,'\n')[2]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_address3
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN ad.CITY
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_city
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN ad.COUNTY
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_state
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.sag_euaddress_name
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           THEN CONCAT_WS(',',s.sag_euaddress_street1, s.sag_euaddress_street2)
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN CONCAT_WS(' ',s.sag_euaddress_street1, s.sag_euaddress_street2)
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN (CASE
# MAGIC                 WHEN s.SAG_EUADDRESS_STREET1 = '' AND s.SAG_EUADDRESS_STREET2 = '' AND s.SAG_EUADDRESS_CITY = '' AND s.SAG_EUADDRESS_COUNTY = '' AND s.SAG_EUADDRESS_POSTCODE = ''
# MAGIC 			          THEN ''
# MAGIC 			          WHEN s.SAG_EUADDRESS_STREET2 = ''
# MAGIC 			          THEN s.SAG_EUADDRESS_STREET1 +  ', ' + s.SAG_EUADDRESS_CITY + ', ' + s.SAG_EUADDRESS_COUNTY + ', ' + s.SAG_EUADDRESS_POSTCODE
# MAGIC 			          ELSE s.SAG_EUADDRESS_STREET1 + ', ' + s.SAG_EUADDRESS_STREET2 + ', ' + s.SAG_EUADDRESS_CITY + ', ' + s.SAG_EUADDRESS_COUNTY + ', ' + s.SAG_EUADDRESS_POSTCODE
# MAGIC 	              END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC             THEN s.sag_euaddress_street1
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             THEN s.sag_euaddress_street1
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address1
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.sag_euaddress_street2
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address2
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.sag_euaddress_city
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_city
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.sag_euaddress_county
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN 'NA'
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_state
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.sag_euaddress_postcode
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_postal_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN s.sag_euaddress_country
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.sag_euaddress_contact
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC             THEN SPLIT(s.sag_euaddress_contact,' ')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact_first_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC             THEN SPLIT(s.sag_euaddress_contact,' ')[1]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact_last_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.sag_euaddress_email -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_email
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_phone_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN it.inventserialid
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN UPPER(it.inventserialid)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS serial_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia'
# MAGIC           OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.salesid
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS d365_sales_order_number
# MAGIC , 'Infinigate Global Services Ltd'                                    AS distributor_name
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN it.invoiceid
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN ct.invoiceid
# MAGIC     ELSE NULL
# MAGIC     END)                                                        AS invoice_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.customerref
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_po_to_infinigate
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC             OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC             OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC             THEN sp.purchtableid_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS infinigate_po_to_vendor
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN s.currencycode
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.currencycode
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN ct.currencycode
# MAGIC     ELSE NULL
# MAGIC     END)                                                     AS sell_currency
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.sag_purchprice
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
# MAGIC             THEN sp.purchprice_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS final_vendor_unit_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           THEN s.sag_purchprice
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS final_vendor_unit_buy_price2
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN s.sag_purchprice * (-1 * it.qty)  -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS final_vendor_total_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                       WHEN sp.purchlineamount_intercomp = 0 AND sp.purchqty_intercomp = 0 THEN s.sag_purchprice
# MAGIC                                                       ELSE sp.purchlineamount_intercomp / sp.purchqty_intercomp
# MAGIC                                                     END) -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS po_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                       WHEN sp.purchlineamount_intercomp = 0 AND sp.purchqty_intercomp = 0 THEN (-1 * it.qty) * s.sag_purchprice
# MAGIC                                                       ELSE (-1 * it.qty) * (sp.purchlineamount_intercomp / sp.purchqty_intercomp)
# MAGIC                                                     END) -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS po_total_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN sp.purchprice_intercomp / sp.purchqty_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS purchase_price_usd
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN sp.purchprice_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS purchase_extended_price
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (sp.purchprice_intercomp - s.sag_purchprice) * (-1 * it.qty)  -- AddOn
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC            THEN (CASE
# MAGIC                     WHEN di_ic.PrimaryVendorID LIKE 'VAC001014%' THEN (s.sag_vendorstandardcost - s.sag_purchprice) * ct.qty
# MAGIC                     ELSE 0
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS claim_amount
# MAGIC , di.primaryvendorid                                                  AS vendor_id
# MAGIC , di.primaryvendorname                                                AS vendor_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.salesstatus -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_status
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.sag_shipanddebit -- Sophos
# MAGIC     WHEN ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN (CASE
# MAGIC                 WHEN s.sag_shipanddebit = 0 THEN 'No'
# MAGIC                 WHEN s.sag_shipanddebit = 1 THEN 'Yes'
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_and_debit
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN di.itemgroupid -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS part_code_group_id
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.statusissue  -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS status_issue
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.statusreceipt -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS status_receipt
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.invoicereturned -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS invoice_returned
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.packingslipreturned -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS packing_slip_returned
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN '' -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS price_per_unit_for_this_deal
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN '' -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS extended_price_for_this_deal
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN NULL -- Sophos
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') THEN NULL -- Nokia
# MAGIC     ELSE it.recid
# MAGIC     END)                                                              AS transaction_record_id
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%'
# MAGIC           OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.custaccount  -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS customer_account
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN sp.purchlineamount_intercomp  -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS line_amount
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN sp.purchqty_intercomp  -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS purch_quantity
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                     WHEN sp.purchlineamount_intercomp=0 AND sp.purchqty_intercomp=0 THEN 'PO QTY Zero'
# MAGIC                                                     ELSE ''
# MAGIC                                                     END) -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS check
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                     WHEN s.currencycode = 'USD' THEN s.salesprice
# MAGIC                                                     WHEN s.currencycode = 'GBP' THEN s.salesprice * ex2.rate
# MAGIC                                                     ELSE s.salesprice / ex.rate
# MAGIC                                                     END)  -- AddOn
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN (CASE
# MAGIC                     WHEN ct.currencycode = 'USD' THEN ct.salesprice
# MAGIC                     WHEN ct.currencycode = 'GBP' THEN ct.salesprice * ex2.rate
# MAGIC                     ELSE ct.salesprice / ex.rate
# MAGIC                  END)
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC             THEN (CASE
# MAGIC                     WHEN s.currencycode = 'USD' THEN (s.salesprice * (-1 * it.qty))
# MAGIC                     WHEN s.currencycode = 'GBP' THEN (s.salesprice * (-1 * it.qty)) * ex2.rate
# MAGIC                     ELSE (s.salesprice * (-1 * it.qty)) / ex.rate
# MAGIC                  END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_price_usd
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC           THEN s.sag_purchprice * (-1 * it.qty)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_price_total_usd
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN ct.salesprice
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             THEN s.salesprice
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC             THEN s.salesprice * (-1 * it.qty)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_price
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC             OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN sp.purchtableid_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS distributer_purchase_order
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS country_of_purchase
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS custom1
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC             OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS custom2
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
# MAGIC             THEN (CASE WHEN ct.currencycode = 'USD' THEN 1 ELSE IFNULL(ex.rate, ex2.rate) END)
# MAGIC     END)                                                              AS exchange_rate
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN IFNULL(sp.purchprice_intercomp, s.sag_purchprice)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS usd_disti_unit_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN (CASE WHEN sp.purchtableid_intercomp IS NULL THEN 'Fulfilled from existing distributor stock' ELSE 'Back to Back' END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS order_type
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN s.salesordertype
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_order_type
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN rwg.row_id
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS fortinet_row_id
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           THEN (CASE
# MAGIC                 WHEN ng.INVENTLOCATIONID LIKE '%2' AND ng.DATAAREAID = 'NNL2' --add post Rome Go Live (MW)
# MAGIC                 THEN '101421538' -- Venlo ID
# MAGIC                 WHEN ng.INVENTLOCATIONID LIKE'%5' AND ng.DATAAREAID = 'NGS1'
# MAGIC                 THEN '101417609' -- UK ID
# MAGIC                 WHEN ng.INVENTLOCATIONID LIKE'CORR%'AND ng.DATAAREAID = 'NGS1'
# MAGIC                 THEN '101417609' --Added to pick up random name convention for Corrective Warehouse in NGS1
# MAGIC                 WHEN ng.INVENTLOCATIONID LIKE '%2' AND ng.DATAAREAID = 'NGS1'
# MAGIC                 THEN '101456761' --NGS1 Venlo ID
# MAGIC                 END)
# MAGIC     WHEN ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN (CASE
# MAGIC                 WHEN ng.inventlocationid = 'MAIN2' AND ng.dataareaid = 'NNL2' --add post Rome Go Live (MW)
# MAGIC 		        THEN '101421538' -- Venlo ID
# MAGIC 		        WHEN ng.inventlocationid = 'MAIN5' AND ng.dataareaid = 'NGS1'
# MAGIC 		        THEN '101417609' -- UK ID
# MAGIC 		        WHEN ng.inventlocationid = 'MAIN2' AND ng.dataareaid = 'NGS1'
# MAGIC 		        THEN '101456761' --NGS1 Venlo ID
# MAGIC 		        ELSE ''
# MAGIC 	            END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS distributer_id_number
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC            THEN (CASE WHEN -1*it.QTY > 0 THEN 'POS'
# MAGIC                 WHEN -1*it.QTY <0 THEN 'RD'
# MAGIC                 ELSE ''
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS distributer_transaction_type
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS export_licence_number
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS business_model1
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS business_model2
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS juniper_var_id2
# MAGIC , (CASE
# MAGIC     WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC            THEN ng.inventlocationid
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS warehouse
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN sp.salestableid_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS intercompany_sales_order
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           THEN su.sku
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sla
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           THEN su.sku
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS rma_support
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN SPLIT(di_ic.PrimaryVendorName,' ')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS oem_name
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN (CASE
# MAGIC                 WHEN di_ic.PrimaryVendorName	LIKE 'Advantech%'
# MAGIC 		            THEN di_ic.ItemDescription
# MAGIC 			          ELSE di_ic.ItemName
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS oem_part_number
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           THEN CONCAT(sp.PurchTableID_InterComp, '/', su.ponumber)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS po_number_hardware_support
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN (CASE
# MAGIC                 WHEN RIGHT(sp.SalesTableID_InterComp, 4) = 'NGS1' THEN 'United Kingdom'
# MAGIC 		            WHEN RIGHT(sp.SalesTableID_InterComp, 4) = 'NNL2' THEN 'Netherlands'
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS shipping_legal_entity
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN di.itemname
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS snwl_pn_sku
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN it.inventtransid
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS invent_trans_id
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             THEN 'Zycko Ltd'
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             THEN 'Nuvias Global Services Ltd'
# MAGIC     WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC             THEN 'Infinigate UK Ltd'
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS distributor_account_name
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             THEN s.SAG_CREATEDDATETIME
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS sales_order_date
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN DATE_FORMAT(s.SAG_CREATEDDATETIME, 'dd-MM-yyyy')
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS order_date
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN s.SAG_NGS1POBUYPRICE * ex3.RATE
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS product_cost_eur
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN (s.SAG_NGS1POBUYPRICE * ex3.RATE) * (-1 * it.qty)
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS product_cost_eur_total
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS blank3
# MAGIC , (CASE
# MAGIC     WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS blank4
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           THEN 'www.nuvias.com'
# MAGIC     WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN 'infinigate.co.uk'
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS distributor_domain
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN 'GB'
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS distributor_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2') -- Versa POS 1.0
# MAGIC           OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC           THEN s.SAG_RESELLEREMAILADDRESS
# MAGIC     WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS reseller_email
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS end_customer_domain
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
# MAGIC           THEN s.LINENUM
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS line_item
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS ship_to_state_jabil
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC   END)                                                              AS end_customer_state_jabil
# MAGIC   FROM sales_data s
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC     ON it.inventtransid = s.inventtransid
# MAGIC    AND it.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN so_po_id_list sp
# MAGIC     ON sp.saleslineid_local = s.inventtransid
# MAGIC   LEFT JOIN v_distinctitems di
# MAGIC     ON di.itemid = s.itemid
# MAGIC    AND di.companyid = (CASE WHEN s.dataareaid = 'NUK1' THEN 'NGS1' ELSE 'NNL2' END)
# MAGIC   LEFT JOIN v_distinctitems di_ic
# MAGIC     ON di_ic.itemid = s.itemid
# MAGIC    AND di_ic.companyid = substr(sp.SalesTableID_InterComp, -4)
# MAGIC   LEFT JOIN ngs_inventory ng
# MAGIC     ON ng.inventtransid = sp.SalesLineID_InterComp
# MAGIC    AND (
# MAGIC         (di_ic.PrimaryVendorName LIKE 'Juniper%'
# MAGIC           AND di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))  -- Juniper
# MAGIC         OR
# MAGIC         (di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_custcustomerv3staging WHERE Sys_Silver_IsCurrent = 1) cu
# MAGIC     ON cu.customeraccount = s.custaccount
# MAGIC    AND cu.dataareaid = s.dataareaid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_customerpostaladdressstaging WHERE Sys_Silver_IsCurrent = 1 AND ISPRIMARY = 1) pa
# MAGIC     ON pa.CUSTOMERACCOUNTNUMBER = s.INVOICEACCOUNT
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging WHERE Sys_Silver_IsCurrent = 1) ad
# MAGIC     ON ad.ADDRESSRECID = s.DELIVERYPOSTALADDRESS
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_logisticsaddresscountryregionstaging WHERE Sys_Silver_IsCurrent = 1) b
# MAGIC     ON b.COUNTRYREGION = ad.COUNTRYREGIONID
# MAGIC   LEFT JOIN oracle_data o
# MAGIC     ON o.sales_order = s.salesid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_custinvoicetransstaging WHERE Sys_Silver_IsCurrent = 1) ct
# MAGIC     ON ct.inventtransid = s.inventtransid
# MAGIC    AND ct.dataareaid NOT IN ('NGS1' ,'NNL2')
# MAGIC    AND di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
# MAGIC   LEFT JOIN exchangerates ex
# MAGIC     ON ex.startdate = TO_DATE(it.datephysical)
# MAGIC    AND ex.tocurrency = s.currencycode
# MAGIC    AND ex.fromcurrency = 'USD'
# MAGIC   LEFT JOIN exchangerates ex2
# MAGIC     ON ex2.startdate = TO_DATE(it.datephysical)
# MAGIC    AND ex2.fromcurrency = s.currencycode
# MAGIC    AND ex2.fromcurrency = 'GBP'
# MAGIC    AND ex2.tocurrency = 'USD'
# MAGIC   LEFT JOIN exchangerates ex3
# MAGIC     ON ex3.startdate = TO_DATE(it.datephysical)
# MAGIC    AND ex3.fromcurrency = 'GBP'
# MAGIC    AND ex3.tocurrency = 'EUR'
# MAGIC   LEFT JOIN support_items su
# MAGIC     ON su.serialnumber = it.inventserialid
# MAGIC   LEFT JOIN row_gen_ rwg
# MAGIC     ON (-1 * it.qty) >= rwg.row_id
# MAGIC    AND di_ic.PrimaryVendorName LIKE 'Fortinet%'
# MAGIC  WHERE 1 = 1
# MAGIC   AND (
# MAGIC         -- Sophos
# MAGIC         (
# MAGIC                 s.SALESSTATUS IN ('1', '2', '3') -- MW 20/06/2023 added status 1 to capture part shipments
# MAGIC             AND s.SAG_SHIPANDDEBIT = '1'
# MAGIC             AND di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC             AND di.ItemGroupID = 'Hardware'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- WatchGuard
# MAGIC         (
# MAGIC             (  it.STATUSISSUE IN ('1', '3') OR (it.STATUSRECEIPT LIKE '1' AND it.INVOICERETURNED = 1))
# MAGIC             AND di.PrimaryVendorName LIKE 'WatchGuard%'
# MAGIC             AND it.PACKINGSLIPRETURNED <> 1
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- AddON
# MAGIC         (
# MAGIC             di_ic.PrimaryVendorName LIKE 'Prolabs%'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Extreme
# MAGIC         (
# MAGIC           di.PrimaryVendorID IN ('VAC001044_NGS1')
# MAGIC           AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Cambium
# MAGIC         (
# MAGIC           di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
# MAGIC           AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Fortinet
# MAGIC         (
# MAGIC             di_ic.PrimaryVendorName LIKE 'Fortinet%'
# MAGIC             AND (-1 * it.qty) > 0
# MAGIC             AND s.salesordertype NOT LIKE 'Demo'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Juniper
# MAGIC         (
# MAGIC             di_ic.PrimaryVendorName LIKE 'Juniper%'
# MAGIC             AND di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2')
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Mist
# MAGIC         (
# MAGIC             ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%'))
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Nokia
# MAGIC         (
# MAGIC             di_ic.PrimaryVendorName LIKE 'Nokia%'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Yubico POS
# MAGIC         (
# MAGIC             di.PrimaryVendorID IN('VAC001388_NGS1')
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Yubico POS Report
# MAGIC         (
# MAGIC             di.PrimaryVendorID = 'VAC001358_NGS1'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- SmartOptics
# MAGIC         (
# MAGIC             di.PrimaryVendorName LIKE 'Smart Optics%'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Versa POS 1.0
# MAGIC         (
# MAGIC             di_ic.PrimaryVendorID IN ('VAC000895_NGS1', 'VAC000850_NGS1', 'VAC001068_NGS1', 'VAC000895_NNL2', 'VAC000850_NNL2', 'VAC001068_NNL2')
# MAGIC             AND it.STATUSISSUE = '1'
# MAGIC             AND s.DATAAREAID <> 'NGS1'
# MAGIC             AND di.ItemGroupID IN ('Hardware', 'Accessory')
# MAGIC             AND s.SALESSTATUS = '3'
# MAGIC             AND s.SALESORDERTYPE LIKE 'New'
# MAGIC             AND s.DATAAREAID <> 'NGS1'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Versa POS v1.2 External
# MAGIC         (
# MAGIC             di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
# MAGIC             AND it.STATUSISSUE = '1'
# MAGIC             AND s.SALESSTATUS = '3'
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- SonicWall
# MAGIC         (
# MAGIC             di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2')
# MAGIC             AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Jabil v1.2 External
# MAGIC         (
# MAGIC           di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
# MAGIC           AND it.STATUSISSUE = '1'
# MAGIC           AND s.SALESSTATUS = '3'
# MAGIC           AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW gold_dev.ops_reporting.v_pos_reports_common OWNER TO `az_edw_data_engineers_ext_db`
