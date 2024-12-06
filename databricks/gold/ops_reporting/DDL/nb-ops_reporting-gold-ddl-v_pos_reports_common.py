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
# MAGIC serial_numbers_cte AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     sl.SALESID,
# MAGIC     it.ItemID,
# MAGIC     it.INVENTTRANSID,
# MAGIC     sl.DATAAREAID,
# MAGIC     ARRAY_AGG(it.INVENTSERIALID) AS serial_numbers
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC     ON it.inventtransid = sl.inventtransid
# MAGIC     AND it.dataareaid = sl.dataareaid
# MAGIC   GROUP BY
# MAGIC     sl.SALESID,
# MAGIC     it.ItemID,
# MAGIC     it.INVENTTRANSID,
# MAGIC     sl.DATAAREAID
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
# MAGIC ),
# MAGIC row_gen_ AS
# MAGIC (
# MAGIC     select explode(sequence(1,10000)) as row_id
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
# MAGIC     ELSE it.datephysical
# MAGIC     END)                                                              AS report_date
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN NULL -- WatchGuard
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') THEN ct.invoicedate -- Cambium
# MAGIC     ELSE it.datephysical
# MAGIC     END)                                                              AS invoice_date
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                             AS financial_date
# MAGIC , s.dataareaid                                                       AS entity
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC             OR di.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC             OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC             OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC             THEN di.itemname
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN (CASE WHEN di.itemname = 'FORTICARE' THEN 'RENEWAL' ELSE di.itemname END)
# MAGIC     ELSE NULL
# MAGIC     END)                                         AS part_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN di.itemdescription
# MAGIC     ELSE NULL
# MAGIC   END)                                                                AS part_code_description
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           THEN di.itemgroupname
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS part_code_category
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC             OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
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
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN ct.qty
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' THEN 1 -- Fortinet
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN ABS(-1 * it.qty)
# MAGIC     WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN SUM(-1* it.qty) OVER (PARTITION BY s.salesname, pa.ADDRESSCOUNTRYREGIONISOCODE, s.SAG_EUADDRESS_NAME, s.SAG_EUADDRESS_COUNTRY, s.CUSTOMERREF, di.ItemName 
# MAGIC           /* s.SAG_CREATEDDATETIME should be added also when it will be added to view*/
# MAGIC           )
# MAGIC     ELSE (-1* it.qty)
# MAGIC     END)                                                              AS quantity
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN snc.serial_numbers -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS serial_numbers --- TO DO
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.purchorderformnum -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS special_pricing_identifier
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN s.sag_vendorreferencenumber
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS vendor_promotion
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN s.sag_vendorstandardcost -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS mspunit_cost
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN s.sag_vendorstandardcost * (-1 * it.qty)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS mspunit_total_cost
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN s.salesname
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.organizationname -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             THEN pa.ADDRESSDESCRIPTION
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_name  -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC             THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1]) --  Sophos
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1')
# MAGIC             THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1],SPLIT(pa.ADDRESSSTREET,'\n')[2]) --  Extreme
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN pa.ADDRESSSTREET
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             THEN CONCAT_WS(' ', pa.ADDRESSSTREET, pa.ADDRESSCITY)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(pa.ADDRESSSTREET,'\n')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address1
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(pa.ADDRESSSTREET,'\n')[1]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address2
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(pa.ADDRESSSTREET,'\n')[2]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address3
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN pa.ADDRESSCITY
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_city
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           THEN ''
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN 'NA'
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
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
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
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
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
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
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN ad.DESCRIPTION
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'   -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN b.ISOCODE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN ad.ZIPCODE
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_postal_code
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(ad.STREET,'\n')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_address1
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(ad.STREET,'\n')[1]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_address2
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN SPLIT(ad.STREET,'\n')[2]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_address3
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN ad.CITY
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_city
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
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
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN s.sag_euaddress_name
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           THEN CONCAT_WS(',',s.sag_euaddress_street1, s.sag_euaddress_street2)
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN CONCAT_WS(' ',s.sag_euaddress_street1, s.sag_euaddress_street2)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             THEN s.sag_euaddress_street1
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             THEN s.sag_euaddress_street1
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address1
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.sag_euaddress_street2
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address2
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.sag_euaddress_city
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_city
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.sag_euaddress_county
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN 'NA'
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_state
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN s.sag_euaddress_postcode
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_postal_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN s.sag_euaddress_country
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN s.sag_euaddress_contact -- Extreme
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN SPLIT(s.sag_euaddress_contact,' ')[0]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact_first_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC             OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN SPLIT(s.sag_euaddress_contact,' ')[1]
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact_last_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           THEN s.sag_euaddress_email -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_email
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_phone_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN it.inventserialid
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN UPPER(it.inventserialid)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS serial_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia'
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN s.salesid
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS d365_sales_order_number
# MAGIC , 'Infinigate Global Services Ltd'                                    AS distributor_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN it.invoiceid
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           THEN ct.invoiceid
# MAGIC     ELSE NULL
# MAGIC     END)                                                        AS invoice_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN s.customerref
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_po_to_infinigate
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
# MAGIC             OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC             OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC             OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             THEN sp.purchtableid_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS infinigate_po_to_vendor
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
# MAGIC           OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC           OR di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC           OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
# MAGIC           THEN s.currencycode
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.currencycode
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN ct.currencycode
# MAGIC     ELSE NULL
# MAGIC     END)                                                     AS sell_currency
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
# MAGIC           OR di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
# MAGIC           THEN s.sag_purchprice
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
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
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                       WHEN sp.purchlineamount_intercomp = 0 AND sp.purchqty_intercomp = 0 THEN s.sag_purchprice
# MAGIC                                                       ELSE sp.purchlineamount_intercomp / sp.purchqty_intercomp
# MAGIC                                                     END) -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS po_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                       WHEN sp.purchlineamount_intercomp = 0 AND sp.purchqty_intercomp = 0 THEN (-1 * it.qty) * s.sag_purchprice
# MAGIC                                                       ELSE (-1 * it.qty) * (sp.purchlineamount_intercomp / sp.purchqty_intercomp)
# MAGIC                                                     END) -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS po_total_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN (sp.purchprice_intercomp - s.sag_purchprice) * (-1 * it.qty)  -- AddOn
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC            THEN (CASE
# MAGIC                     WHEN di.PrimaryVendorID LIKE 'VAC001014%' THEN (s.sag_vendorstandardcost - s.sag_purchprice) * ct.qty
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
# MAGIC     WHEN di.PrimaryVendorID NOT IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN it.recid -- any vendor except for Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS transaction_record_id
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN s.custaccount  -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS customer_account
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN sp.purchlineamount_intercomp  -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS line_amount
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN sp.purchqty_intercomp  -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS purch_quantity
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                     WHEN sp.purchlineamount_intercomp=0 AND sp.purchqty_intercomp=0 THEN 'PO QTY Zero'
# MAGIC                                                     ELSE ''
# MAGIC                                                     END) -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS check
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
# MAGIC                                                     WHEN s.currencycode = 'USD' THEN s.salesprice
# MAGIC                                                     WHEN s.currencycode = 'GBP' THEN s.salesprice * ex2.rate
# MAGIC                                                     ELSE s.salesprice / ex.rate
# MAGIC                                                     END)  -- AddOn
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN (CASE
# MAGIC                     WHEN ct.currencycode = 'USD' THEN ct.salesprice
# MAGIC                     WHEN ct.currencycode = 'GBP' THEN ct.salesprice * ex2.rate
# MAGIC                     ELSE ct.salesprice / ex.rate
# MAGIC                  END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_price_usd
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN ct.salesprice
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC           OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC           OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
# MAGIC             THEN s.salesprice
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC             OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
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
# MAGIC             OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS custom1
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
# MAGIC             OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS custom2
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC             THEN (CASE WHEN ct.currencycode = 'USD' THEN 1 ELSE IFNULL(ex.rate, ex2.rate) END)
# MAGIC     END)                                                              AS exchange_rate
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN IFNULL(sp.purchprice_intercomp, s.sag_purchprice)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS usd_disti_unit_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN (CASE WHEN sp.purchtableid_intercomp IS NULL THEN 'Fulfilled from existing distributor stock' ELSE 'Back to Back' END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS order_type
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN s.salesordertype
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_order_type
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
# MAGIC             THEN rwg.row_id
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS fortinet_row_id
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
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
# MAGIC     WHEN ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
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
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC            OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC            THEN (CASE WHEN -1*it.QTY > 0 THEN 'POS'
# MAGIC                 WHEN -1*it.QTY <0 THEN 'RD'
# MAGIC                 ELSE ''
# MAGIC                 END)
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS distributer_transaction_type
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS export_licence_number
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS business_model1
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS business_model2
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC             OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC            THEN ''
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS juniper_var_id2
# MAGIC , (CASE
# MAGIC     WHEN ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
# MAGIC            OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist 
# MAGIC            THEN ng.inventlocationid
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS warehouse
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
# MAGIC           OR di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
# MAGIC           OR di.PrimaryVendorName LIKE 'Nokia%' -- Nokia
# MAGIC           THEN sp.salestableid_intercomp
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS intercompany_sales_order    
# MAGIC   FROM sales_data s
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC     ON it.inventtransid = s.inventtransid
# MAGIC    AND it.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN so_po_id_list sp
# MAGIC     ON sp.saleslineid_local = s.inventtransid
# MAGIC   LEFT JOIN v_distinctitems di
# MAGIC     ON di.itemid = s.itemid
# MAGIC    AND di.companyid = (CASE WHEN s.dataareaid = 'NUK1' THEN 'NGS1' ELSE 'NNL2' END)
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventdimstaging WHERE Sys_Silver_IsCurrent = 1) ng
# MAGIC     ON ng.inventdimid = it.inventdimid
# MAGIC    AND it.dataareaid IN ( 'NGS1','NNL2')
# MAGIC    AND (statusissue IN ('1', '3') OR (statusreceipt LIKE '1' AND invoicereturned = 1))
# MAGIC    AND ng.inventlocationid NOT LIKE 'DD'
# MAGIC    AND (di.PrimaryVendorName LIKE 'Juniper%')
# MAGIC    AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_custcustomerv3staging WHERE Sys_Silver_IsCurrent = 1) cu
# MAGIC     ON cu.customeraccount = s.custaccount
# MAGIC    AND cu.dataareaid = s.dataareaid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_customerpostaladdressstaging WHERE Sys_Silver_IsCurrent = 1) pa
# MAGIC     ON pa.CUSTOMERACCOUNTNUMBER = s.INVOICEACCOUNT
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging WHERE Sys_Silver_IsCurrent = 1) ad
# MAGIC     ON ad.ADDRESSRECID = s.DELIVERYPOSTALADDRESS
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_logisticsaddresscountryregionstaging WHERE Sys_Silver_IsCurrent = 1) b
# MAGIC     ON b.COUNTRYREGION = ad.COUNTRYREGIONID
# MAGIC   LEFT JOIN oracle_data o
# MAGIC     ON o.sales_order = s.salesid
# MAGIC   LEFT JOIN serial_numbers_cte snc
# MAGIC     ON s.SALESID = snc.SALESID
# MAGIC    AND it.ItemID = snc.ItemID
# MAGIC    AND it.INVENTTRANSID = snc.INVENTTRANSID
# MAGIC    AND s.DATAAREAID = snc.DATAAREAID
# MAGIC    AND di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
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
# MAGIC   LEFT JOIN row_gen_ rwg
# MAGIC     ON (-1 * it.qty) >= rwg.row_id
# MAGIC    AND di.PrimaryVendorName LIKE 'Fortinet%'
# MAGIC  WHERE 1 = 1
# MAGIC   AND s.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC   AND pa.ISPRIMARY = 1
# MAGIC   AND (
# MAGIC         -- Sophos
# MAGIC         (
# MAGIC                 s.SALESSTATUS IN ('1', '2', '3') -- MW 20/06/2023 added status 1 to capture part shipments
# MAGIC             AND s.SAG_SHIPANDDEBIT = '1'
# MAGIC             AND di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
# MAGIC             AND di.ItemGroupID = 'Hardware'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- WatchGuard
# MAGIC         (
# MAGIC             (  it.STATUSISSUE IN ('1', '3') OR (it.STATUSRECEIPT LIKE '1' AND it.INVOICERETURNED = 1))
# MAGIC             AND di.PrimaryVendorName LIKE 'WatchGuard%'
# MAGIC             AND it.PACKINGSLIPRETURNED <> 1
# MAGIC         )
# MAGIC         OR
# MAGIC         -- AddON
# MAGIC         (
# MAGIC             di.PrimaryVendorName LIKE 'Prolabs%'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Extreme
# MAGIC         (
# MAGIC           di.PrimaryVendorID IN ('VAC001044_NGS1')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Cambium
# MAGIC         (
# MAGIC           di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Fortinet
# MAGIC         (
# MAGIC             di.PrimaryVendorName LIKE 'Fortinet%'
# MAGIC             AND (-1 * it.qty) > 1
# MAGIC             AND s.salesordertype NOT LIKE 'Demo'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Juniper
# MAGIC         (
# MAGIC             di.PrimaryVendorName LIKE 'Juniper%'
# MAGIC             AND di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2')
# MAGIC             AND ng.inventlocationid NOT LIKE 'DD'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Mist
# MAGIC         (
# MAGIC             ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%'))
# MAGIC             AND ng.inventlocationid NOT LIKE 'DD'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Nokia
# MAGIC         (
# MAGIC             di.PrimaryVendorName LIKE 'Nokia%'
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Yubico POS
# MAGIC         (
# MAGIC             di.PrimaryVendorID IN('VAC001388_NGS1')
# MAGIC         )
# MAGIC         OR
# MAGIC         -- Yubico POS Report
# MAGIC         (
# MAGIC             di.PrimaryVendorID = 'VAC001358_NGS1'
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW gold_dev.ops_reporting.v_pos_reports_common OWNER TO `az_edw_data_engineers_ext_db`
