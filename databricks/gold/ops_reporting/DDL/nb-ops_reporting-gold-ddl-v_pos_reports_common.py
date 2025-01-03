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
CREATE OR REPLACE VIEW v_pos_reports_common AS
WITH sales_data as (
  SELECT DISTINCT
    sl.SALESID
  , sl.DATAAREAID
  , sl.inventtransid
  , sl.inventreftransid
  , sl.sag_resellervendorid
  , sl.sag_vendorreferencenumber
  , sl.sag_purchprice
  , sl.sag_vendorstandardcost
  , sl.salesstatus
  , sl.sag_shipanddebit
  , sl.currencycode
  , sl.salesprice
  , sl.itemid
  , sl.sag_ngs1pobuyprice
  , sl.linenum
  , st.purchorderformnum
  , st.sag_euaddress_name
  , st.sag_euaddress_street1
  , st.sag_euaddress_street2
  , st.sag_euaddress_city
  , st.sag_euaddress_county
  , st.sag_euaddress_postcode
  , st.sag_euaddress_country
  , st.sag_euaddress_contact
  , st.sag_euaddress_email
  , st.salesname
  , st.customerref
  , st.custaccount
  , st.salesordertype
  , st.INVOICEACCOUNT
  , st.DELIVERYPOSTALADDRESS
  , st.sag_reselleremailaddress
  , st.sag_createddatetime
    FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
    JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) st
      ON st.salesid    = sl.salesid								--Sales Line Local
     AND st.dataareaid = sl.dataareaid
)
, oracle_data as (
  SELECT
    op.sales_order
  , op.contact_name
  , co.firstName
  , co.lastName
  , co.emailaddress
  , co.contactisprimaryforaccount
  , co.creationdate
    FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prodtrans_sqlbyod.ora_oracle_opportunities WHERE Sys_Silver_IsCurrent = 1) op
    LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prodtrans_sqlbyod.ora_oracle_contacts WHERE Sys_Silver_IsCurrent = 1) co
      ON CONCAT_WS(' ', STRING(co.firstName), STRING(co.lastName)) = STRING(op.contact_name)
)
, distinctitem_cte AS
(
SELECT
	ROW_NUMBER() OVER(PARTITION BY it.itemid, it.dataareaid ORDER BY it.itemid) rn
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
    s.salesid AS salestableid_local,
    s.inventtransid AS saleslineid_local,
    pt.purchid AS purchtableid_local,
    pl.inventtransid AS purchlineid_local,
    '>> Inter Joins >>' AS hdr,
    upper(pt.intercompanycompanyid) as intercompanycompanyid,
    pl.intercompanyinventtransid
  FROM sales_data s
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pl
    ON s.inventreftransid = pl.inventtransid
    AND s.dataareaid = pl.dataareaid				--Purchase Line Local
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_purchtablestaging WHERE Sys_Silver_IsCurrent = 1) pt
    ON pl.purchid = pt.purchid					--Purchase Header Local
    AND pl.dataareaid = pt.dataareaid
),
so_po_id_list AS
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
row_gen_ AS
(
    select explode(sequence(1,10000)) as row_id
),
ngs_inventory as
(
  SELECT DISTINCT
    it.inventtransid
  , id.inventlocationid
  , id.dataareaid
    FROM (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
    JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventdimstaging WHERE Sys_Silver_IsCurrent = 1) id
      ON it.inventdimid = id.inventdimid
     AND it.DATAAREAID in ('NGS1','NNL2')
	 AND (it.STATUSISSUE IN ('1', '3') OR (it.STATUSRECEIPT LIKE '1' AND it.INVOICERETURNED = 1))
     AND id.inventlocationid NOT LIKE 'DD'
)
SELECT DISTINCT
  (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') THEN ct.invoicedate -- Cambium
    ELSE it.datephysical
    END)                                                              AS report_date
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN NULL -- WatchGuard
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') THEN ct.invoicedate -- Cambium
    ELSE it.datephysical
    END)                                                              AS invoice_date
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN it.datefinancial -- Sophos
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
    ELSE NULL
    END)                                                             AS financial_date
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
    THEN TO_DATE(it.datephysical)
    WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN it.datephysical
    ELSE NULL
    END)                                                             AS shipping_date
, s.dataareaid                                                       AS entity
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN s.itemid
    ELSE NULL
    END)                                                             AS part_code_id
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
            OR di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
            OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
            OR di.PrimaryVendorID IN('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
            OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
            OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
            OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
            THEN di.itemname
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
            OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
            OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
            OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
            THEN di_ic.itemname
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            THEN (CASE WHEN di_ic.itemname = 'FORTICARE' THEN 'RENEWAL' ELSE di_ic.itemname END)
    ELSE NULL
    END)                                         AS part_code
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di.PrimaryVendorID IN('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN di.itemdescription
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN di_ic.itemdescription
    ELSE NULL
  END)                                                                AS part_code_description
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
    THEN di.itemgroupname
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
    THEN di_ic.itemgroupname
    ELSE NULL
    END)                                                              AS part_code_category
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
            OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
            OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
            THEN s.sag_resellervendorid
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN ''   -- Sophos
    ELSE NULL
    END)                                                              AS partner_id
, CAST((CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SUM(-1* it.qty) OVER(
      PARTITION BY s.salesid, it.datephysical, di.itemname, s.inventtransid, s.dataareaid,
      s.purchorderformnum, s.currencycode, pa.addressdescription, CONCAT_WS(', ',SPLIT(pa.addressstreet,'\n')[0],SPLIT(pa.addressstreet,'\n')[1]),
      pa.addresscity, pa.addresszipcode, pa.addresscountryregionisocode,SPLIT(o.contact_name, ' +')[0], SPLIT(o.contact_name, ' +')[1],
      o.emailaddress, s.sag_euaddress_name, CONCAT_WS(', ', s.sag_euaddress_street1, s.sag_euaddress_street2),
      s.sag_euaddress_city, s.sag_euaddress_county, s.sag_euaddress_postcode, s.sag_euaddress_country, s.sag_euaddress_contact, s.sag_euaddress_email,
      o.contactisprimaryforaccount, o.creationdate) -- Sophos
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN ct.qty
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' THEN 1 -- Fortinet
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN ABS(-1 * it.qty)
    WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          THEN SUM(-1* it.qty) OVER (PARTITION BY s.salesname, pa.ADDRESSCOUNTRYREGIONISOCODE, s.SAG_EUADDRESS_NAME, s.SAG_EUADDRESS_COUNTRY, s.CUSTOMERREF, di.ItemName
          /* s.SAG_CREATEDDATETIME should be added also when it will be added to view*/
          )
    ELSE (-1* it.qty)
    END) as BIGINT)                                                             AS quantity
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
    THEN array_join(array_sort(collect_set(it.INVENTSERIALID) over (partition by s.SALESID, it.ItemID, it.INVENTTRANSID, s.DATAAREAID)), ', ') -- Sophos
    ELSE NULL
    END)                                                              AS serial_numbers --- TO DO
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.purchorderformnum -- Sophos
    ELSE NULL
    END)                                                              AS special_pricing_identifier
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.sag_vendorreferencenumber
    ELSE NULL
    END)                                                              AS vendor_promotion
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.sag_vendorstandardcost
    ELSE NULL
    END)                                                              AS mspunit_cost
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.sag_vendorstandardcost * (-1 * it.qty)
    ELSE NULL
    END)                                                              AS mspunit_total_cost
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
    ELSE NULL
    END)                                                              AS reseller_number
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.salesname
    ELSE NULL
    END)                                                              AS sales_name
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.organizationname -- WatchGuard
    ELSE NULL
    END)                                                              AS bill_to_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
            OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
            OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
            THEN pa.ADDRESSDESCRIPTION
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.salesname
    ELSE NULL
    END)                                                              AS reseller_name  -- v_CustomerPrimaryPostalAddressSplit
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
            THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1]) --  Sophos
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1')
            THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1],SPLIT(pa.ADDRESSSTREET,'\n')[2]) --  Extreme
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN pa.ADDRESSSTREET
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            THEN CONCAT_WS(' ', pa.ADDRESSSTREET, pa.ADDRESSCITY)
    ELSE NULL
    END)                                                              AS reseller_address -- v_CustomerPrimaryPostalAddressSplit
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN SPLIT(pa.ADDRESSSTREET,'\n')[0]
    ELSE NULL
    END)                                                              AS reseller_address1
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN SPLIT(pa.ADDRESSSTREET,'\n')[1]
    ELSE NULL
    END)                                                              AS reseller_address2
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN SPLIT(pa.ADDRESSSTREET,'\n')[2]
    ELSE NULL
    END)                                                              AS reseller_address3
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN pa.ADDRESSCITY
    ELSE NULL
    END)                                                              AS reseller_city
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          THEN ''
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN 'NA'
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN pa.ADDRESSSTATE
    ELSE NULL
    END)                                                              AS reseller_state
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.addresszipcode -- WatchGuard
    ELSE NULL
    END)                                                              AS bill_to_postal_code
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN pa.ADDRESSZIPCODE
    ELSE NULL
    END)                                                              AS reseller_postal_code
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.addresscountryregionisocode -- WatchGuard
    ELSE NULL
    END)                                                              AS bill_to_country
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN pa.ADDRESSCOUNTRYREGIONISOCODE
    ELSE NULL
    END)                                                              AS reseller_country
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(o.contact_name, ' +')[0] -- Sophos
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
    ELSE NULL
    END)                                                              AS reseller_contact_first_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(o.contact_name, ' +')[1] -- Sophos
    ELSE NULL
    END)                                                              AS reseller_contact_last_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
    ELSE NULL
    END)                                                              AS reseller_contact_phone_number
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN o.emailaddress -- Sophos
    ELSE NULL
    END)                                                              AS reseller_contact_email -- ora.Oracle_Contacts not ingested
--, 'To Be Done'                                                        AS reseller_email -- probably added by mistake
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN ad.DESCRIPTION
    ELSE NULL
    END)                                                              AS ship_to_name
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
            OR di.PrimaryVendorName LIKE 'WatchGuard%'   -- WatchGuard
          THEN ad.COUNTRYREGIONID
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN b.ISOCODE
    ELSE NULL
    END)                                                              AS ship_to_country
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN ad.ZIPCODE
    ELSE NULL
    END)                                                              AS ship_to_postal_code
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN SPLIT(ad.STREET,'\n')[0]
    ELSE NULL
    END)                                                              AS ship_to_address1
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN SPLIT(ad.STREET,'\n')[1]
    ELSE NULL
    END)                                                              AS ship_to_address2
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN SPLIT(ad.STREET,'\n')[2]
    ELSE NULL
    END)                                                              AS ship_to_address3
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN ad.CITY
    ELSE NULL
    END)                                                              AS ship_to_city
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN ad.COUNTY
    ELSE NULL
    END)                                                              AS ship_to_state
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN ''
    ELSE NULL
    END)                                                              AS end_customer_number
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.sag_euaddress_name
    ELSE NULL
    END)                                                              AS end_customer_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          THEN CONCAT_WS(', ',s.sag_euaddress_street1, s.sag_euaddress_street2)
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN CONCAT_WS(' ',s.sag_euaddress_street1, s.sag_euaddress_street2)
    WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN (CASE
                WHEN s.SAG_EUADDRESS_STREET1 = '' AND s.SAG_EUADDRESS_STREET2 = '' AND s.SAG_EUADDRESS_CITY = '' AND s.SAG_EUADDRESS_COUNTY = '' AND s.SAG_EUADDRESS_POSTCODE = ''
			          THEN ''
			          WHEN s.SAG_EUADDRESS_STREET2 = ''
			          THEN s.SAG_EUADDRESS_STREET1 +  ', ' + s.SAG_EUADDRESS_CITY + ', ' + s.SAG_EUADDRESS_COUNTY + ', ' + s.SAG_EUADDRESS_POSTCODE
			          ELSE s.SAG_EUADDRESS_STREET1 + ', ' + s.SAG_EUADDRESS_STREET2 + ', ' + s.SAG_EUADDRESS_CITY + ', ' + s.SAG_EUADDRESS_COUNTY + ', ' + s.SAG_EUADDRESS_POSTCODE
	              END)
    ELSE NULL
    END)                                                              AS end_customer_address
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
            OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
            OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
            THEN s.sag_euaddress_street1
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            THEN CONCAT(s.sag_euaddress_street1, ' ', s.sag_euaddress_street2, ' ', s.sag_euaddress_city)
    ELSE NULL
    END)                                                              AS end_customer_address1
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.sag_euaddress_street2
    ELSE NULL
    END)                                                              AS end_customer_address2
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR ((di.PrimaryVendorName LIKE 'Juniper%') AND (di.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.sag_euaddress_city
    ELSE NULL
    END)                                                              AS end_customer_city
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.sag_euaddress_county
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN 'NA'
    ELSE NULL
    END)                                                              AS end_customer_state
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.sag_euaddress_postcode
    ELSE NULL
    END)                                                              AS end_customer_postal_code
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')  -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN s.sag_euaddress_country
    ELSE NULL
    END)                                                              AS end_customer_country
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.sag_euaddress_contact
    ELSE NULL
    END)                                                              AS end_customer_contact
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
            OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
            THEN SUBSTRING(s.sag_euaddress_contact, 1, POSITION(' ' IN s.sag_euaddress_contact) - 1)
    ELSE NULL
    END)                                                              AS end_customer_contact_first_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
            OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
            THEN SUBSTRING(s.sag_euaddress_contact, POSITION(' ' IN s.sag_euaddress_contact) + 1, LENGTH(s.sag_euaddress_contact))
    ELSE NULL
    END)                                                              AS end_customer_contact_last_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.sag_euaddress_email -- Sophos
    ELSE NULL
    END)                                                              AS end_customer_email
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN ''
    ELSE NULL
    END)                                                              AS end_customer_phone_number
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
          OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR di_ic.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN it.inventserialid
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN LEFT(UPPER(it.inventserialid),30)
    ELSE NULL
    END)                                                              AS serial_number
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
          OR di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia'
          OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.salesid
    ELSE NULL
    END)                                                              AS d365_sales_order_number
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          THEN 'Infinigate Global Services'
    ELSE 'Infinigate Global Services Ltd'
    END)                                                              AS distributor_name
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN it.invoiceid
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          THEN ct.invoiceid
    ELSE NULL
    END)                                                        AS invoice_number
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
          OR di_ic.PrimaryVendorName LIKE 'Prolabs%'  -- AddOn
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.customerref
    ELSE NULL
    END)                                                              AS reseller_po_to_infinigate
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' -- WatchGuard
            OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
            OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
            OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
            OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
            OR di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
            THEN sp.purchtableid_intercomp
    ELSE NULL
    END)                                                              AS infinigate_po_to_vendor
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') -- Sophos
          OR di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          OR di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
          THEN s.currencycode
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN s.currencycode
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN ct.currencycode
    ELSE NULL
    END)                                                     AS sell_currency
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%'  -- WatchGuard
          OR di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN s.sag_purchprice
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR di.PrimaryVendorID IN ('VAC001388_NGS1') -- Yubico
            THEN sp.purchprice_intercomp
    ELSE NULL
    END)                                                              AS final_vendor_unit_buy_price
, (CASE
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          THEN s.sag_purchprice
    ELSE NULL
    END)                                                              AS final_vendor_unit_buy_price2
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN s.sag_purchprice * (-1 * it.qty)  -- WatchGuard
    ELSE NULL
    END)                                                              AS final_vendor_total_buy_price
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
                                                      WHEN sp.purchlineamount_intercomp = 0 AND sp.purchqty_intercomp = 0 THEN s.sag_purchprice
                                                      ELSE sp.purchlineamount_intercomp / sp.purchqty_intercomp
                                                    END) -- AddOn
    ELSE NULL
    END)                                                              AS po_buy_price
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
                                                      WHEN sp.purchlineamount_intercomp = 0 AND sp.purchqty_intercomp = 0 THEN (-1 * it.qty) * s.sag_purchprice
                                                      ELSE (-1 * it.qty) * (sp.purchlineamount_intercomp / sp.purchqty_intercomp)
                                                    END) -- AddOn
    ELSE NULL
    END)                                                              AS po_total_buy_price
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN sp.purchprice_intercomp / sp.purchqty_intercomp
    ELSE NULL
    END)                                                              AS purchase_price_usd
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN sp.purchprice_intercomp
    ELSE NULL
    END)                                                              AS purchase_extended_price
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (sp.purchprice_intercomp - s.sag_purchprice) * (-1 * it.qty)  -- AddOn
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
           THEN (CASE
                    WHEN di_ic.PrimaryVendorID LIKE 'VAC001014%' THEN (s.sag_vendorstandardcost - s.sag_purchprice) * ct.qty
                    ELSE 0
                END)
    ELSE NULL
    END)                                                              AS claim_amount
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN '' -- Extreme
    ELSE NULL
    END)                                                              AS amount
, di.primaryvendorid                                                  AS vendor_id
, di.primaryvendorname                                                AS vendor_name
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.salesstatus -- Sophos
    ELSE NULL
    END)                                                              AS sales_status
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN s.sag_shipanddebit -- Sophos
    WHEN ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN (CASE
                WHEN s.sag_shipanddebit = 0 THEN 'No'
                WHEN s.sag_shipanddebit = 1 THEN 'Yes'
                END)
    ELSE NULL
    END)                                                              AS ship_and_debit
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN di.itemgroupid -- Sophos
    ELSE NULL
    END)                                                              AS part_code_group_id
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.statusissue  -- WatchGuard
    ELSE NULL
    END)                                                              AS status_issue
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.statusreceipt -- WatchGuard
    ELSE NULL
    END)                                                              AS status_receipt
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.invoicereturned -- WatchGuard
    ELSE NULL
    END)                                                              AS invoice_returned
, (CASE
    WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.packingslipreturned -- WatchGuard
    ELSE NULL
    END)                                                              AS packing_slip_returned
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN '' -- Sophos
    ELSE NULL
    END)                                                              AS price_per_unit_for_this_deal
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN '' -- Sophos
    ELSE NULL
    END)                                                              AS extended_price_for_this_deal
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN NULL -- Sophos
    -- WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' THEN NULL -- Nokia
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') THEN NULL -- Yubico
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') THEN NULL -- Extreme
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) THEN NULL -- Juniper
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') THEN ct.recid -- Cambium
    ELSE it.recid
    END)                                                              AS transaction_record_id
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%'
          OR di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
          OR di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.custaccount  -- AddOn
    ELSE NULL
    END)                                                              AS customer_account
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN sp.purchlineamount_intercomp  -- AddOn
    ELSE NULL
    END)                                                              AS line_amount
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN sp.purchqty_intercomp  -- AddOn
    ELSE NULL
    END)                                                              AS purch_quantity
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
                                                    WHEN sp.purchlineamount_intercomp=0 AND sp.purchqty_intercomp=0 THEN 'PO QTY Zero'
                                                    ELSE ''
                                                    END) -- AddOn
    ELSE NULL
    END)                                                              AS check
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' THEN (CASE
                                                    WHEN s.currencycode = 'USD' THEN s.salesprice
                                                    WHEN s.currencycode = 'GBP' THEN s.salesprice * ex2.rate
                                                    ELSE s.salesprice / ex.rate
                                                    END)  -- AddOn
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN (CASE
                    WHEN ct.currencycode = 'USD' THEN ct.salesprice
                    WHEN ct.currencycode = 'GBP' THEN ct.salesprice * ex2.rate
                    ELSE ct.salesprice / ex.rate
                 END)
    WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
            THEN (CASE
                    WHEN s.currencycode = 'USD' THEN (s.salesprice * (-1 * it.qty))
                    WHEN s.currencycode = 'GBP' THEN (s.salesprice * (-1 * it.qty)) * ex2.rate
                    ELSE (s.salesprice * (-1 * it.qty)) / ex.rate
                 END)
    ELSE NULL
    END)                                                              AS sales_price_usd
, (CASE
    WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
          THEN s.sag_purchprice * (-1 * it.qty)
    ELSE NULL
    END)                                                              AS sales_price_total_usd
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN ct.salesprice
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
          OR ((di.PrimaryVendorID LIKE 'VAC000904_%') OR (di.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          OR di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
            THEN s.salesprice
    WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
            THEN s.salesprice * (-1 * it.qty)
    ELSE NULL
    END)                                                              AS sales_price
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
            OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN sp.purchtableid_intercomp
    ELSE NULL
    END)                                                              AS distributer_purchase_order
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
          THEN ''
    ELSE NULL
    END)                                                              AS country_of_purchase
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
            OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
    END)                                                              AS custom1
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001044_NGS1') -- Extreme
            OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
    END)                                                              AS custom2
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
            THEN (CASE WHEN ct.currencycode = 'USD' THEN 1 ELSE IFNULL(ex.rate, ex2.rate) END)
    WHEN di.PrimaryVendorName LIKE 'Smart Optics%' -- SmartOptics
            THEN (CASE WHEN s.currencycode = 'USD' THEN 1 ELSE IFNULL(ex.rate, ex2.rate) END)
    END)                                                              AS exchange_rate
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            THEN IFNULL(sp.purchprice_intercomp, s.sag_purchprice)
    ELSE NULL
    END)                                                              AS usd_disti_unit_buy_price
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            THEN (CASE WHEN sp.purchtableid_intercomp IS NULL THEN 'Fulfilled from existing distributor stock' ELSE 'Back to Back' END)
    ELSE NULL
    END)                                                              AS order_type
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            THEN s.salesordertype
    ELSE NULL
    END)                                                              AS sales_order_type
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Fortinet%' -- Fortinet
            THEN rwg.row_id
    ELSE NULL
    END)                                                              AS fortinet_row_id
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          THEN (CASE
                WHEN ng.INVENTLOCATIONID LIKE '%2' AND ng.DATAAREAID = 'NNL2' --add post Rome Go Live (MW)
                THEN '101421538' -- Venlo ID
                WHEN ng.INVENTLOCATIONID LIKE'%5' AND ng.DATAAREAID = 'NGS1'
                THEN '101417609' -- UK ID
                WHEN ng.INVENTLOCATIONID LIKE'CORR%'AND ng.DATAAREAID = 'NGS1'
                THEN '101417609' --Added to pick up random name convention for Corrective Warehouse in NGS1
                WHEN ng.INVENTLOCATIONID LIKE '%2' AND ng.DATAAREAID = 'NGS1'
                THEN '101456761' --NGS1 Venlo ID
                END)
    WHEN ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
          THEN (CASE
                WHEN ng.inventlocationid = 'MAIN2' AND ng.dataareaid = 'NNL2' --add post Rome Go Live (MW)
		        THEN '101421538' -- Venlo ID
		        WHEN ng.inventlocationid = 'MAIN5' AND ng.dataareaid = 'NGS1'
		        THEN '101417609' -- UK ID
		        WHEN ng.inventlocationid = 'MAIN2' AND ng.dataareaid = 'NGS1'
		        THEN '101456761' --NGS1 Venlo ID
		        ELSE ''
	            END)
    ELSE NULL
    END)                                                              AS distributer_id_number
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
           THEN (CASE WHEN -1*it.QTY > 0 THEN 'POS'
                WHEN -1*it.QTY <0 THEN 'RD'
                ELSE ''
                END)
    ELSE NULL
    END)                                                              AS distributer_transaction_type
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
           THEN ''
    ELSE NULL
    END)                                                              AS export_licence_number
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
           THEN ''
    ELSE NULL
    END)                                                              AS business_model1
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
           THEN ''
    ELSE NULL
    END)                                                              AS business_model2
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
            OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
           THEN ''
    ELSE NULL
    END)                                                              AS juniper_var_id2
, (CASE
    WHEN ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
           OR ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
           THEN ng.inventlocationid
    ELSE NULL
    END)                                                              AS warehouse
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
          OR di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          OR di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN sp.salestableid_intercomp
    ELSE NULL
    END)                                                              AS intercompany_sales_order
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN SPLIT(di_ic.PrimaryVendorName,' ')[0]
    ELSE NULL
    END)                                                              AS oem_name
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN (CASE
                WHEN di_ic.PrimaryVendorName	LIKE 'Advantech%'
		            THEN di_ic.ItemDescription
			          ELSE di_ic.ItemName
                END)
    ELSE NULL
    END)                                                              AS oem_part_number
, (CASE
    WHEN di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN (CASE
                WHEN RIGHT(sp.SalesTableID_InterComp, 4) = 'NGS1' THEN 'United Kingdom'
		            WHEN RIGHT(sp.SalesTableID_InterComp, 4) = 'NNL2' THEN 'Netherlands'
                END)
    ELSE NULL
    END)                                                              AS shipping_legal_entity
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN di.itemname
    ELSE NULL
    END)                                                              AS snwl_pn_sku
, (CASE
    WHEN di.PrimaryVendorID IN('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          OR ((di_ic.PrimaryVendorName LIKE 'Juniper%') AND (di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))) -- Juniper
          THEN it.inventtransid
    ELSE NULL
    END)                                                              AS invent_trans_id
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            THEN 'Zycko Ltd'
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
            THEN 'Nuvias Global Services Ltd'
    WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
            THEN 'Infinigate UK Ltd'
    ELSE NULL
  END)                                                              AS distributor_account_name
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
            THEN s.SAG_CREATEDDATETIME
    ELSE NULL
  END)                                                              AS sales_order_date
, (CASE
    WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          THEN DATE_FORMAT(s.SAG_CREATEDDATETIME, 'dd-MM-yyyy')
    ELSE NULL
  END)                                                              AS order_date
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN s.SAG_NGS1POBUYPRICE * ex3.RATE
    ELSE NULL
  END)                                                              AS product_cost_eur
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN (s.SAG_NGS1POBUYPRICE * ex3.RATE) * (-1 * it.qty)
    ELSE NULL
  END)                                                              AS product_cost_eur_total
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                              AS blank3
, (CASE
    WHEN di_ic.PrimaryVendorName LIKE 'Nokia%' -- Nokia
          THEN ''
    ELSE NULL
  END)                                                              AS blank4
, (CASE
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          THEN 'www.nuvias.com'
    WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          THEN 'infinigate.co.uk'
    ELSE NULL
  END)                                                              AS distributor_domain
, (CASE
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          THEN 'GB'
    ELSE NULL
  END)                                                              AS distributor_country
, (CASE
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
          THEN s.SAG_RESELLEREMAILADDRESS
    WHEN di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          THEN ''
    ELSE NULL
  END)                                                              AS reseller_email
, (CASE
    WHEN di.PrimaryVendorID IN('VAC001388_NGS1') -- Yubico
          OR di.PrimaryVendorID = 'VAC001358_NGS1' -- Yubico POS Report
          THEN ''
    ELSE NULL
  END)                                                              AS end_customer_domain
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2') -- SonicWall
          THEN s.LINENUM
    ELSE NULL
  END)                                                              AS line_item
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN ''
    ELSE NULL
  END)                                                              AS ship_to_state_jabil
, (CASE
    WHEN di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          THEN ''
    ELSE NULL
  END)                                                              AS end_customer_state_jabil
, (CASE
    WHEN (di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')
            THEN ''
    ELSE NULL
  END)                                                              AS distributer_id_no2
  FROM sales_data s
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
    ON it.inventtransid = s.inventtransid
   AND it.dataareaid NOT IN ('NGS1','NNL2')
  LEFT JOIN so_po_id_list sp
    ON sp.saleslineid_local = s.inventtransid
  LEFT JOIN v_distinctitems di
    ON di.itemid = s.itemid
   AND di.companyid = (CASE WHEN s.dataareaid = 'NUK1' THEN 'NGS1' ELSE 'NNL2' END)
  LEFT JOIN v_distinctitems di_ic
    ON di_ic.itemid = s.itemid
   AND di_ic.companyid = substr(sp.SalesTableID_InterComp, -4)
  LEFT JOIN ngs_inventory ng
    ON ng.inventtransid = sp.SalesLineID_InterComp
   AND (
        (di_ic.PrimaryVendorName LIKE 'Juniper%'
          AND di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2'))  -- Juniper
        OR
        (di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%')) -- Mist
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_custcustomerv3staging WHERE Sys_Silver_IsCurrent = 1) cu
    ON cu.customeraccount = s.custaccount
   AND cu.dataareaid = s.dataareaid
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_customerpostaladdressstaging WHERE Sys_Silver_IsCurrent = 1 AND ISPRIMARY = 1) pa
    ON (  
          (
            di_ic.PrimaryVendorName LIKE 'Fortinet%'
            AND pa.CUSTOMERACCOUNTNUMBER = s.CUSTACCOUNT
          )
          OR
          (
            di_ic.PrimaryVendorName NOT LIKE 'Fortinet%'
            AND pa.CUSTOMERACCOUNTNUMBER = s.INVOICEACCOUNT
          )
        )
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging WHERE Sys_Silver_IsCurrent = 1) ad
    ON ad.ADDRESSRECID = s.DELIVERYPOSTALADDRESS
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_logisticsaddresscountryregionstaging WHERE Sys_Silver_IsCurrent = 1) b
    ON b.COUNTRYREGION = ad.COUNTRYREGIONID
  LEFT JOIN oracle_data o
    ON o.sales_order = s.salesid
  LEFT JOIN (SELECT * FROM silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custinvoicetransstaging WHERE Sys_Silver_IsCurrent = 1) ct
    ON ct.inventtransid = s.inventtransid
   AND ct.dataareaid NOT IN ('NGS1' ,'NNL2')
   AND di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
  LEFT JOIN exchangerates ex
    ON  (
          di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
          AND ex.startdate = TO_DATE(ct.invoicedate)
          AND ex.tocurrency = s.currencycode
          AND ex.fromcurrency = 'USD'
        ) 
    OR  (
          (
              di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
              OR di.PrimaryVendorName LIKE 'Smart Optics%' --SmartOptics
          )
          AND ex.startdate = TO_DATE(it.datephysical)
          AND ex.tocurrency = s.currencycode
          AND ex.fromcurrency = 'USD'
        )
  LEFT JOIN exchangerates ex2
    ON (
        di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2') -- Cambium
        AND ex2.startdate = TO_DATE(ct.invoicedate)
        AND ex2.fromcurrency = s.currencycode
        AND ex2.fromcurrency = 'GBP'
        AND ex2.tocurrency = 'USD'
        )
    OR (
        (
            di_ic.PrimaryVendorName LIKE 'Prolabs%' -- AddOn
            OR di.PrimaryVendorName LIKE 'Smart Optics%' --SmartOptics
        )
        AND ex2.startdate = TO_DATE(it.datephysical)
        AND ex2.fromcurrency = s.currencycode
        
        AND ex2.tocurrency = 'USD'
        )
  LEFT JOIN exchangerates ex3
    ON ex3.startdate = TO_DATE(it.datephysical)
   AND ex3.fromcurrency = 'GBP'
   AND ex3.tocurrency = 'EUR'
  LEFT JOIN row_gen_ rwg
    ON (-1 * it.qty) >= rwg.row_id
   AND di_ic.PrimaryVendorName LIKE 'Fortinet%'
 WHERE 1 = 1
  AND (
        -- Sophos
        (
                s.SALESSTATUS IN ('1', '2', '3') -- MW 20/06/2023 added status 1 to capture part shipments
            AND s.SAG_SHIPANDDEBIT = '1'
            AND di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
            AND di.ItemGroupID = 'Hardware'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- WatchGuard
        (
            (  it.STATUSISSUE IN ('1', '3') OR (it.STATUSRECEIPT LIKE '1' AND it.INVOICERETURNED = 1))
            AND di.PrimaryVendorName LIKE 'WatchGuard%'
            AND it.PACKINGSLIPRETURNED <> 1
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- AddON
        (
            di_ic.PrimaryVendorName LIKE 'Prolabs%'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Extreme
        (
          di.PrimaryVendorID IN ('VAC001044_NGS1')
          AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Cambium
        (
          di_ic.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
          AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Fortinet
        (
            di_ic.PrimaryVendorName LIKE 'Fortinet%'
            AND (-1 * it.qty) > 0
            AND s.salesordertype NOT LIKE 'Demo'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Juniper
        (
            di_ic.PrimaryVendorName LIKE 'Juniper%'
            AND di_ic.PrimaryVendorID NOT IN ('VAC000904_NGS1','VAC000904_NNL2','VAC001110_NGS1','VAC001110_NNL2')
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
            AND ng.INVENTLOCATIONID NOT LIKE 'DD'
        )
        OR
        -- Mist
        (
            ((di_ic.PrimaryVendorID LIKE 'VAC000904_%') OR (di_ic.PrimaryVendorID LIKE 'VAC001110_%'))
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Nokia
        (
            di_ic.PrimaryVendorName LIKE 'Nokia%'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Yubico POS
        (
            it.STATUSISSUE IN ('1', '3')
            AND di.PrimaryVendorID IN('VAC001388_NGS1')
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Yubico POS Report
        (
            di.PrimaryVendorID = 'VAC001358_NGS1'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- SmartOptics
        (
            di.PrimaryVendorName LIKE 'Smart Optics%'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Versa POS v1.2 External
        (
            di_ic.PrimaryVendorID IN ('VAC000850_NGS1', 'VAC000850_NNL2') -- Versa POS 1.2 External
            AND it.STATUSISSUE = '1'
            AND s.SALESSTATUS = '3'
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- SonicWall
        (
            di.PrimaryVendorID IN ('VAC001400_NGS1', 'VAC001401_NGS1', 'VAC001443_NGS1', 'VAC001400_NNL2', 'VAC001401_NNL2', 'VAC001443_NNL2')
            AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
        OR
        -- Jabil v1.2 External
        (
          di.PrimaryVendorID IN ('VAC001208_NGS1', 'VAC001208_NNL2') -- Jabil v1.2 External
          AND it.STATUSISSUE = '1'
          AND s.SALESSTATUS = '3'
          AND s.DATAAREAID NOT IN ('NGS1','NNL2')
        )
    )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW v_pos_reports_common OWNER TO `az_edw_data_engineers_ext_db`
