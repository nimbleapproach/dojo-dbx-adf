# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ops_reporting;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_pos_reports_common AS
# MAGIC WITH distinctitem_cte AS 
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
# MAGIC 	,mg.name					AS itemmodelgroupname
# MAGIC 	,fd.description				AS practicedescr
# MAGIC FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtablestaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_vendvendorv2staging WHERE Sys_Silver_IsCurrent = 1) ve
# MAGIC     ON (ve.vendoraccountnumber = it.primaryvendorid AND ve.dataareaid = it.dataareaid)
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventitemgroupstaging WHERE Sys_Silver_IsCurrent = 1) ig
# MAGIC     ON (ig.itemgroupid = it.itemgroupid AND ig.dataareaid = it.dataareaid)
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventmodelgroupstaging WHERE Sys_Silver_IsCurrent = 1) mg
# MAGIC     ON (mg.modelgroupid = it.modelgroupid AND mg.dataareaid = it.dataareaid)
# MAGIC 	LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_financialdimensionvalueentitystaging WHERE Sys_Silver_IsCurrent = 1) fd
# MAGIC     ON (fd.dimensionvalue = it.PRACTICE AND fd.financialdimension = 'Practice')
# MAGIC WHERE LEFT(primaryvendorid,3) = 'VAC'
# MAGIC ),
# MAGIC v_distinctitems AS
# MAGIC (
# MAGIC   SELECT *--, ROW_NUMBER() OVER(ORDER BY itemid ASC) AS dirn
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
# MAGIC     st.salesid AS salestableid_local,
# MAGIC     sl.inventtransid AS saleslineid_local,
# MAGIC     pt.purchid AS purchtableid_local,
# MAGIC     pl.inventtransid AS purchlineid_local,
# MAGIC     '>> Inter Joins >>' AS hdr,
# MAGIC     pt.intercompanycompanyid,
# MAGIC     pl.intercompanyinventtransid
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) st
# MAGIC   INNER JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
# MAGIC     ON st.salesid = sl.salesid								--Sales Line Local
# MAGIC     AND st.dataareaid = sl.dataareaid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pl
# MAGIC     ON sl.inventreftransid = pl.inventtransid
# MAGIC     AND sl.dataareaid = pl.dataareaid				--Purchase Line Local
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
# MAGIC     pli.inventtransid AS purchlineid_intercomp
# MAGIC   FROM local_id_list lil
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sli
# MAGIC     ON lil.intercompanycompanyid = sli.dataareaid
# MAGIC     AND lil.intercompanyinventtransid = sli.inventtransid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pli
# MAGIC     ON sli.inventreftransid = pli.inventtransid
# MAGIC     AND sli.dataareaid = pli.dataareaid
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC   (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
# MAGIC     ELSE it.datephysical
# MAGIC     END)                                                              AS report_date
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN NULL -- WatchGuard
# MAGIC     ELSE it.datephysical
# MAGIC     END)                                                              AS invoice_date
# MAGIC , (CASE 
# MAGIC      WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.datefinancial -- WatchGuard
# MAGIC      ELSE NULL
# MAGIC      END)                                                             AS financial_date
# MAGIC , sl.dataareaid                                                       AS entity
# MAGIC , di.itemname                                                         AS part_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN di.itemdescription -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC   END)                                                                AS part_code_description
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN di.itemgroupname -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS part_code_category
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sl.sag_resellervendorid -- WatchGuard
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN ''   -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS partner_id
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SUM(-1* it.qty) OVER(
# MAGIC       PARTITION BY sl.salesid, it.datephysical, di.itemname, sl.inventtransid, sl.dataareaid, 
# MAGIC       sh.purchorderformnum, sh.currencycode, pa.addressdescription, CONCAT_WS(', ',SPLIT(pa.addressstreet,'\n')[0],SPLIT(pa.addressstreet,'\n')[1]),
# MAGIC       pa.addresscity, pa.addresszipcode, pa.addresscountryregionisocode,SPLIT(op.contact_name, ' +')[0], SPLIT(op.contact_name, ' +')[1],
# MAGIC       co.emailaddress, sh.sag_euaddress_name, CONCAT_WS(', ', sh.sag_euaddress_street1, sh.sag_euaddress_street2),
# MAGIC       sh.sag_euaddress_city, sh.sag_euaddress_county, sh.sag_euaddress_postcode, sh.sag_euaddress_country, sh.sag_euaddress_contact, sh.sag_euaddress_email,
# MAGIC       co.contactisprimaryforaccount, co.creationdate) -- Sophos 
# MAGIC     ELSE (-1* it.qty) 
# MAGIC     END)                                                              AS quantity 
# MAGIC , (CASE 
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN snc.serial_numbers -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS serial_numbers --- TO DO 
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN sh.purchorderformnum -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS special_pricing_identifier
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN NULL -- Sophos
# MAGIC     ELSE sl.sag_vendorreferencenumber
# MAGIC     END)                                                              AS vendor_promotion
# MAGIC , (CASE 
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sl.sag_vendorstandardcost -- WatchGuard
# MAGIC     ELSE 0
# MAGIC     END)                                                              AS mspunit_cost
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sl.sag_vendorstandardcost * (-1 * it.qty)
# MAGIC     ELSE 0
# MAGIC     END)                                                              AS mspunit_total_cost
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'Prolabs%' THEN sh.salesname -- AddOn
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_name
# MAGIC , (CASE 
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.organizationname -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN pa.ADDRESSDESCRIPTION -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_name  -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN CONCAT_WS(', ',SPLIT(pa.ADDRESSSTREET,'\n')[0],SPLIT(pa.ADDRESSSTREET,'\n')[1]) --  Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_address -- v_CustomerPrimaryPostalAddressSplit
# MAGIC --, SPLIT(pa.ADDRESSSTREET,'\n')[0]                                     AS reseller_address1
# MAGIC --, SPLIT(pa.ADDRESSSTREET,'\n')[1]                                     AS reseller_address2
# MAGIC --, SPLIT(pa.ADDRESSSTREET,'\n')[2]                                     AS reseller_address3
# MAGIC --, SPLIT(pa.ADDRESSSTREET,'\n')[3]                                     AS reseller_address4
# MAGIC --, SPLIT(pa.ADDRESSSTREET,'\n')[4]                                     AS reseller_address5
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN pa.ADDRESSCITY -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_city
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN ''  -- Sophos
# MAGIC     ELSE NULL /*pa.ADDRESSSTATE*/
# MAGIC     END)                                                              AS reseller_state
# MAGIC , (CASE 
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.addresszipcode -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_postal_code
# MAGIC , (CASE 
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN pa.ADDRESSZIPCODE -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_postal_code
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN cu.addresscountryregionisocode -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS bill_to_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN pa.ADDRESSCOUNTRYREGIONISOCODE -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(op.contact_name, ' +')[0] -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_first_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(op.contact_name, ' +')[1] -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_last_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN co.emailaddress -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_contact_email -- ora.Oracle_Contacts not ingested
# MAGIC --, 'To Be Done'                                                        AS reseller_email -- probably added by mistake
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN ad.DESCRIPTION -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN b.ISOCODE -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_country
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN ad.ZIPCODE -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS ship_to_postal_code
# MAGIC --, SPLIT(ad.STREET,'\n')[0]                                            AS ship_to_address1
# MAGIC --, SPLIT(ad.STREET,'\n')[1]                                            AS ship_to_address2
# MAGIC --, SPLIT(ad.STREET,'\n')[2]                                            AS ship_to_address3
# MAGIC --, SPLIT(ad.STREET,'\n')[3]                                            AS ship_to_address4
# MAGIC --, SPLIT(ad.STREET,'\n')[4]                                            AS ship_to_address5
# MAGIC , sh.sag_euaddress_name                                               AS end_customer_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN CONCAT_WS(',',sh.sag_euaddress_street1, sh.sag_euaddress_street2) -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_address
# MAGIC --, sh.sag_euaddress_street1                                            AS end_customer_address1
# MAGIC --, sh.sag_euaddress_street2                                            AS end_customer_address2
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN sh.sag_euaddress_city -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_city
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN sh.sag_euaddress_county -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_state
# MAGIC , sh.sag_euaddress_postcode                                           AS end_customer_postal_code
# MAGIC , sh.sag_euaddress_country                                            AS end_customer_country
# MAGIC --, sh.sag_euaddress_contact                                            AS end_customer_contact
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(sh.sag_euaddress_contact,' ')[0] -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact_first_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN SPLIT(sh.sag_euaddress_contact,' ')[1] -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_contact_last_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN sh.sag_euaddress_email -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS end_customer_email
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN it.inventserialid -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS serial_number
# MAGIC , sl.salesid                                                          AS d365_sales_order_number
# MAGIC , 'Infinigate Global Services Ltd'                                    AS distributor_name
# MAGIC --, it.invoiceid                                                        AS invoice_number
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sh.customerref -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS reseller_po_to_infinigate
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sp.purchtableid_intercomp -- WatchGuard
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS infinigate_po_to_vendor
# MAGIC , sl.currencycode                                                     AS sell_currency
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sl.sag_purchprice -- WatchGuard
# MAGIC     ELSE 0
# MAGIC     END)                                                              AS final_vendor_unit_buy_price
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorName LIKE 'WatchGuard%' THEN sl.sag_purchprice * (-1 * it.qty)  -- WatchGuard
# MAGIC     ELSE 0
# MAGIC     END)                                                              AS final_vendor_total_buy_price
# MAGIC --, 0                                                                   AS claim_amount        --tbd
# MAGIC , di.primaryvendorid                                                  AS vendor_id
# MAGIC , di.primaryvendorname                                                AS vendor_name
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN sl.salesstatus -- Sophos
# MAGIC     ELSE NULL
# MAGIC     END)                                                              AS sales_status
# MAGIC , (CASE
# MAGIC     WHEN di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2') THEN sl.sag_shipanddebit -- Sophos
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
# MAGIC   FROM (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE Sys_Silver_IsCurrent = 1) sl
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE Sys_Silver_IsCurrent = 1) it
# MAGIC     ON it.inventtransid = sl.inventtransid
# MAGIC    AND it.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE Sys_Silver_IsCurrent = 1) sh
# MAGIC     ON sh.salesid = sl.salesid
# MAGIC    AND sh.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN so_po_id_list sp
# MAGIC     ON sp.saleslineid_local = sl.inventtransid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE Sys_Silver_IsCurrent = 1) pl
# MAGIC     ON pl.inventtransid = sp.purchlineid_intercomp
# MAGIC    AND pl.dataareaid IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN v_distinctitems di 
# MAGIC   ON di.itemid = sl.itemid
# MAGIC   AND di.companyid = (CASE WHEN sl.dataareaid = 'NUK1' THEN 'NGS1' ELSE 'NNL2' END)
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_custcustomerv3staging WHERE Sys_Silver_IsCurrent = 1) cu
# MAGIC     ON cu.customeraccount = sh.custaccount
# MAGIC     AND cu.dataareaid = sh.dataareaid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_customerpostaladdressstaging WHERE Sys_Silver_IsCurrent = 1) pa
# MAGIC     ON pa.CUSTOMERACCOUNTNUMBER = sh.INVOICEACCOUNT
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging WHERE Sys_Silver_IsCurrent = 1) ad
# MAGIC     ON ad.ADDRESSRECID = sh.DELIVERYPOSTALADDRESS
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prod_sqlbyod.dbo_logisticsaddresscountryregionstaging WHERE Sys_Silver_IsCurrent = 1) b
# MAGIC     ON b.COUNTRYREGION = ad.COUNTRYREGIONID
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.ora_oracle_opportunities WHERE Sys_Silver_IsCurrent = 1) op
# MAGIC     ON op.sales_order = sl.salesid
# MAGIC   LEFT JOIN (SELECT * FROM silver_dev.nuav_prodtrans_sqlbyod.ora_oracle_contacts WHERE Sys_Silver_IsCurrent = 1) co
# MAGIC     ON CONCAT_WS(' ', STRING(co.firstName), STRING(co.lastName)) = STRING(op.contact_name)
# MAGIC   LEFT JOIN serial_numbers_cte snc
# MAGIC     ON sl.SALESID = snc.SALESID
# MAGIC     AND it.ItemID = snc.ItemID
# MAGIC     AND it.INVENTTRANSID = snc.INVENTTRANSID
# MAGIC     AND sl.DATAAREAID = snc.DATAAREAID
# MAGIC  WHERE 1 = 1 
# MAGIC   AND sl.DATAAREAID NOT IN ('NGS1','NNL2')
# MAGIC   AND pa.ISPRIMARY = 1
# MAGIC   AND (
# MAGIC         -- Sophos
# MAGIC         (
# MAGIC                 sl.SALESSTATUS IN ('1', '2', '3') -- MW 20/06/2023 added status 1 to capture part shipments
# MAGIC             AND sl.SAG_SHIPANDDEBIT = '1'
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
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW gold_dev.ops_reporting.v_pos_reports_common OWNER TO `az_edw_data_engineers_ext_db`
