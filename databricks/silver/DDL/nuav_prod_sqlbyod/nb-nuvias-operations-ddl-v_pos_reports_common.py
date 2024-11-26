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
# MAGIC CREATE OR REPLACE VIEW v_pos_reports_common AS
# MAGIC SELECT
# MAGIC   it.datephysical                                                     AS invoice_date
# MAGIC , it.datefinancial                                                    AS financial_date
# MAGIC , sl.dataareaid                                                       AS entity
# MAGIC , di.itemname                                                         AS part_code
# MAGIC , di.itemdescription                                                  AS part_code_description
# MAGIC , di.itemgroupname                                                    AS part_code_category
# MAGIC , (-1* it.qty)                                                        AS quantity
# MAGIC , 'To Be Done'                                                        AS serial_numbers
# MAGIC , sh.purchorderformnum                                                AS special_pricing_identifier
# MAGIC , sl.sag_vendorreferencenumber                                        AS vendor_promotion
# MAGIC , sl.sag_vendorstandardcost                                           AS mspunit_cost
# MAGIC , sl.sag_vendorstandardcost * (-1 * it.qty)                           AS mspunit_total_cost
# MAGIC
# MAGIC , sh.salesname                                                        AS sales_name
# MAGIC , cu.organizationname                                                 AS bill_to_name
# MAGIC , pa.addressdescription                                               AS reseller_name  -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , 'To Be Done'                                                        AS reseller_address -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , SPLIT(pa.addressstreet,'\n')[0]                                     AS reseller_address1
# MAGIC , SPLIT(pa.addressstreet,'\n')[1]                                     AS reseller_address2
# MAGIC , SPLIT(pa.addressstreet,'\n')[2]                                     AS reseller_address3
# MAGIC , SPLIT(pa.addressstreet,'\n')[3]                                     AS reseller_address4
# MAGIC , SPLIT(pa.addressstreet,'\n')[4]                                     AS reseller_address5
# MAGIC , pa.addresscity                                                      AS reseller_city
# MAGIC , pa.addressstate                                                     AS reseller_state
# MAGIC , cu.addresszipcode                                                   AS bill_to_postal_code
# MAGIC , pa.addresszipcode                                                   AS reseller_postal_code
# MAGIC , cu.addresscountryregionisocode                                      AS bill_to_country
# MAGIC , pa.addresscountryregionisocode                                      AS reseller_country
# MAGIC , 'To Be Done'                                                        AS reseller_contact_name  -- ora.Oracle_Opportunities not ingested
# MAGIC , 'To Be Done'                                                        AS reseller_contact_email -- ora.Oracle_Contacts not ingested
# MAGIC , 'To Be Done'                                                        AS reseller_email -- probably added by mistake
# MAGIC , ad.description                                                      AS ship_to_name
# MAGIC , b.isocode                                                           AS ship_to_country
# MAGIC , ad.zipcode                                                          AS ship_to_postal_code
# MAGIC , SPLIT(ad.street,'\n')[0]                                            AS ship_to_address1
# MAGIC , SPLIT(ad.street,'\n')[1]                                            AS ship_to_address2
# MAGIC , SPLIT(ad.street,'\n')[2]                                            AS ship_to_address3
# MAGIC , SPLIT(ad.street,'\n')[3]                                            AS ship_to_address4
# MAGIC , SPLIT(ad.street,'\n')[4]                                            AS ship_to_address5
# MAGIC , sh.sag_euaddress_name                                               AS end_customer_name
# MAGIC , sh.sag_euaddress_street1                                            AS end_customer_address1
# MAGIC , sh.sag_euaddress_street2                                            AS end_customer_address2
# MAGIC , sh.sag_euaddress_city                                               AS end_customer_city
# MAGIC , sh.sag_euaddress_county                                             AS end_customer_state
# MAGIC , sh.sag_euaddress_postcode                                           AS end_customer_postal_code
# MAGIC , sh.sag_euaddress_country                                            AS end_customer_country
# MAGIC , sh.sag_euaddress_contact                                            AS end_customer_contact
# MAGIC , sh.sag_euaddress_email                                              AS end_customer_email
# MAGIC , it.inventserialid                                                   AS serial_number
# MAGIC , sl.salesid                                                          AS d365_sales_order_number
# MAGIC , CASE
# MAGIC     WHEN pl.lineamount = 0 AND pl.purchqty = 0 THEN sl.sag_purchprice		
# MAGIC     ELSE pl.lineamount / pl.purchqty	
# MAGIC   END                                                                 AS po_unit_buy_price
# MAGIC , 'Infinigate Global Services Ltd'                                    AS distributor_name
# MAGIC , it.invoiceid                                                        AS invoice_number
# MAGIC , sh.customerref                                                      AS reseller_po_to_infinigate
# MAGIC , sp.purchtableid_intercomp                                           AS infinigate_po_to_vendor
# MAGIC , sl.currencycode                                                     AS sell_currency
# MAGIC , sl.sag_purchprice                                                   AS final_vendor_unit_buy_price
# MAGIC , sl.sag_purchprice * (-1 * it.qty)                                   AS final_vendor_total_buy_price
# MAGIC , 0                                                                   AS claim_amount        --tbd
# MAGIC , di.primaryvendorid                                                  AS vendor_id
# MAGIC , di.primaryvendorname                                                AS vendor_name
# MAGIC , sl.salesstatus                                                      AS sales_status
# MAGIC , sl.sag_shipanddebit                                                 AS ship_and_debit
# MAGIC , di.itemgroupid                                                      AS part_code_group_id
# MAGIC , it.statusissue                                                      AS status_issue
# MAGIC , it.statusreceipt                                                    AS status_receipt
# MAGIC , it.invoicereturned                                                  AS invoice_returned
# MAGIC , it.packingslipreturned                                              AS packing_slip_returned  
# MAGIC , sl.sag_resellervendorid                                             AS partner_id
# MAGIC , ''                                                                  AS price_per_unit_for_this_deal
# MAGIC , ''                                                                  AS extended_price_for_this_deal       
# MAGIC   FROM (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(sl1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging sl1)) sl
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(it1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging it1)) it
# MAGIC     ON it.inventtransid = sl.inventtransid
# MAGIC    AND it.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(sh1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging sh1)) sh
# MAGIC     ON sh.salesid = sl.salesid
# MAGIC    AND sh.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.ara_so_po_id_list WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(sp1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.ara_so_po_id_list sp1)) sp
# MAGIC     ON sp.saleslineid_local = sl.inventtransid
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(pl1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging pl1)) pl
# MAGIC     ON pl.inventtransid = sp.purchlineid_intercomp
# MAGIC    AND pl.dataareaid IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_v_distinctitems WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(di1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_v_distinctitems di1)) di 
# MAGIC     ON di.itemid = sl.itemid
# MAGIC     AND di.companyid = (CASE WHEN sl.dataareaid = 'NUK1' THEN 'NGS1' ELSE 'NNL2' END)
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_custcustomerv3staging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(cu1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_custcustomerv3staging cu1)) cu
# MAGIC     ON cu.customeraccount = sh.custaccount
# MAGIC     AND cu.dataareaid = sh.dataareaid
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_customerpostaladdressstaging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(pa1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_customerpostaladdressstaging pa1)) pa
# MAGIC     ON pa.customeraccountnumber = sh.invoiceaccount
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(ad1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging ad1)) ad
# MAGIC     ON ad.addressrecid = sh.deliverypostaladdress
# MAGIC   LEFT JOIN (SELECT * FROM bronze_dev.nuav_prod_sqlbyod.dbo_logisticsaddresscountryregionstaging WHERE TO_DATE(Sys_Bronze_InsertDateTime_UTC) = (SELECT TO_DATE(MAX(b1.Sys_Bronze_InsertDateTime_UTC)) FROM bronze_dev.nuav_prod_sqlbyod.dbo_logisticsaddresscountryregionstaging b1)) b
# MAGIC     ON b.countryregion = ad.countryregionid
# MAGIC  WHERE 1 = 1 
# MAGIC     AND sl.dataareaid NOT IN ('NGS1','NNL2')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW silver_dev.nuav_prod_sqlbyod.v_pos_reports_common OWNER TO `az_edw_data_engineers_ext_db`
