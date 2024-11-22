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
# MAGIC , 'To Be Done'                                                        AS reseller_name  -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , 'To Be Done'                                                        AS reseller_address -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , 'To Be Done'                                                        AS reseller_city -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , 'To Be Done'                                                        AS reseller_state -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , cu.addresszipcode                                                   AS bill_to_postal_code
# MAGIC , 'To Be Done'                                                        AS reseller_postal_code -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , cu.addresscountryregionisocode                                      AS bill_to_country
# MAGIC , 'To Be Done'                                                        AS reseller_country -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , 'To Be Done'                                                        AS reseller_contact_name  -- ora.Oracle_Opportunities not ingested
# MAGIC , 'To Be Done'                                                        AS reseller_contact_email -- ora.Oracle_Contacts not ingested
# MAGIC , 'To Be Done'                                                        AS reseller_email -- v_CustomerPrimaryPostalAddressSplit
# MAGIC , 'To Be Done'                                                        AS ship_to_name
# MAGIC , 'To Be Done'                                                        AS ship_to_country
# MAGIC , 'To Be Done'                                                        AS ship_to_postal_code
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
# MAGIC   FROM bronze_dev.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging sl
# MAGIC   LEFT JOIN bronze_dev.nuav_prod_sqlbyod.dbo_sag_inventtransstaging it
# MAGIC     ON it.inventtransid = sl.inventtransid
# MAGIC    AND it.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN bronze_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging sh
# MAGIC     ON sh.salesid = sl.salesid
# MAGIC    AND sh.dataareaid NOT IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN bronze_dev.nuav_prod_sqlbyod.ara_so_po_id_list sp
# MAGIC     ON sp.saleslineid_local = sl.inventtransid
# MAGIC   LEFT JOIN bronze_dev.nuav_prod_sqlbyod.dbo_sag_purchlinestaging pl
# MAGIC     ON pl.inventtransid = sp.purchlineid_intercomp
# MAGIC    AND pl.dataareaid IN ('NGS1','NNL2')
# MAGIC   LEFT JOIN bronze_dev.nuav_prod_sqlbyod.dbo_v_distinctitems di 
# MAGIC     ON di.itemid = sl.itemid
# MAGIC     AND di.companyid = (CASE WHEN sl.dataareaid = 'NUK1' THEN 'NGS1' ELSE 'NNL2' END)
# MAGIC   LEFT JOIN bronze_dev.nuav_prod_sqlbyod.dbo_custcustomerv3staging cu
# MAGIC     ON cu.customeraccount = sh.custaccount
# MAGIC     AND cu.dataareaid = sh.dataareaid
# MAGIC  WHERE 1 = 1 
# MAGIC    AND sl.dataareaid NOT IN ('NGS1','NNL2')
