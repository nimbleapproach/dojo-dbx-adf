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
# MAGIC , 'tbd'                                                               AS part_code
# MAGIC , it.qty                                                              AS quantity
# MAGIC , sh.salesname                                                        AS reseller_name
# MAGIC , it.inventserialid                                                   AS serial_number
# MAGIC , 'tbd'                                                               AS d365_sales_order_number
# MAGIC , CASE
# MAGIC     WHEN pl.lineamount = 0 AND pl.purchqty = 0 THEN sl.sag_purchprice		
# MAGIC     ELSE pl.lineamount / pl.purchqty	
# MAGIC   END                                                                 AS po_unit_buy_price
# MAGIC , 'Infinigate Global Services Ltd'                                    AS distributor_name
# MAGIC , it.invoiceid                                                        AS invoice_number
# MAGIC , sh.salesid                                                          AS reseller_po_to_infinigate
# MAGIC , sp.purchtableid_intercomp                                           AS infinigate_po_to_vendor
# MAGIC , sl.currencycode                                                     AS sell_currency
# MAGIC , sl.sag_purchprice                                                   AS final_vendor_buy_price
# MAGIC , 0                                                                   AS claim_amount        --tbd
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
# MAGIC  WHERE 1 = 1 
# MAGIC    AND sl.dataareaid NOT IN ('NGS1','NNL2')
