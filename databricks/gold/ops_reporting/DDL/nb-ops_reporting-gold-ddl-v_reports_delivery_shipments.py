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
# MAGIC create or replace view v_reports_daily_shipments as
# MAGIC with rep_switch as 
# MAGIC   (select 'DS_CSC'     report_name union 
# MAGIC    select 'DS_MAIN'    report_name union
# MAGIC    select 'BECHTLE_DS' report_name) 
# MAGIC select
# MAGIC         cp.dataareaid                                 AS Entity,
# MAGIC         cp.deliverydate                               AS DeliveryDate,
# MAGIC         cp.origsalesid                                AS SalesOrder,
# MAGIC         it.invoiceid                                  AS InvoiceNumber,
# MAGIC         sa.customer                                   AS CustomerAccount,
# MAGIC         cu.organizationname                           AS CustomerName,
# MAGIC         sa.customerref                                AS CustomerPONumber,
# MAGIC         di.itemname                                   AS SKU,
# MAGIC         -1*it.qty                                     AS Quantity,
# MAGIC         it.inventserialid                             AS SerialNumber,
# MAGIC         di.primaryvendorname                          AS Vendor,
# MAGIC         sa.sag_cpqaccountmanager                      AS SalesRep,
# MAGIC         sa.sagtransactionnumber                       AS QuoteRef,
# MAGIC         CASE WHEN ph.sag_trackingnumber IS NULL
# MAGIC              THEN ''
# MAGIC              ELSE ph.sag_trackingnumber
# MAGIC          END                                          AS TrackingNumber,
# MAGIC         cp.inventtransid                              AS InventTransID,
# MAGIC         li.salestableid_intercomp                     AS NGSOrderNumber,
# MAGIC         cp.itemid                                     AS ItemID,
# MAGIC         cp.qty                                        AS Qty,
# MAGIC         rep_switch.report_name                        AS ReportName
# MAGIC FROM    (select * from silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingsliptransstaging where Sys_Silver_IsCurrent = 1) cp
# MAGIC CROSS JOIN rep_switch
# MAGIC LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_salestablestaging sa
# MAGIC        ON sa.salesid = cp.origsalesid
# MAGIC       AND sa.Sys_Silver_IsCurrent = 1
# MAGIC LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventtransstaging it
# MAGIC        ON it.inventtransid = cp.inventtransid
# MAGIC       AND it.datephysical = cp.deliverydate
# MAGIC       AND it.statusissue IN ('1', '2')
# MAGIC       AND it.Sys_Silver_IsCurrent = 1
# MAGIC LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.ara_so_po_id_list li
# MAGIC        ON li.saleslineid_local = cp.inventtransid
# MAGIC       AND li.Sys_Silver_IsCurrent = 1
# MAGIC LEFT JOIN ${catalog_schema}.dbo_sag_custpackingsliptransstaging ps
# MAGIC        ON ps.inventtransid = CASE WHEN rep_switch.report_name = 'DS_CSC'  THEN li.SalesLineID_Local 
# MAGIC                                   WHEN rep_switch.report_name = 'DS_MAIN' THEN li.SalesLineID_InterComp
# MAGIC                                   ELSE li.SalesLineID_InterComp 
# MAGIC                              END
# MAGIC       AND ps.deliverydate = it.datephysical
# MAGIC       AND ps.dataareaid in ('NGS1','NNL2')
# MAGIC       AND ps.Sys_Silver_IsCurrent = 1
# MAGIC LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingslipjourstaging ph
# MAGIC        ON ph.packingslipid = ps.packingslipid
# MAGIC       AND ph.dataareaid in('NGS1','NNL2')
# MAGIC       AND ph.Sys_Silver_IsCurrent = 1
# MAGIC LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_custcustomerv3staging cu
# MAGIC        ON cu.customeraccount = sa.custaccount
# MAGIC       AND cu.dataareaid = cp.dataareaid
# MAGIC       AND cu.Sys_Silver_IsCurrent = 1
# MAGIC LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems di
# MAGIC        ON di.itemid = cp.itemid
# MAGIC       AND di.Sys_Silver_IsCurrent = 1
# MAGIC       AND di.companyid = CASE WHEN rep_switch.report_name = 'BECHTLE_DS' THEN di.companyid
# MAGIC                               WHEN rep_switch.report_name = 'DS_CSC'     THEN 'NNL2'
# MAGIC                               ELSE (CASE WHEN cp.dataareaid = 'NUK1'
# MAGIC                                            THEN 'NGS1'
# MAGIC                                            ELSE 'NNL2'
# MAGIC                                     END)
# MAGIC                               END
# MAGIC    WHERE cp.itemid != 'Delivery_Out'
# MAGIC      AND cp.qty > 0
# MAGIC      AND cp.dataareaid = CASE WHEN rep_switch.report_name = 'DS_CSC'     THEN 'NNL2'
# MAGIC                               WHEN rep_switch.report_name = 'BECHTLE_DS' THEN 'NDE1'
# MAGIC                               ELSE cp.dataareaid
# MAGIC                           END
# MAGIC      AND cu.organizationname ILIKE CASE WHEN rep_switch.report_name = 'BECHTLE_DS'
# MAGIC                                         THEN 'Bechtle%'
# MAGIC                                         ELSE '%'
# MAGIC                                     END
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW v_reports_daily_shipments OWNER TO `az_edw_data_engineers_ext_db`
