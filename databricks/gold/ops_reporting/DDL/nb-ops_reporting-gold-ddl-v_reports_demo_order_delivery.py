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
# MAGIC create or replace view v_reports_demo_order_delivery as
# MAGIC    select
# MAGIC           st.dataareaid                                                                     as Entity
# MAGIC         , st.currencycode                                                                   as CurrencyTransaction
# MAGIC         , sl.sag_unitcostinquotecurrency * sl.qtyordered                                    as BuyPriceTotal
# MAGIC         , sl.lineamount - (sl.sag_unitcostinquotecurrency * sl.qtyordered)                  as MarginTotal --Reporting Layer
# MAGIC         , sl.lineamount                                                                     as LineAmount
# MAGIC         , st.salesid                                                                        as SONumber
# MAGIC         , sl.itemid                                                                         as ItemID
# MAGIC         , di.itemname                                                                       as SKU
# MAGIC         , di.primaryvendorid                                                                as PrimaryVendorID
# MAGIC         , di.primaryvendorname                                                              as PrimaryVendorName
# MAGIC         , case
# MAGIC            when st.fixedexchrate/100 = 0
# MAGIC            then 1
# MAGIC            else st.fixedexchrate/100
# MAGIC            end                                                                              as FXRateToBase
# MAGIC         , sl.shippingdaterequested                                                          as RequestedShipDate --?? check this as this is the requested shipping date
# MAGIC         , sl.receiptdaterequested                                                           as RequestedReceiptDate --??? is this needed this is also the requested receipt date
# MAGIC         , ss.salesorderstatusname                                                           as LineStatus
# MAGIC         , di.itemdescription                                                                as ItemDescription
# MAGIC         , st.customerref                                                                    as CustomerPOReference
# MAGIC         , st.sag_cpqaccountmanager                                                          as CPQAccountOwner
# MAGIC         , st.sag_cpqsalestaker                                                              as CPQQuoteCreator
# MAGIC         , st.sagtransactionnumber                                                           as CPQQuoteNumber
# MAGIC         , st.custaccount                                                                    as CustomerAccount
# MAGIC         , di.itemgroupname                                                                  as ItemGroup
# MAGIC         , di.practice                                                                       as Practice
# MAGIC         , st.salesordertype                                                                 as OrderType
# MAGIC         , datediff(current_date(), ps.deliverydate)                                         as DaysOnSite -- Replaced Overdue
# MAGIC         , case when datediff(current_date(), ps.deliverydate) < 55  then '1. OK'
# MAGIC                when datediff(current_date(), ps.deliverydate) >= 55
# MAGIC                 and datediff(current_date(), ps.deliverydate) <= 60 then '2. In Danger'
# MAGIC                when datediff(current_date(), ps.deliverydate) > 60  then '3. Overdue'
# MAGIC           end                                                                               as SalesOrderLineStatus -- replaced SalesOrderLineStatus, need to confirm logic for this
# MAGIC         , st.salesname                                                                      as CustomerName
# MAGIC         , sl.salesqty                                                                       as SalesQty
# MAGIC         , ps.deliverydate                                                                   as ShipDate
# MAGIC    from   (select * from silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_salestablestaging  where Sys_Silver_IsCurrent = 1) st
# MAGIC    left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_saleslinev2staging sl
# MAGIC           on sl.salesid = st.salesid
# MAGIC          and sl.sys_silver_iscurrent = 1
# MAGIC    left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dim_salesorderstatus ss
# MAGIC           on ss.salesorderstatusid = sl.salesstatus
# MAGIC    left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingsliptransstaging ps
# MAGIC           on ps.inventtransid = sl.inventtransid
# MAGIC          and ps.dataareaid = sl.dataareaid
# MAGIC          and ps.sys_silver_iscurrent = 1
# MAGIC    left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems di
# MAGIC           on di.itemid = sl.itemid
# MAGIC          and di.sys_silver_iscurrent = 1
# MAGIC          and di.companyid = case when st.dataareaid = 'NUK1'
# MAGIC                                  then 'NGS1'
# MAGIC                                  else 'NNL2'
# MAGIC                             end
# MAGIC        where st.salesordertype ilike 'DEMO'
# MAGIC          and sl.salesstatus = '2'
# MAGIC          and sl.salesqty > 0
# MAGIC          and sl.itemid not in ('Delivery_Out', '6550686')
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW v_reports_demo_order_delivery OWNER TO `az_edw_data_engineers_ext_db`
