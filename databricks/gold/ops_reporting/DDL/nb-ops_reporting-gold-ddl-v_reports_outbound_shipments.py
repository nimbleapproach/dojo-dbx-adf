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
# MAGIC create or replace view v_reports_outbound_shipments as
# MAGIC    select
# MAGIC            weekofyear(ps.deliverydate)                       as WeekNo
# MAGIC          , ps.salesid                                        as SalesOrder
# MAGIC          , ps.packingslipid                                  as DeliveryNote
# MAGIC          , ps.deliverydate                                   as ShipDate
# MAGIC          , ps.dlvterm                                        as Terms
# MAGIC          , upper(pj.intercompanycompanyid)                   as IntercompanyCompany
# MAGIC          , pj.sag_trackingnumber                             as TrackingNumber
# MAGIC          , ps.itemid                                         as Item
# MAGIC          , sum(ps.weight)                                    as Weight
# MAGIC          , sum(ps.qty)                                       as Qty
# MAGIC          , upper(id.inventlocationid)                        as Warehouse
# MAGIC          , di.itemgroupname                                  as ItemGroup
# MAGIC          , pr.trackingdimensiongroupname                     as Serialized
# MAGIC          , case when pr.trackingdimensiongroupname = 'SN'
# MAGIC                 then 'Yes'
# MAGIC                 else 'No'
# MAGIC             end                                              as Serialized2
# MAGIC          , di.primaryvendorname                              as VendorName
# MAGIC      from (select * from silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingsliptransstaging where Sys_Silver_IsCurrent = 1) ps
# MAGIC left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_custpackingslipjourstaging pj
# MAGIC        on pj.packingslipid = ps.packingslipid
# MAGIC       and pj.dataareaid in ('NGS1','NNL2')
# MAGIC       and pj.Sys_Silver_IsCurrent = 1
# MAGIC left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_sag_inventdimstaging id
# MAGIC        on id.inventdimid = ps.inventdimid
# MAGIC       and id.dataareaid in ('NGS1','NNL2')
# MAGIC       and id.Sys_Silver_IsCurrent = 1
# MAGIC left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems di
# MAGIC        on di.itemid = ps.itemid
# MAGIC       and di.companyid = substring(ps.salesid, length(ps.salesid)-3, 4)
# MAGIC       and di.Sys_Silver_IsCurrent = 1
# MAGIC left join silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_ecoresproductv2staging pr
# MAGIC        on pr.productnumber = ps.itemid
# MAGIC       and pr.Sys_Silver_IsCurrent = 1
# MAGIC     where ps.dataareaid in ('NGS1','NNL2')
# MAGIC       and ps.qty <> 0
# MAGIC  group by weekofyear(ps.deliverydate)
# MAGIC         , ps.deliverydate
# MAGIC         , ps.salesid
# MAGIC         , ps.packingslipid
# MAGIC         , ps.dlvterm
# MAGIC         , upper(pj.intercompanycompanyid)
# MAGIC         , pj.sag_trackingnumber
# MAGIC         , ps.itemid
# MAGIC         , upper(id.inventlocationid)
# MAGIC         , di.itemgroupname
# MAGIC         , pr.trackingdimensiongroupname
# MAGIC         , di.primaryvendorname
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER VIEW v_reports_outbound_shipments OWNER TO `az_edw_data_engineers_ext_db`
