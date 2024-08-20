# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# SET TO False BEFORE DEPLOYMENT
run_local = False # set to False before deploy
spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")
if run_local == True :
    trans = "phil_globaltransactions"
    vuzion_trans = 'phil_vuzion_globaltransactions'
    trans_with_gp1 = f"phil_vuzion_globaltransactions_gp1"
    trans_without_gp1 = f"phil_vuzion_globaltransactions_without_gp1"
    spark.sql(f"drop table if exists {trans_without_gp1}")
    spark.sql(f"drop table if exists {trans_with_gp1}")
    spark.sql(f"drop view if exists {vuzion_trans}")
    spark.sql("drop view if exists Product_GP1")
    spark.sql("drop table if exists detid_deferred_periods")
    spark.sql(f"create table if not exists {trans_with_gp1} as select * from gold_{ENVIRONMENT}.obt.vuzion_globaltransactions_gp1")
    spark.sql(f"create table if not exists {trans} as select * from gold_{ENVIRONMENT}.obt.globaltransactions")
else:
    trans = f"gold_{ENVIRONMENT}.obt.globaltransactions"
    vuzion_trans = f"gold_{ENVIRONMENT}.obt.vuzion_globaltransactions"
    trans_with_gp1 = f"gold_{ENVIRONMENT}.obt.vuzion_globaltransactions_gp1"
    trans_without_gp1 = f"gold_{ENVIRONMENT}.obt.vuzion_globaltransactions_without_gp1"
    spark.sql(f"""TRUNCATE TABLE {trans_without_gp1}""")

print("Running with\n")
print("Tables = ", "\n", trans , "\n", vuzion_trans , "\n", trans_without_gp1)


# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

df_prod_gp1 = spark.sql(f"""
SELECT SKUDescription, GPPercentage
FROM
(
SELECT ROW_NUMBER() OVER(Partition by Product ORDER BY cast(Percentage as DECIMAL(10,2)) DESC) AS Row_Number,
trim(regexp_replace(regexp_replace(Product,'[\\u00A0]',''),"[\n\r]","")) AS SKUDescription,
cast(Percentage as DECIMAL(10,2)) AS GPPercentage
FROM gold_{ENVIRONMENT}.obt.vuzion_gp
WHERE trim(Product) IS NOT NULL
)
WHERE Row_Number = 1                        
""").createOrReplaceTempView("Product_GP1")

# COMMAND ----------

# DBTITLE 1,Derived the deferred transactions and periods

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import Window

# Generate a range of periods for each doc based on DetSDate and DetEDate ranges
def generate_date_range(start, end):
    return [x for x in range(start, end + 86400, 86400)]  # 86400 seconds in a day

# register the UDF
generate_date_range_udf = F.udf(generate_date_range, returnType=ArrayType(IntegerType()))

def create_detid_month_view():
 
    df_docdet = spark.table(f"silver_{ENVIRONMENT}.cloudblue_pba.DocDet")
                   #where from_unixtime(DetSDate, 'yyyy-MM-dd') >='2024-04-01'")

    # we need to group by DocID to provide a count of the months between start and end
    # we use this to identify deferred revenue AND to exclude 10+ year revenue (talk to Maneesh)
    w = Window.partitionBy('DetID')

    # only grab the iscurrent stuff
    filtered_df = df_docdet.filter(
        (df_docdet.Sys_Silver_IsCurrent == True) 
        &
        (
            df_docdet.DetSDate < df_docdet.DetEDate
        )
    )

    # in order to create a range of dates between start and end, they must first be converted to date columns
    # subtract 1 from end date to ensure that amounts are deferred into the correct months because we only
    # defer revenue where the months diff > 2 and < 120. AND the sequence generator below is INCLUSIVE
    # so for example a year would generate 13 periods, hence the minus 1
    filtered_df = filtered_df \
    .withColumn('def_start_date', F.from_unixtime('DetSDate', "yyyy-MM-dd").cast("date")) \
    .withColumn('def_end_date', F.add_months(F.from_unixtime("DetEDate", "yyyy-MM-dd").cast("date"),-1))

    # DO NOT DEFER ANY REVENUE >= 10 YEARS BETWEEN START AND END DATES
    # the code picks up > 2 and <=120.  
    # thats because finance don't defer any revenue where the diff in months is 0-2, they take the full amount
    deferrals_df = filtered_df.filter(
        (F.months_between(filtered_df.def_end_date, filtered_df.def_start_date) > 2) # Not deffered revenune if < = 2 months
        &
        (F.months_between(filtered_df.def_end_date, filtered_df.def_start_date) < 120) # ignore ten year revenue
    )
 
    # Now generate the dates inbetween start and end at the MONTH granularity, i.e. month level
    expanded_df = deferrals_df \
        .withColumn('DeferredDate', F.explode(F.expr('sequence(def_start_date, def_end_date, interval 1 month)'))).select("DetID","DeferredDate") \
        .withColumn("DeferredPeriod", F.date_format("DeferredDate","yyyyMM")) \
        .withColumn("PeriodCount", F.count("DetID").over(w)).distinct()

    # Store this as a temp view for processing later
    if run_local == True:
        expanded_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("detid_deferred_periods")
    else:
        expanded_df.select("DetID", "DeferredDate", "DeferredPeriod", "PeriodCount").createOrReplaceTempView("detid_deferred_periods")
     

create_detid_month_view()

# COMMAND ----------

# DBTITLE 1,Silver to Gold Vuzion
initial_sql = f"""WITH initial_query
AS 
(
--Main Revenue
SELECT
'VU' AS GroupEntityCode
,'Main' AS RevenueType
,'NaN' AS EntityCode
,to_date(cast(ar.DocDate AS TIMESTAMP)) AS TransactionDate
,to_date(cast(ar.DocDate AS TIMESTAMP)) AS SalesOrderDate
,cast(ar.DocID AS STRING) AS SalesOrderID
,cast(dd.DetID AS STRING) AS SalesOrderItemID
,coalesce(dd.SKU,'NaN') AS SKUInternal
,dd.resourceID
,CASE bm.resourceID
WHEN 1002543 then "Azure RI''s"
WHEN 1001618 then "Azure Plan v1"
WHEN 1002489 then "Azure Plan v2"
ELSE coalesce(bm.MPNumber,'NaN')
END AS MPNInternal
,coalesce(datanowarr.SKU, 'NaN') AS SKUMaster
,coalesce(dd.Descr,'NaN') AS Description
,'NaN' AS ProductTypeInternal
,coalesce(datanowarr.Product_Type,'NaN') AS ProductTypeMaster
,coalesce(datanowarr.Commitment_Duration_in_months,'NaN') AS CommitmentDuration1Master
,coalesce(datanowarr.Commitment_Duration_Value,'NaN') AS CommitmentDuration2Master
,coalesce(datanowarr.Billing_Frequency,'NaN') AS BillingFrequencyMaster
,coalesce(datanowarr.Consumption_Model,'NaN') AS ConsumptionModelMaster
,cast(coalesce(bm.Manufacturer,'NaN') AS string) AS VendorCode
,CASE 
WHEN bm.Manufacturer = 'VA-888-104' THEN 'Microsoft'
ELSE coalesce(bm.ManufacturerName,'NaN') 
END AS VendorNameInternal
,coalesce(datanowarr.Vendor_Name,'NaN') AS VendorNameMaster
,'NaN' AS VendorGeography
,cast(coalesce(ar.Customer_AccountID,'NaN') AS string) AS ResellerCode
,coalesce(r.CompanyName,'NaN') AS ResellerNameInternal
,'NaN' AS ResellerGeographyInternal
,coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode 
,coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName 
,coalesce(ar.CurrencyID,'NaN') AS CurrencyCode
,CASE WHEN ar.DocType = 80
THEN cast((coalesce(dd.ExtendedPrice_Value,0.00) * -1) AS DECIMAL(10,2))
ELSE cast(coalesce(dd.ExtendedPrice_Value,0.00) AS DECIMAL(10,2))
END AS RevenueAmount
FROM 
  silver_{ENVIRONMENT}.cloudblue_pba.ARDoc ar
INNER JOIN 
  silver_{ENVIRONMENT}.cloudblue_pba.DocDet dd
ON 
  ar.DocID = dd.DocID
AND
  ar.Sys_Silver_IsCurrent = true
AND
  dd.Sys_Silver_IsCurrent = true
LEFT OUTER JOIN 
  silver_{ENVIRONMENT}.cloudblue_pba.Account r
ON 
  ar.Customer_AccountID = r.AccountID
AND
  r.Sys_Silver_IsCurrent = true
LEFT OUTER JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
ON
  dd.resourceID = bm.resourceID
AND
  bm.Sys_Silver_IsCurrent = true
LEFT JOIN 
(
  SELECT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
  WHERE InfinigateCompany = 'Vuzion'
  AND Sys_Silver_IsCurrent = true
) rg
ON 
  cast(r.AccountID as string) = rg.ResellerID
LEFT JOIN 
  gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr
ON
  datanowarr.SKU = bm.MPNumber
WHERE
  (ar.Vendor_AccountID IN (1000003, 1000007, 20003268, 20019554))
AND 
  (ar.DocType = 20 OR ar.DocType = 80 OR ar.DocType = 90) 
AND 
  (ar.Status = 1000 OR ar.Status = 3000)

UNION ALL
--Cobweb Revenue
SELECT
'VU' AS GroupEntityCode
,'Cobweb' AS RevenueType
,'NaN' AS EntityCode
,to_date(cast(ar.DocDate AS TIMESTAMP)) AS TransactionDate
,to_date(cast(ar.DocDate AS TIMESTAMP)) AS SalesOrderDate
,cast(ar.DocID AS STRING) AS SalesOrderID
,cast(dd.DetID AS STRING) AS SalesOrderItemID
,coalesce(dd.SKU,'NaN') AS SKUInternal
,dd.resourceID
,CASE bm.resourceID
WHEN 1002543 then "Azure RI''s"
WHEN 1001618 then "Azure Plan v1"
WHEN 1002489 then "Azure Plan v2"
ELSE coalesce(bm.MPNumber,'NaN') 
END AS MPNInternal
,coalesce(datanowarr.SKU, 'NaN') AS SKUMaster
,coalesce(dd.Descr,'NaN') AS Description
,'NaN' AS ProductTypeInternal
,coalesce(datanowarr.Product_Type,'NaN') AS ProductTypeMaster
,coalesce(datanowarr.Commitment_Duration_in_months,'NaN') AS CommitmentDuration1Master
,coalesce(datanowarr.Commitment_Duration_Value,'NaN') AS CommitmentDuration2Master
,coalesce(datanowarr.Billing_Frequency,'NaN') AS BillingFrequencyMaster
,coalesce(datanowarr.Consumption_Model,'NaN') AS ConsumptionModelMaster
,cast(coalesce(bm.Manufacturer,'NaN') AS string) AS VendorCode
,CASE 
WHEN bm.Manufacturer = 'VA-888-104' THEN 'Microsoft'
ELSE coalesce(bm.ManufacturerName,'NaN') 
END AS VendorNameInternal
,coalesce(datanowarr.Vendor_Name,'NaN') AS VendorNameMaster
,'NaN' AS VendorGeography
,cast(coalesce(ar.Customer_AccountID,'NaN') AS string) AS ResellerCode
,coalesce(r.CompanyName,'NaN') AS ResellerNameInternal
,'NaN' AS ResellerGeographyInternal
,coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode 
,coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName 
,coalesce(ar.CurrencyID,'NaN') AS CurrencyCode
,CASE WHEN ar.DocType = 80
THEN cast((coalesce(dd.ExtendedPrice_Value,0.00) * -1) AS DECIMAL(10,2))
ELSE cast(coalesce(dd.ExtendedPrice_Value,0.00) AS DECIMAL(10,2))
END AS RevenueAmount
FROM 
  silver_{ENVIRONMENT}.cloudblue_pba.ARDoc ar
INNER JOIN 
  silver_{ENVIRONMENT}.cloudblue_pba.DocDet dd
ON 
  ar.DocID = dd.DocID
AND
  ar.Sys_Silver_IsCurrent = true
AND
  dd.Sys_Silver_IsCurrent = true
LEFT OUTER JOIN 
  silver_{ENVIRONMENT}.cloudblue_pba.Account r
ON 
  ar.Customer_AccountID = r.AccountID
AND
  r.Sys_Silver_IsCurrent = true
LEFT OUTER JOIN
  silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
ON
  dd.resourceID = bm.resourceID
AND
  bm.Sys_Silver_IsCurrent = true
LEFT JOIN 
(
  SELECT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
  WHERE InfinigateCompany = 'Vuzion'
  AND Sys_Silver_IsCurrent = true
) rg
ON 
  cast(r.AccountID as string) = rg.ResellerID
LEFT JOIN 
  gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr
ON
  datanowarr.SKU = bm.MPNumber
WHERE
  (ar.Vendor_AccountID IN (1000011, 20013835, 1000010, 20001095, 20001329, 20023529, 20019394, 20021792, 20031407))
AND 
  (ar.DocType = 20 OR ar.DocType = 80 OR ar.DocType = 90) 
AND 
  (ar.Status = 1000 OR ar.Status = 3000)
)
, main_dates
AS
(
SELECT
  GroupEntityCode,
  RevenueType,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  CASE WHEN MPNInternal = 'NaN' THEN SKUInternal ELSE MPNInternal END AS SKUInternal,
  initial_query.resourceID,
  SKUMaster,
  Description,
  ProductTypeInternal,
  ProductTypeMaster,
  CommitmentDuration1Master,
  CommitmentDuration2Master,
  BillingFrequencyMaster,
  ConsumptionModelMaster,
  VendorCode,
  CASE
  WHEN VendorNameMaster IN ('Acronis','BitTitan','Bluedog','Exclaimer','Infinigate Cloud','LastPass','Microsoft','SignNow')
  THEN VendorNameMaster
  when lower(Description) like '%acronis%' then 'Acronis'
  WHEN  coalesce(coalesce(r1.ManufacturerName, r2.ManufacturerName),VendorNameInternal) = 'VA-888-104' THEN  'Microsoft' 
  ELSE coalesce(coalesce(r1.ManufacturerName, r2.ManufacturerName),VendorNameInternal) END AS VendorNameInternal ,
  VendorNameMaster,
  VendorGeography,
  CASE
    WHEN VendorCode <> 'NaN' THEN MIN(TransactionDate) OVER(PARTITION BY VendorCode)
    ELSE to_date('1900-01-01')
  END AS VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  CASE
    WHEN ResellerCode <> 'NaN' THEN MIN(TransactionDate) OVER(PARTITION BY ResellerCode)
    ELSE to_date('1900-01-01')
  END AS ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
  CurrencyCode,
  RevenueAmount,
  CASE WHEN trim(regexp_extract(Description,'^(.*?):(.*?).Recurring',2)) = "" THEN initcap(trim(Description))
  ELSE initcap(trim(regexp_extract(Description,'^(.*?):(.*?).Recurring',2))) END AS NewDescription
FROM initial_query
 left join (
        select split(MPNumber, ':')[0] as  MPNumber,
        resourceID,
      max(ManufacturerName)ManufacturerName
      from  silver_{ENVIRONMENT}.cloudblue_pba.bmresource
      where Sys_Silver_IsCurrent=1
      AND  ManufacturerName<>'NaN'
      group by all)r1
on initial_query.resourceID = r1.resourceID
 left join (
        select split(MPNumber, ':')[0] as  MPNumber,
      max(ManufacturerName)ManufacturerName
      from  silver_{ENVIRONMENT}.cloudblue_pba.bmresource
      where Sys_Silver_IsCurrent=1
      and ManufacturerName<>'NaN'
      group by all)r2
on split(initial_query.SKUInternal, ':')[0]= r2.MPNumber
)
, results
(
SELECT
  GroupEntityCode,
  RevenueType,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  resourceID,
  SKUMaster,
  Description,
  ProductTypeInternal,
  ProductTypeMaster,
  CommitmentDuration1Master,
  CommitmentDuration2Master,
  BillingFrequencyMaster,
  ConsumptionModelMaster,
  VendorCode,
  case  
      WHEN VendorNameInternal ='NaN'  and lower(Description) like '%signnow%' THEN 'SignNow'
      when VendorNameInternal ='NaN'  and  lower(Description) like '%domain%' THEN 'Domains'
      when VendorNameInternal ='NaN'  and  lower(Description) like '%wavenet%' THEN 'Wavenet' 
    else VendorNameInternal end as VendorNameInternal,
  VendorNameMaster,
  VendorGeography,
  to_date(VendorStartDate,'yyyy-MM-dd') AS VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  to_date(ResellerStartDate,'yyyy-MM-dd') AS ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,  
  CASE
    WHEN ResellerGroupName = 'NaN' THEN to_date('1900-01-01')
    WHEN ResellerGroupName = 'No Group'
    THEN (
      CASE
        WHEN ResellerStartDate <> '1900-01-01' THEN MIN(TransactionDate) OVER(PARTITION BY ResellerCode)
        ELSE to_date('1900-01-01')
      END
    )
    ELSE (
      CASE
        WHEN ResellerStartDate <> '1900-01-01' THEN MIN(TransactionDate) OVER(PARTITION BY ResellerGroupName)
        ELSE to_date('1900-01-01')
      END
    )
  END AS ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount,
  NewDescription  
  FROM main_dates
)
, product_cte
AS
(
SELECT g.*, trim(regexp_replace(regexp_replace(coalesce(m.Product,s.Product),'[\\u00A0]',''),"[\n\r]","")) AS Product
FROM 
  results g
LEFT JOIN
  silver_{ENVIRONMENT}.vuzion_budget.mpn m
ON
  g.SKUInternal = m.`desc`
AND
  m.Sys_Silver_IsCurrent = true
AND
  m.`desc` <> 'NaN'
LEFT JOIN
  silver_{ENVIRONMENT}.vuzion_budget.sku s
ON
  g.SKUInternal = s.`desc`
AND
  s.Sys_Silver_IsCurrent = true
AND
  s.`desc` <> 'NaN'
)

SELECT
  GroupEntityCode,
  RevenueType,  
  EntityCode,
  case when dp.deferreddate is null then TransactionDate 
    else to_date(cast(year(dp.deferreddate) as string)||'-'||right('00'||cast(month(dp.deferreddate) as string),2)||'-01' )
    end as TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  resourceID,
  SKUMaster,
  Description,
  ProductTypeInternal,
  ProductTypeMaster,
  CommitmentDuration1Master,
  CommitmentDuration2Master,
  BillingFrequencyMaster,
  ConsumptionModelMaster,
  VendorCode,
  VendorNameInternal,
  VendorNameMaster,
  VendorGeography,
  VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
  ResellerGroupStartDate,
  CurrencyCode,
  p.GPPercentage,
  NewDescription,
  case when dp.periodcount is null then RevenueAmount
    else cast((g.RevenueAmount / dp.periodcount) as decimal(10,2)) 
    end as RevenueAmount,
  Product,
  case when dp.periodcount is null then false
    else true
    end as DeferredRevenue,
  ifnull(dp.PeriodCount, 1) as PeriodCount
FROM
  product_cte g
LEFT JOIN
  Product_GP1 p
ON
  g.Product = p.SKUDescription
LEFT JOIN 
  detid_deferred_periods dp
ON
  dp.DetID = g.SalesOrderItemID

"""
df_initial = spark.sql(initial_sql)

df_initial.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{trans_without_gp1}")

# COMMAND ----------

# DBTITLE 1,Optimize Gold Transactions
spark.sql(f"""OPTIMIZE {trans_without_gp1}""")
spark.sql(f"""OPTIMIZE gold_{ENVIRONMENT}.obt.vuzion_gp""")

# COMMAND ----------

# DBTITLE 1,Load GP1
from pyspark.sql.functions import levenshtein, regexp_extract, col, row_number, broadcast
from pyspark.sql import Window

df_vuzion_gp = spark.read.table("Product_GP1")

df_vuzion_data = spark.sql(f"""SELECT DISTINCT NewDescription FROM {trans_without_gp1} WHERE Product IS NULL""")

df_vuzion_data.cache()

df_crossjoin = df_vuzion_data.crossJoin(broadcast(df_vuzion_gp))

df_crossjoin_similar = df_crossjoin.withColumn("Similarity",(levenshtein('SKUDescription', 'NewDescription'))).filter('Similarity == 0')

df_crossjoin_similar.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{trans_with_gp1}")


# COMMAND ----------

# DBTITLE 1,Silver to Gold Vuzion
if run_local == True:
  create_sql = f"CREATE OR Replace temporary VIEW {vuzion_trans} AS "
else:
  create_sql = f"CREATE OR Replace VIEW {vuzion_trans} AS "

spark.sql(f"""{create_sql}
SELECT 
  GroupEntityCode,
  RevenueType,  
  EntityCode,
  g.TransactionDate,
  SalesOrderDate,
  g.SalesOrderID,
  g.SalesOrderItemID,
  g.SKUInternal,
  g.resourceID,
  SKUMaster,
  g.Description,
  ProductTypeInternal,
  ProductTypeMaster,
  CommitmentDuration1Master,
  CommitmentDuration2Master,
  BillingFrequencyMaster,
  ConsumptionModelMaster,
  VendorCode,
  VendorNameInternal,
  VendorNameMaster,
  VendorGeography,
  VendorStartDate,
  ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,  
  ResellerGroupStartDate,
  CurrencyCode,
  CASE WHEN g.GPPercentage IS NOT NULL THEN g.GPPercentage ELSE c.GPPercentage END AS GPPercentage,
  g.Product,
  g.NewDescription,  
  RevenueAmount,
  (
    CASE
      WHEN g.GPPercentage IS NOT NULL THEN
        CAST(RevenueAmount - ((RevenueAmount * g.GPPercentage)/100) AS DECIMAL(10,2))
      WHEN c.GPPercentage IS NOT NULL THEN
        CAST(RevenueAmount - ((RevenueAmount * c.GPPercentage)/100) AS DECIMAL(10,2))
      ELSE
        0.00
      END
  ) AS CostAmount,
  (
    CASE
      WHEN g.GPPercentage IS NOT NULL THEN
        CAST((RevenueAmount * g.GPPercentage)/100 AS DECIMAL(10,2))
      WHEN c.GPPercentage IS NOT NULL THEN
        CAST((RevenueAmount * c.GPPercentage)/100 AS DECIMAL(10,2))
      ELSE
        0.00
      END
  ) AS GP1
FROM 
  {trans_without_gp1} g
LEFT JOIN 
  {trans_with_gp1} c
ON
  g.NewDescription = c.NewDescription
WHERE
  upper(g.SKUInternal) <> 'VUZION-TSA'
""")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

df_obt = spark.read.table(f"{trans}")
df_vuzion = spark.read.table(f"{vuzion_trans}")


# COMMAND ----------

from pyspark.sql.functions import col

target_columns = df_obt.columns
source_columns = df_vuzion.columns
intersection_columns = [column for column in target_columns if column in source_columns]
selection_columns = [col(column) for column in intersection_columns if column not in ['SID']]

# COMMAND ----------

df_selection = df_vuzion.select(selection_columns)

# COMMAND ----------

if run_local == True:
  df_selection.createOrReplaceTempView(f"{trans}")
else:
  df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'VU'").saveAsTable(f"{trans}")

