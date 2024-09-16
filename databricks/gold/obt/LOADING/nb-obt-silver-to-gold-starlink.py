# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

# DBTITLE 1,Silver to Gold Starlink
spark.sql(
    f"""         
CREATE OR REPLACE VIEW starlink_globaltransactions AS

with cte as 
(SELECT
  'SL' AS GroupEntityCode,
  'AE1' AS EntityCode,
  si.SID,
  to_date(si.Date) AS TransactionDate,
  TO_DATE(si.Sales_Order_Date) AS SalesOrderDate,
  RIGHT(si.Sales_Order_Number,9) AS SalesOrderID,
  si.SKU_ID AS SalesOrderItemID,
  COALESCE(it.SKU_ID, "NaN") AS SKUInternal,
  COALESCE(datanowarr.SKU, 'NaN')  AS SKUMaster,
  COALESCE(it.Description,'NaN') AS Description,
  COALESCE(it.Item_Category,'NaN') AS ProductTypeInternal,
  COALESCE(datanowarr.Product_Type,'NaN') AS ProductTypeMaster,
  coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
  coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
  coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
  coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
  coalesce(ven.Vendor_ID,   'NaN') AS VendorCode,
  coalesce(ven.Vendor_Name, 'NaN') AS VendorNameInternal,
  coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
  'AE1' AS VendorGeography,
  to_date('1900-01-01') AS VendorStartDate,
  --coalesce(cu.Customer_Name,'NaN') AS ResellerCode,
  --coalesce(cu.Customer_Name,'NaN') AS ResellerNameInternal,
  coalesce(rs.Reseller_Name,si.Reseller_Name,'NaN') AS ResellerCode,
  coalesce(rs.Reseller_Name,si.Reseller_Name,'NaN') AS ResellerNameInternal,
  'AE1' AS ResellerGeographyInternal,
  --to_date(coalesce(cu.Date_Created, '1900-01-01' )) AS ResellerStartDate,
  to_date(coalesce(rs.Date_Created, '1900-01-01' )) AS ResellerStartDate,  
  --'NaN' AS ResellerGroupCode,
  --'NaN' AS ResellerGroupName,
  --Comment by MS (30/01/2024) - Start
  --Added reseller group code and reseller group name
  coalesce(rg.ResellerGroupCode,'NaN') AS ResellerGroupCode,
  coalesce(rg.ResellerGroupName,'NaN') AS ResellerGroupName,
  --Comment by MS (30/01/2024) - End
  --to_date(coalesce(cu.Date_Created, '1900-01-01' )) AS ResellerGroupStartDate,
  to_date(coalesce(rs.Date_Created, '1900-01-01' )) AS ResellerGroupStartDate,  
  si.Deal_Currency AS CurrencyCode,
  cast(si.Revenue_USD as DECIMAL(10, 2)) AS RevenueAmount,
  cast((si.Revenue_USD - si.GP_USD)*(-1) as DECIMAL(10, 2))  as CostAmount,
  CAST(si.GP_USD AS  DECIMAL(10, 2)) as GP1


FROM
  silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
  left join silver_{ENVIRONMENT}.netsuite.masterdatasku AS it ON si.SKU_ID = it.SKU_ID
  and it.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatavendor AS ven ON si.Vendor_Name = ven.Vendor_Name
  and ven.Sys_Silver_IsCurrent is not false
  --LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatacustomer AS cu ON si.Customer_Name = cu.Customer_Name
  --and cu.Sys_Silver_IsCurrent = 1
  LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatareseller AS rs ON si.Reseller_Name = rs.Reseller_Name
  and rs.Sys_Silver_IsCurrent = 1
  LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = it.SKU_ID
  --Comment by MS (30/01/2024) - Start
  LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg 
  ON si.Reseller_Name = rg.ResellerName
  AND rg.InfinigateCompany = 'Starlink'
  AND rg.Sys_Silver_IsCurrent = true
  --Comment by MS (30/01/2024) - End
where
  si.Sys_Silver_IsCurrent = 1
/*
Change Date [29/02/2024]
Change BY [MS]
Exclude deleted revenue rows
*/
AND
  si.Sys_Silver_IsDeleted != true
  )

  select
  GroupEntityCode,
  EntityCode,
  TransactionDate,
  SalesOrderDate,
  SalesOrderID,
  SalesOrderItemID,
  SKUInternal,
  case when lower(SKUInternal)  like '%discount%' then 'Discount'
       when lower(SKUInternal)  like '%round%' then 'Rounding'
       when SKUInternal  like '%Funded%' then 'Funding'
       when ProductTypeInternal  like '%Shipping%' then 'Shipping'
       else SKUMaster end as SKUMaster,
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
  EntityCode as VendorGeography,
  case
    when VendorStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, VendorCode)
    else VendorStartDate
  end as VendorStartDate,
  substring_index(ResellerCode, ' ', 1)ResellerCode,
  ResellerNameInternal,
  ResellerGeographyInternal,
  case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerStartDate,
  ResellerGroupCode,
  ResellerGroupName,
case
    when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY EntityCode, ResellerCode)
    else ResellerStartDate
  end as ResellerGroupStartDate,
  CurrencyCode,
  RevenueAmount,
  CostAmount,
  GP1
from
  cte

""")

# COMMAND ----------

spark.sql(
    f"""

CREATE OR REPLACE TABLE globaltransactions_sl_gp1
   
with sl_sum as(
  select
    year(TransactionDate) as PostingYear,
    month(TransactionDate) as PostingMonth,
    ven.IFG_Mapping,
    TAG.VendorName AS TagetikVendorName,
    sum(RevenueAmount) RevenueAmount,
    sum(gp1) AS GP1_VendorSum,
    TAG.GP1_EUR AS GP1_TAG
  from
    gold_dev.obt.starlink_globaltransactions sl
    left join silver_dev.netsuite.masterdatavendor ven on ven.Vendor_ID = sl.VendorCode
    and ven.Sys_Silver_IsCurrent = 1
    LEFT JOIN (
      select
        *
      from
        platinum_dev.obt.globaltransactions_tagetik_reconciliation_ytd -- where upper(VendorName) like '%ACRON%'
      where
        GroupEntityCode = 'SL'
    ) TAG ON year(TransactionDate) = TAG.Year
    and month(TransactionDate) = TAG.Month -- AND ven.IFG_Mapping = UPPER(TAG.VendorName)
    and CHARINDEX(
      case
        when ven.IFG_Mapping = 'JUNIPER & MIST' then 'JUNIPER'
        WHEN ven.IFG_Mapping = 'NETSCOUT ARBOR' THEN 'ARBOR NETWORKS'
        WHEN ven.IFG_Mapping = 'RIVERBED & ATERNITY' THEN 'RIVERBED'
        when ven.IFG_Mapping='H2O' THEN 'DISCONTINUED BUSINESS'
        when ven.IFG_Mapping ='KEYSIGHT' THEN 'IXIA' 
        else ven.IFG_Mapping
      end,
      UPPER(TAG.VendorName)
    ) > 0 
  GROUP BY
    ALL
)


 SELECT
   SL_OBT.GroupEntityCode,
   SL_OBT.EntityCode,
   SL_OBT.TransactionDate,
   SL_OBT.SalesOrderDate,
   SL_OBT.SalesOrderID,
   SL_OBT.SalesOrderItemID,
   SL_OBT.SKUInternal,
   SL_OBT.SKUMaster,
   SL_OBT.Description,
   SL_OBT.ProductTypeInternal,
   SL_OBT.ProductTypeMaster,
   SL_OBT.CommitmentDuration1Master,
   SL_OBT.CommitmentDuration2Master,
   SL_OBT.BillingFrequencyMaster,
   SL_OBT.ConsumptionModelMaster,
   SL_OBT.VendorCode,
   SL_OBT.VendorNameInternal,
   SL_OBT.VendorNameMaster,
   SL_OBT.VendorGeography,
   SL_OBT.VendorStartDate,
   SL_OBT.ResellerCode,
   SL_OBT.ResellerNameInternal,
   SL_OBT.ResellerGeographyInternal,
   SL_OBT.ResellerStartDate,
   SL_OBT.ResellerGroupCode,
   SL_OBT.ResellerGroupName,
   SL_OBT.ResellerGroupStartDate,
   SL_OBT.CurrencyCode,
   SL_OBT.RevenueAmount,
   SL_OBT.CostAmount,
   SL_OBT.GP1 as GP1,
   ven.IFG_Mapping, 
   TagetikVendorName,
   ((SL_OBT.GP1/GP1_VendorSum)* GP1_TAG) AS GP1_Trueup
 from
   gold_{ENVIRONMENT}.obt.starlink_globaltransactions AS SL_OBT
   left join silver_{ENVIRONMENT}.netsuite.masterdatavendor ven on ven.Vendor_ID = SL_OBT.VendorCode
    and ven.Sys_Silver_IsCurrent = 1
  left join sl_sum on 
  ven.IFG_Mapping = sl_sum.IFG_Mapping
   AND year(SL_OBT.TransactionDate) = sl_sum.PostingYear
   AND MONTH(SL_OBT.TransactionDate) = sl_sum.PostingMonth
"""
)

# COMMAND ----------

# df_obt = spark.read.table("globaltransactions")

# df_sl = spark.read.table(f"gold_{ENVIRONMENT}.obt.starlink_globaltransactions")


# COMMAND ----------

# from pyspark.sql.functions import col


# target_columns = df_obt.columns

# source_columns = df_sl.columns

# intersection_columns = [column for column in target_columns if column in source_columns]

# selection_columns = [
#     col(column) for column in intersection_columns if column not in ["SID"]
# ]

# COMMAND ----------

# df_selection = df_sl.select(selection_columns)
# df_selection = df_selection.fillna(value= 'NaN').replace('', 'NaN')

# COMMAND ----------

# df_selection.write.mode("overwrite").option("replaceWhere", "GroupEntityCode = 'SL'").saveAsTable("globaltransactions")
