# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

#  Original Date [19/04/2024]
#  Created BY [YZ]
#  Create table to capture the prorata cost adjustment

spark.sql(f"""

WITH gl AS (
SELECT
  gle.DocumentNo_,
  GLE.PostingDate,
  CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
  gle.Sys_DatabaseName,
  SUM(gle.Amount) as CostAmount
FROM silver_{ENVIRONMENT}.igsql03.g_l_entry gle
LEFT JOIN silver_{ENVIRONMENT}.igsql03.g_l_account ga ON gle.G_LAccountNo_ = ga.No_
                                                      AND gle.Sys_DatabaseName = ga.Sys_DatabaseName
                                                      AND ga.Sys_Silver_IsCurrent=1
                                                      AND gle.Sys_Silver_IsCurrent=1
WHERE gle.Sys_Silver_IsCurrent=1
AND ga.Consol_CreditAcc_ in (
  '371988'
  ,'400988'
  ,'401988'
  ,'402988'
  ,'420988'
  ,'421988'
  ,'422988'
  ,'440188'
  ,'440288'
  ,'440388'
  ,'440588'
  ,'440688'
  ,'440788'
  ,'440888'
  ,'449988'
  ,'450888'
  ,'450988'
  ,'451988'
  ,'452788'
  ,'452888'
  ,'452988'
  ,'468988'
  ,'469988'
  ,'499988')
GROUP BY ALL
HAVING sum(Amount)<>0)


--Aggregate global_transactions and value_entry_adjustments to DocumentNo/PostingDate level and combine.
, obt_ve AS (
SELECT
  CASE WHEN g.EntityCode = 'AT1' THEN 'DE1'
       WHEN g.EntityCode = 'BE1' THEN 'NL1'
       ELSE g.EntityCode 
       END AS EntityCode,
  Sys_DatabaseName,
  g.DocumentNo,
  g.TransactionDate as PostingDate,
  SUM(g.CostAmount)  AS CostAmount
FROM gold_{ENVIRONMENT}.obt.infinigate_globaltransactions g
WHERE SKUInternal NOT like 'IC-%'
GROUP BY ALL
UNION all
SELECT
  EntityCode,
  Sys_DatabaseName,
  DocumentNo_ as DocumentNo,
  PostingDate,
  SUM(CostPostedtoG_L) as CostAmount
FROM gold_{ENVIRONMENT}.obt.value_entry_adjustments
GROUP BY ALL
),


--Aggregate again to Document level.
obt_ve_sum AS (
SELECT
  EntityCode,
  Sys_DatabaseName,
  DocumentNo,
  SUM(CostAmount) CostAmount
FROM obt_ve
GROUP BY ALL
),


--Calculate cost gap between combined global_transactions + value_entry and general ledger
cost_gap AS (
SELECT
  obt_sum.EntityCode,
  obt_sum.Sys_DatabaseName,
  obt_sum.DocumentNo,
  CAST(SUM(coalesce(gl_doc.CostAmount, 0)) as DECIMAL(38,2)) SumCostAmount,
  CAST(SUM(coalesce(gl_doc.CostAmount, 0) - obt_sum.CostAmount) as DECIMAL(38,2)) CostAmount_Gap
FROM obt_ve_sum obt_sum
LEFT JOIN (SELECT EntityCode
                ,Sys_DatabaseName
                ,DocumentNo_
                ,SUM(CostAmount)*(-1) CostAmount 
           FROM gl
           GROUP BY ALL
          ) gl_doc ON obt_sum.EntityCode=gl_doc.EntityCode
                   AND obt_sum.DocumentNo = gl_doc.DocumentNo_
                   AND obt_sum.Sys_DatabaseName = gl_doc.Sys_DatabaseName
GROUP BY ALL
   
  ),


--Splits the cost gap out between varioius posting dates and assigns the correct cost.
cost_adjustment AS (

         SELECT cg.EntityCode
              ,cg.Sys_DatabaseName
              ,cg.DocumentNo
              ,cg.GL_Doc_PostingDate
              ,CASE WHEN SumCostZeroFlag = 1 THEN cg.CostAmount_Gap / COUNT(*) OVER (PARTITION BY cg.EntityCode, cg.Sys_DatabaseName,cg.DocumentNo)
                    ELSE CostAmount_Gap END AS CostAmount_Gap --If total cost = 0, divide by number of records.
              
      FROM (
         SELECT cg.EntityCode
                ,cg.Sys_DatabaseName
                ,cg.DocumentNo DocumentNo
                ,COALESCE(gl.PostingDate, obt.TransactionDate) AS GL_Doc_PostingDate
                ,COALESCE(TRY_DIVIDE((gl.CostAmount * (-1)), cg.SumCostAmount) ,1)  --Works out what ratio this cost is of total cost,
                            * cg.CostAmount_Gap AS CostAmount_Gap                   --and then multiplys by the cost gap to split cost.
                ,ROW_NUMBER() OVER (PARTITION BY cg.EntityCode,cg.Sys_DatabaseName,cg.DocumentNo,COALESCE(gl.PostingDate, obt.TransactionDate)
                                    ORDER BY ABS(DATE_DIFF(gl.PostingDate,obt.TransactionDate)) ASC )  ca_rn --Used to stop duplication from joining to global_transactions
                 ,CASE WHEN cg.SumCostAmount = 0 THEN 1 ELSE 0 END AS SumCostZeroFlag 
         FROM cost_gap cg
         LEFT JOIN gl ON cg.EntityCode=gl.EntityCode
                                AND cg.DocumentNo = gl.DocumentNo_
                                AND cg.Sys_DatabaseName = gl.Sys_DatabaseName
      INNER JOIN gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt ON CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                    WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                    ELSE obt.EntityCode
                                    END = cg.EntityCode
                            AND obt.DocumentNo = cg.DocumentNo
                            AND obt.Sys_DatabaseName= cg.Sys_DatabaseName
      ) cg 
      WHERE ca_rn = 1
   
  ),


--Aggregates value_entry adjustments to be combined in final costs.
value_entry_adjustments AS (
SELECT ve.*,
       COUNT(*) OVER (PARTITION BY  ve.EntityCode, ve.DocumentNo_, ve.DocumentLineNo_, ve.Sys_DatabaseName) ve_count 
FROM (
      SELECT 
        CONCAT( RIGHT (ve.Sys_DatabaseName,2),'1')  AS EntityCode,
        ve.Sys_DatabaseName,
        ve.DocumentLineNo_,
        ve.DocumentNo_,
        ve.PostingDate,
        SUM(ve.CostPostedtoG_L) CostPostedtoG_L

      FROM silver_{ENVIRONMENT}.igsql03.value_entry ve
      LEFT JOIN silver_{ENVIRONMENT}.igsql03.dimension_set_entry region ON region.DimensionCode = 'RPTREGION'
                                                                        AND region.Sys_Silver_IsCurrent =1
                                                                        AND ve.Sys_DatabaseName = region.Sys_DatabaseName
                                                                        AND ve.DimensionSetID=region.DimensionSetID
      WHERE ve.Adjustment = 1
      AND ve.Sys_Silver_IsCurrent =1
      AND ve.DocumentType in (2,4)
      AND NOT(ve.DocumentNo_= 22506211 AND ve.PostingDate = '2024-07-01T00:00:00.000') --Manual exclusion of ≈50 million record added in error to CH1
      GROUP BY ALL)
ve
),


obt_sum AS (
   SELECT
      obt.EntityCode,
      obt.DocumentNo,
      obt.Sys_DatabaseName,
      SUM(obt.RevenueAmount) RevenueAmount_Sum,
      COUNT(obt.LineNo) LineCount
    FROM  gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
    GROUP BY ALL
),

combined_obt_ve as (
SELECT 
   obt.GroupEntityCode
  ,obt.EntityCode
  ,obt.Sys_DatabaseName
  ,obt.DocumentNo
  ,obt.LineNo
  ,obt.Gen_Bus_PostingGroup
  ,obt.MSPUsageHeaderBizTalkGuid
  ,obt.Type
  ,obt.SalesInvoiceDescription
  ,obt.TransactionDate
  ,obt.SalesOrderDate
  ,obt.SalesOrderID
  ,obt.SalesOrderItemID
  ,obt.SKUInternal
  ,obt.Gen_Prod_PostingGroup
  ,obt.SKUMaster
  ,obt.Description
  ,obt.ProductTypeInternal
  ,obt.ProductTypeMaster
  ,obt.CommitmentDuration1Master
  ,obt.CommitmentDuration2Master
  ,obt.BillingFrequencyMaster
  ,obt.ConsumptionModelMaster
  ,obt.VendorCode
  ,obt.VendorNameInternal
  ,obt.VendorNameMaster
  ,obt.VendorGeography
  ,obt.VendorStartDate
  ,obt.ResellerCode
  ,obt.ResellerNameInternal
  ,obt.ResellerGeographyInternal
  ,obt.ResellerStartDate
  ,obt.ResellerGroupCode
  ,obt.ResellerGroupName
  ,obt.ResellerGroupStartDate
  ,obt.CurrencyCode 
  ,obt.RevenueAmount  / COALESCE(ve_count,1)  AS RevenueAmount
  ,obt.CostAmount / COALESCE(ve_count,1)  AS CostAmount
  ,obt.GP1 / COALESCE(ve_count,1)  AS GP1
  ,CAST(COALESCE(VE.CostPostedtoG_L,0) AS DECIMAL(20, 4)) AS CostAmount_ValueEntry
  ,CAST(0.00 AS DECIMAL(20,4)) AS Cost_ProRata_Adj
  ,COALESCE(ve.PostingDate,obt.TransactionDate) AS GL_Doc_PostingDate
FROM gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
LEFT JOIN value_entry_adjustments ve ON obt.DocumentNo = ve.DocumentNo_
                                     AND obt.LineNo = ve.DocumentLineNo_
                                     AND obt.Sys_DatabaseName = ve.Sys_DatabaseName
                                     AND CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                              WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                              ELSE obt.EntityCode END = VE.EntityCode 
)


  SELECT fc.GroupEntityCode
        ,fc.EntityCode
        ,fc.Sys_DatabaseName
        ,fc.DocumentNo
        ,fc.LineNo
        ,fc.Gen_Bus_PostingGroup
        ,fc.MSPUsageHeaderBizTalkGuid
        ,fc.Type
        ,fc.SalesInvoiceDescription
        ,fc.TransactionDate
        ,fc.SalesOrderDate
        ,fc.SalesOrderID
        ,fc.SalesOrderItemID
        ,fc.SKUInternal
        ,fc.Gen_Prod_PostingGroup
        ,fc.SKUMaster
        ,fc.Description
        ,fc.ProductTypeInternal
        ,fc.ProductTypeMaster
        ,fc.CommitmentDuration1Master
        ,fc.CommitmentDuration2Master
        ,fc.BillingFrequencyMaster
        ,fc.ConsumptionModelMaster
        ,fc.VendorCode
        ,fc.VendorNameInternal
        ,fc.VendorNameMaster
        ,fc.VendorGeography
        ,fc.VendorStartDate
        ,fc.ResellerCode
        ,fc.ResellerNameInternal
        ,fc.ResellerGeographyInternal
        ,fc.ResellerStartDate
        ,fc.ResellerGroupCode
        ,fc.ResellerGroupName
        ,fc.ResellerGroupStartDate
        ,fc.CurrencyCode 
        ,SUM(fc.RevenueAmount) AS RevenueAmount
        ,SUM(fc.CostAmount) AS CostAmount
        ,SUM(fc.GP1) AS GP1
        ,SUM(fc.CostAmount_ValueEntry) AS CostAmount_ValueEntry
        ,SUM(fc.Cost_ProRata_Adj) AS Cost_ProRata_Adj
        ,fc.GL_Doc_PostingDate
  FROM (
    SELECT *
    FROM combined_obt_ve
    UNION ALL
    SELECT 
    obt.GroupEntityCode
    ,obt.EntityCode
    ,obt.Sys_DatabaseName
    ,obt.DocumentNo
    ,obt.LineNo
    ,obt.Gen_Bus_PostingGroup
    ,obt.MSPUsageHeaderBizTalkGuid
    ,obt.Type
    ,obt.SalesInvoiceDescription
    ,obt.TransactionDate
    ,obt.SalesOrderDate
    ,obt.SalesOrderID
    ,obt.SalesOrderItemID
    ,obt.SKUInternal
    ,obt.Gen_Prod_PostingGroup
    ,obt.SKUMaster
    ,obt.Description
    ,obt.ProductTypeInternal
    ,obt.ProductTypeMaster
    ,obt.CommitmentDuration1Master
    ,obt.CommitmentDuration2Master
    ,obt.BillingFrequencyMaster
    ,obt.ConsumptionModelMaster
    ,obt.VendorCode
    ,obt.VendorNameInternal
    ,obt.VendorNameMaster
    ,obt.VendorGeography
    ,obt.VendorStartDate
    ,obt.ResellerCode
    ,obt.ResellerNameInternal
    ,obt.ResellerGeographyInternal
    ,obt.ResellerStartDate
    ,obt.ResellerGroupCode
    ,obt.ResellerGroupName
    ,obt.ResellerGroupStartDate
    ,obt.CurrencyCode 
    ,0.00 AS RevenueAmount --Pass through values as 0 as they've already been added in top selection of union.
    ,0.00 AS CostAmount
    ,0.00 AS GP1
    ,0.00 AS CostAmount_ValueEntry
    ,CASE WHEN COALESCE(obt_sum.RevenueAmount_Sum,0) = 0 THEN ca.CostAmount_Gap  / obt_sum.LineCount --If total revenue = 0, divide by number of rcords introduced by obt.
        ELSE CAST(TRY_DIVIDE((obt.RevenueAmount), obt_sum.RevenueAmount_Sum) AS DECIMAL(10, 4)) --Works out what ratio of total revenue this is,
              * ca.CostAmount_Gap                                                               --and then multiplys by the cost gap to split cost.
        END  AS Cost_ProRata_Adj
    ,COALESCE(ca.GL_Doc_PostingDate,obt.TransactionDate) AS GL_Doc_PostingDate
  FROM gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
  INNER JOIN cost_adjustment ca ON CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                      WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                      ELSE obt.EntityCode
                                      END = ca.EntityCode
                              AND obt.DocumentNo = ca.DocumentNo
                              AND obt.Sys_DatabaseName= ca.Sys_DatabaseName
  LEFT JOIN obt_sum ON obt.EntityCode = obt_sum.EntityCode
                    AND obt.DocumentNo = obt_sum.DocumentNo
                    AND obt.Sys_DatabaseName = obt_sum.Sys_DatabaseName
) fc
GROUP BY ALL

""").createOrReplaceTempView('temp')



# COMMAND ----------

spark.sql(f"""
create or replace table infinigate_globaltransactions_cost_adjusted_gl as
with gl_doc_category as (
  select
    distinct gle.DocumentNo_,
    CASE
      WHEN ga.Consol_CreditAcc_ IN (
        '469988',
        '371988',
        '350988',
        '452788',
        '499988',
        '351988',
        '451988',
        '391988'
      ) THEN 'IC'
      ELSE 'Revenue'
    end as GL_Group,
    gle.Sys_DatabaseName,
    concat(right(gle.Sys_DatabaseName, 2), '1') as EntityCode
  from
    silver_{ENVIRONMENT}.igsql03.g_l_entry gle
    left join silver_{ENVIRONMENT}.igsql03.g_l_account ga on gle.G_LAccountNo_ = ga.No_
    and gle.Sys_DatabaseName = ga.Sys_DatabaseName
    and ga.Sys_Silver_IsCurrent = 1
    and gle.Sys_Silver_IsCurrent = 1
  where
    gle.Sys_Silver_IsCurrent = 1
    and ga.Consol_CreditAcc_ in (
      -- Revenue Accounts
      '310388',
      '309988',
      '310988',
      '310688',
      '370988',
      '311088',
      '310888',
      '320988',
      '310188',
      '310788',
      '312088',
      '314088',
      '310488',
      '313088',
      '310588',
      '310288',
      '322988',
      -- IC accounts
      '469988',
      '371988',
      '350988',
      '452788',
      '499988',
      '351988',
      '451988',
      '391988'
    )
  group by
    all
),
gl_doc_category_sum as
(select gl_doc_category.* from gl_doc_category inner join
(select
  distinct DocumentNo_,
  EntityCode,
  count(distinct GL_Group)
from
  gl_doc_category
group by
  all
having
  count(distinct GL_Group) = 1)count on gl_doc_category.EntityCode =count.EntityCode
  and gl_doc_category.DocumentNo_ = count.DocumentNo_)


select temp.*,
gl_doc_category_sum.GL_Group
 from temp
left join gl_doc_category_sum on
temp.Sys_DatabaseName = gl_doc_category_sum.Sys_DatabaseName
    AND temp.DocumentNo = gl_doc_category_sum.DocumentNo_
""")
