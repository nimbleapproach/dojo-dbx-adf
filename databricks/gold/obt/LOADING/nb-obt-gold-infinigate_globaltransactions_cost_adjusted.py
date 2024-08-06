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
-- create or replace VIEW infinigate_globaltransactions_cost_adjusted as

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
-- AND PostingDate BETWEEN '2023-04-01' AND '2024-03-31'
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

, obt_ve AS (
SELECT
  CASE WHEN g.EntityCode = 'AT1' THEN 'DE1'
       WHEN g.EntityCode = 'BE1' THEN 'NL1'
       ELSE g.EntityCode 
       END AS EntityCode,
  Sys_DatabaseName,
  g.DocumentNo,
  G.TransactionDate as PostingDate,
  SUM(g.CostAmount)  AS CostAmount
FROM gold_{ENVIRONMENT}.obt.infinigate_globaltransactions G
WHERE -- ProductTypeMaster NOT IN ('Logistics', 'Marketing') AND 
    SKUInternal NOT like 'IC-%'
    -- AND Gen_Bus_PostingGroup NOT LIKE 'IC%'
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

obt AS (
SELECT
  EntityCode,
  Sys_DatabaseName,
  DocumentNo,
  MIN(PostingDate) PostingDate,
  SUM(CostAmount) CostAmount
FROM obt_ve
GROUP BY ALL
),

ig_adjustedCost_sum AS (
SELECT
  obt.EntityCode,
  obt.Sys_DatabaseName,
  DocumentNo,
  OBT.PostingDate,
  MIN(gl_doc.PostingDate) AS GL_Doc_PostingDate,
  SUM(obt.CostAmount) CostAmount_OBT,
  SUM(coalesce(gl_doc.CostAmount, 0)) CostAmount_GL,
  SUM(obt.CostAmount)CostAmount_OBT,
  CAST(SUM(coalesce(gl_doc.CostAmount, 0) - obt.CostAmount) as DECIMAL(38,2)) CostAmount_Gap
FROM obt
LEFT JOIN (SELECT EntityCode
                ,Sys_DatabaseName
                ,DocumentNo_
                ,MIN(PostingDate) PostingDate
                ,SUM(CostAmount)*(-1) CostAmount 
           FROM gl
           GROUP BY ALL
          ) gl_doc ON obt.EntityCode=gl_doc.EntityCode
                   AND obt.DocumentNo = gl_doc.DocumentNo_
                   AND obt.Sys_DatabaseName = gl_doc.Sys_DatabaseName
GROUP BY ALL ),

value_entry_adjustments AS (
SELECT 
  CONCAT( RIGHT (ve.Sys_DatabaseName,2),'1')  AS EntityCode,
  ve.Sys_DatabaseName,
  DocumentLineNo_,
  DocumentNo_,
  MIN(PostingDate) PostingDate,
  SUM(CostPostedtoG_L) CostPostedtoG_L
FROM silver_{ENVIRONMENT}.igsql03.value_entry ve
LEFT JOIN (
  SELECT  * 
  FROM silver_{ENVIRONMENT}.igsql03.dimension_set_entry
  WHERE DimensionCode = 'RPTREGION'
  AND Sys_Silver_IsCurrent =1
  ) region ON ve.Sys_DatabaseName = region.Sys_DatabaseName
           AND ve.DimensionSetID=region.DimensionSetID
WHERE Adjustment = 1
AND ve.Sys_Silver_IsCurrent =1
AND DocumentType in (2,4)
GROUP BY ALL
),

cost_adjustment AS (
SELECT
    EntityCode,
    DocumentNo,
    Sys_DatabaseName,
    MIN(GL_Doc_PostingDate) GL_Doc_PostingDate,
    Sum(CostAmount_Gap) CostAmount_Gap
FROM ig_adjustedCost_sum
GROUP BY ALL
)

SELECT
  obt.*,
  -- CAST(try_divide(obt.RevenueAmount, obt_sum.RevenueAmount_Sum) AS DECIMAL(10, 4)) LineRate,
  CAST(COALESCE(VE.CostPostedtoG_L,0) AS DECIMAL(20, 4)) AS CostAmount_ValueEntry,
  CASE WHEN COALESCE(obt_sum.RevenueAmount_Sum,0) = 0 THEN CostAmount_Gap/LineCount
       ELSE CAST(TRY_DIVIDE(obt.RevenueAmount, obt_sum.RevenueAmount_Sum) AS DECIMAL(10, 4))
            * CostAmount_Gap
       END AS Cost_ProRata_Adj,
  COALESCE(ve.PostingDate,COALESCE(cost_adjustment.GL_Doc_PostingDate, obt.TransactionDate)) AS GL_Doc_PostingDate
FROM gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
LEFT JOIN value_entry_adjustments ve ON obt.DocumentNo = ve.DocumentNo_
                                     AND obt.LineNo = ve.DocumentLineNo_
                                     AND obt.Sys_DatabaseName = ve.Sys_DatabaseName
                                     AND CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                              WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                              ELSE obt.EntityCode END = VE.EntityCode
LEFT JOIN(
    SELECT
      EntityCode,
      DocumentNo,
      Sys_DatabaseName,
      SUM(RevenueAmount) RevenueAmount_Sum,
      COUNT(LineNo)LineCount
    FROM  gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions
    --WHERE  ProductTypeMaster NOT IN ('Logistics', 'Marketing')
    GROUP BY ALL
  ) obt_sum ON obt.EntityCode = obt_sum.EntityCode
            AND obt.DocumentNo = obt_sum.DocumentNo
            AND obt.Sys_DatabaseName = obt_sum.Sys_DatabaseName
  LEFT JOIN cost_adjustment ON CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                    WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                    ELSE obt.EntityCode
                                    END = cost_adjustment.EntityCode
                            AND obt.DocumentNo = cost_adjustment.DocumentNo
                            AND obt.Sys_DatabaseName= cost_adjustment.Sys_DatabaseName

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
    -- ga.Consol_CreditAcc_,
    -- ga.ConsolidationAccountName,
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

# COMMAND ----------

# %sql
# create or replace table infinigate_globaltransactions_gl as
# with gl_doc_rev as (
#   select
#     distinct gle.DocumentNo_,

#     max(ga.Consol_CreditAcc_)Consol_CreditAcc_,
#     max(ga.ConsolidationAccountName)ConsolidationAccountName,
#     concat(right(gle.Sys_DatabaseName, 2), '1') as EntityCode
#   from
#     silver_dev.igsql03.g_l_entry gle
#     left join silver_dev.igsql03.g_l_account ga on gle.G_LAccountNo_ = ga.No_
#     and gle.Sys_DatabaseName = ga.Sys_DatabaseName
#     and ga.Sys_Silver_IsCurrent = 1
#     and gle.Sys_Silver_IsCurrent = 1
#   where
#     gle.Sys_Silver_IsCurrent = 1
#     and left(ga.Consol_CreditAcc_,1) in (3)
#   group by
#     all
# ),
# gl_doc_cost as (
#   select
#     distinct gle.DocumentNo_,

#     max(ga.Consol_CreditAcc_)Consol_CreditAcc_,
#     max(ga.ConsolidationAccountName)ConsolidationAccountName,
#     concat(right(gle.Sys_DatabaseName, 2), '1') as EntityCode
#   from
#     silver_dev.igsql03.g_l_entry gle
#     left join silver_dev.igsql03.g_l_account ga on gle.G_LAccountNo_ = ga.No_
#     and gle.Sys_DatabaseName = ga.Sys_DatabaseName
#     and ga.Sys_Silver_IsCurrent = 1
#     and gle.Sys_Silver_IsCurrent = 1
#   where
#     gle.Sys_Silver_IsCurrent = 1
#     and left(ga.Consol_CreditAcc_,1) in (4)
#   group by
#     all
# )
# ,gl_doc_rev_sum as
# (select gl_doc_rev.* from gl_doc_rev inner join
# (select
#   distinct DocumentNo_,
#   EntityCode,
#   count(distinct Consol_CreditAcc_)
# from
#   gl_doc_rev
# group by
#   all
# having
#   count(distinct Consol_CreditAcc_) = 1)count on gl_doc_rev.EntityCode =count.EntityCode
#   and gl_doc_rev.DocumentNo_ = count.DocumentNo_)
#   ,gl_doc_cost_sum as
# (select gl_doc_cost.* from gl_doc_cost inner join
# (select
#   distinct DocumentNo_,
#   EntityCode,
#   count(distinct Consol_CreditAcc_)
# from
#   gl_doc_cost
# group by
#   all
# having
#   count(distinct Consol_CreditAcc_) = 1)count on gl_doc_cost.EntityCode =count.EntityCode
#   and gl_doc_cost.DocumentNo_ = count.DocumentNo_)

  
# select temp.*,
# gl_doc_rev_sum.Consol_CreditAcc_ as Account_Rev,
# gl_doc_rev_sum.ConsolidationAccountName as AccountName_Rev,
# gl_doc_cost_sum.Consol_CreditAcc_ as Account_Cost,
# gl_doc_cost_sum.ConsolidationAccountName as AccountName_Cost
#  from temp
# left join gl_doc_rev_sum on
#  concat( CASE
#     WHEN temp.EntityCode = 'AT1' THEN 'DE1'
#     WHEN temp.EntityCode = 'BE1' THEN 'NL1'
#     ELSE temp.EntityCode end,DocumentNo )= concat(gl_doc_rev_sum.EntityCode,gl_doc_rev_sum.DocumentNo_)
# left join gl_doc_cost_sum on
#  concat( CASE
#     WHEN temp.EntityCode = 'AT1' THEN 'DE1'
#     WHEN temp.EntityCode = 'BE1' THEN 'NL1'
#     ELSE temp.EntityCode end,DocumentNo )= concat(gl_doc_cost_sum.EntityCode,gl_doc_cost_sum.DocumentNo_)
#      AND temp.Sys_DatabaseName = gl_doc_category_sum.Sys_DatabaseName
