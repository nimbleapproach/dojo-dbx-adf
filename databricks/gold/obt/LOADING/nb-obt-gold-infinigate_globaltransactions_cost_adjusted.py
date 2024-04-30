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
with gl as
(select
gle.DocumentNo_,
GLE.PostingDate,
concat(right(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
SUM(gle.Amount) as CostAmount
 from silver_{ENVIRONMENT}.igsql03.g_l_entry gle
 left join silver_{ENVIRONMENT}.igsql03.g_l_account ga
 on gle.G_LAccountNo_ = ga.No_
 and gle.Sys_DatabaseName = ga.Sys_DatabaseName
 and ga.Sys_Silver_IsCurrent=1
 and gle.Sys_Silver_IsCurrent=1
where gle.Sys_Silver_IsCurrent=1

-- AND PostingDate BETWEEN '2023-04-01' AND '2024-03-31'
and ga.Consol_CreditAcc_ in (
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

group by all
having sum(Amount)<>0)

, obt_ve as (
  select
  CASE WHEN g.EntityCode = 'AT1' THEN 'DE1'
      WHEN g.EntityCode = 'BE1' THEN 'NL1'
      ELSE g.EntityCode END AS EntityCode,
  Sys_DatabaseName,
    g.DocumentNo,
    G.TransactionDate as PostingDate,
    SUM(g.CostAmount)  AS CostAmount
  from
    gold_{ENVIRONMENT}.obt.infinigate_globaltransactions G
  
  where

    -- ProductTypeMaster NOT IN ('Logistics', 'Marketing') AND 
    SKUInternal NOT like 'IC-%'
    -- AND Gen_Bus_PostingGroup NOT LIKE 'IC%'
  group by
    all
  UNION all
  select
  EntityCode,
  Sys_DatabaseName,
    DocumentNo_ as DocumentNo,
    PostingDate,
    SUM(CostPostedtoG_L) as CostAmount
  from
    gold_{ENVIRONMENT}.obt.value_entry_adjustments

  group by
    ALL
),
obt as (
  select
  EntityCode,
  Sys_DatabaseName,
    DocumentNo,
    min(PostingDate) as PostingDate,
    sum(CostAmount) CostAmount
  from
    obt_ve
  group by
    all
),
ig_adjustedCost_sum as (
select
obt.EntityCode,
obt.Sys_DatabaseName,
DocumentNo,
OBT.PostingDate,
sum(obt.CostAmount) CostAmount_OBT,
sum(coalesce(gl_doc.CostAmount, 0))CostAmount_GL,
sum(obt.CostAmount)CostAmount_OBT,
cast(sum(coalesce(gl_doc.CostAmount, 0) - obt.CostAmount) as decimal(38,2))CostAmount_Gap
from
  obt
left join (SELECT EntityCode,DocumentNo_,SUM(CostAmount)*(-1) CostAmount FROM gl
group by all
)gl_doc on obt.EntityCode=gl_doc.EntityCode
and obt.DocumentNo = gl_doc.DocumentNo_

group by all ),
value_entry_adjustments as(
  select 

 CONCAT( RIGHT (ve.Sys_DatabaseName,2),'1')  AS EntityCode,
ve.Sys_DatabaseName,

DocumentLineNo_,
PostingDate,
DocumentNo_,
sum(CostPostedtoG_L)as CostPostedtoG_L

from silver_{ENVIRONMENT}.igsql03.value_entry ve

left join (
  select  * from silver_{ENVIRONMENT}.igsql03.dimension_set_entry
  where DimensionCode = 'RPTREGION'
  and Sys_Silver_IsCurrent =1
)region 
on ve.Sys_DatabaseName = region.Sys_DatabaseName
and ve.DimensionSetID=region.DimensionSetID

where Adjustment = 1
and ve.Sys_Silver_IsCurrent =1
and DocumentType in (2,4)

group by all
)


, cost_adjustment as(
  select
    EntityCode,
    DocumentNo,
    Sum(CostAmount_Gap) CostAmount_Gap
  from
    ig_adjustedCost_sum

  group by
    all
)




SELECT
  obt.*,
  -- CAST(
  --   try_divide(obt.RevenueAmount, obt_sum.RevenueAmount_Sum) AS DECIMAL(10, 4)
  -- ) LineRate,
  cast(coalesce(VE.CostPostedtoG_L,0)AS DECIMAL(10, 4)) AS CostAmount_ValueEntry,
   case when coalesce(obt_sum.RevenueAmount_Sum,0) = 0 then CostAmount_Gap/LineCount
          else CAST(
          try_divide(obt.RevenueAmount, obt_sum.RevenueAmount_Sum) AS DECIMAL(10, 4)
        ) * CostAmount_Gap
        end as Cost_ProRata_Adj

from
  gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
  left join (
    select
      EntityCode,
      DocumentNo_,
      DocumentLineNo_,
      Sys_DatabaseName,
      sum(CostPostedtoG_L) CostPostedtoG_L
    from
      value_entry_adjustments
    group by
      all
  ) ve on obt.DocumentNo = ve.DocumentNo_
  and obt.LineNo = ve.DocumentLineNo_
  and obt.Sys_DatabaseName = ve.Sys_DatabaseName
  and CASE
    WHEN obt.EntityCode = 'AT1' THEN 'DE1'
    WHEN obt.EntityCode = 'BE1' THEN 'NL1'
    ELSE obt.EntityCode END = VE.EntityCode
  left join(
    SELECT
      EntityCode,
      DocumentNo,
      sum(RevenueAmount) RevenueAmount_Sum,
        count(LineNo)LineCount
    from
      gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions
     -- where  ProductTypeMaster NOT IN ('Logistics', 'Marketing')

    GROUP BY
      ALL
  ) obt_sum on obt.EntityCode = obt_sum.EntityCode
  and obt.DocumentNo = obt_sum.DocumentNo
  left join cost_adjustment on 
  CASE
    WHEN obt.EntityCode = 'AT1' THEN 'DE1'
    WHEN obt.EntityCode = 'BE1' THEN 'NL1'
    ELSE obt.EntityCode
  END = cost_adjustment.EntityCode
  and obt.DocumentNo = cost_adjustment.DocumentNo
where
  obt.TransactionDate >= '2023-04-01'

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
        '451988'
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
      '451988'
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
