# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

spark.sql(
    f"""

CREATE OR REPLACE VIEW gold_{ENVIRONMENT}.obt.infinigate_general_ledger AS
WITH sales_doc AS (
    SELECT DISTINCT concat(Sys_DatabaseName,No_) No_ 
                                                  from silver_{ENVIRONMENT}.igsql03.sales_invoice_header
                                                  where Sys_Silver_IsCurrent=1
                                                union all
                                                  SELECT DISTINCT concat(Sys_DatabaseName,No_) No_
                                                  from silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header
                                                  where Sys_Silver_IsCurrent=1
),
doc_cost AS (

with doc_in_cost as(
SELECT DISTINCT
  gle.DocumentNo_,
  CASE WHEN region.DimensionValueCode = 'AT' AND gle.Sys_DatabaseName IN('ReportsAT','ReportsDE') THEN 'AT1'
        WHEN region.DimensionValueCode = 'BE' AND gle.Sys_DatabaseName IN('ReportsNL') THEN 'BE1'
       ELSE CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') END as EntityCode,
  gle.Sys_DatabaseName,
  CASE WHEN ga.Consol_CreditAcc_ IN ( '350988',
                                      '351988',
                                      '371988',
                                      '391988',
                                      '451988',
                                      '452788',
                                      '469988',
                                      '499988') THEN 3
      WHEN ga.Consol_CreditAcc_ IN ( '309988',
                                     '310188',
                                     '310288',
                                     '310388',
                                     '310488',
                                     '310588',
                                     '310688',
                                     '310788',
                                     '310888',
                                     '310988',
                                     '311088',
                                     '312088',
                                     '313088',
                                     '314088',
                                     '320988',
                                     '322988',
                                     '370988') THEN 2  
    WHEN ga.Consol_CreditAcc_ IN (  '400988',
                                    '401988',
                                    '402988',
                                    '420988',
                                    '421988',
                                    '422988',
                                    '440188',
                                    '440288',
                                    '440388',
                                    '440588',
                                    '440688',
                                    '440788',
                                    '440888',
                                    '449988',
                                    '450888',
                                    '450988',
                                    '452888',
                                    '452988',
                                    '468988') THEN 1

END as GL_Group,
CASE WHEN ga.Consol_CreditAcc_ IN (
--- Revenue level adj.
'309988'
,'310188'
,'310288'
,'310388'
,'310488'
,'310588'
,'310688'
,'310788'
,'310888'
,'310988'
,'311088'
,'312088'
,'313088'
,'314088'
,'320988'
,'322988'
,'370988'

--- GP1 level adj.
,'350988'
,'351988'
,'391988'
,'371988'
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
,'499988') THEN 1 ELSE 0 

END AS ProRata
FROM silver_{ENVIRONMENT}.igsql03.g_l_entry gle
INNER JOIN silver_{ENVIRONMENT}.igsql03.g_l_account ga ON gle.G_LAccountNo_ = ga.No_
                                                      AND gle.Sys_DatabaseName = ga.Sys_DatabaseName
                                                      AND ga.Sys_Silver_IsCurrent=1
                                                      AND gle.Sys_Silver_IsCurrent=1

LEFT JOIN sales_doc ON concat(gle.Sys_DatabaseName,gle.DocumentNo_) = sales_doc.No_
LEFT JOIN silver_{ENVIRONMENT}.igsql03.dimension_set_entry region ON gle.Sys_DatabaseName = region.Sys_DatabaseName
                                                               AND gle.DimensionSetID = region.DimensionSetID
                                                               AND region.DimensionCode = 'RPTREGION'
                                                               AND region.Sys_Silver_IsCurrent = 1  

WHERE gle.Sys_Silver_IsCurrent=1
AND sales_doc.No_ IS NOT NULL  
GROUP BY ALL)

select distinct 
concat(EntityCode,DocumentNo_)DocumentNo_
 from doc_in_cost
group by all
having max(GL_Group) = 1
) 
SELECT
  gle.DocumentNo_,
  doc_cost.DocumentNo_ as doc_cost_DocNo,
  GLE.PostingDate,
  --CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
  CASE WHEN region.DimensionValueCode = 'AT' AND gle.Sys_DatabaseName IN('ReportsAT','ReportsDE') THEN 'AT1'
        WHEN region.DimensionValueCode = 'BE' AND gle.Sys_DatabaseName IN('ReportsNL') THEN 'BE1'
       ELSE CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') END as EntityCode,
  gle.Sys_DatabaseName,
  ga.Consol_CreditAcc_,
  coalesce(ven.DimensionValueCode, 'NaN')  as VendorCode,
  SUM(gle.Amount) as CostAmount,
  CASE WHEN ga.Consol_CreditAcc_ IN ( '350988',
                                      '351988',
                                      '371988',
                                      '391988',
                                      '451988',
                                      '452788',
                                      '469988',
                                      '499988') THEN 'IC'
      WHEN ga.Consol_CreditAcc_ IN ( '309988',
                                     '310188',
                                     '310288',
                                     '310388',
                                     '310488',
                                     '310588',
                                     '310688',
                                     '310788',
                                     '310888',
                                     '310988',
                                     '311088',
                                     '312088',
                                     '313088',
                                     '314088',
                                     '320988',
                                     '322988',
                                     '370988') THEN 'Revenue'  
        WHEN doc_cost.DocumentNo_ IS NOT NULL 
              AND  ga.Consol_CreditAcc_ IN(
                                      '400988',
                                      '401988',
                                      '402988',
                                      '420988',
                                      '421988',
                                      '422988',
                                      '440188',
                                      '440288',
                                      '440388',
                                      '440588',
                                      '440688',
                                      '440788',
                                      '440888',
                                      '449988',
                                      '450888',
                                      '450988',
                                      '452888',
                                      '452988',
                                      '468988')  THEN 'Cost'
END as GL_Group,
CASE WHEN ga.Consol_CreditAcc_ IN (
--- Revenue level adj.
'309988'
,'310188'
,'310288'
,'310388'
,'310488'
,'310588'
,'310688'
,'310788'
,'310888'
,'310988'
,'311088'
,'312088'
,'313088'
,'314088'
,'320988'
,'322988'
,'370988'

--- GP1 level adj.
,'350988'
,'351988'
,'391988'
,'371988'
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
,'499988') THEN 1 ELSE 0 

END AS ProRata,

CASE WHEN sales_doc.No_ IS NULL
AND ga.Consol_CreditAcc_ in (
--- Revenue level adj.
'309988'
,'310188'
,'310288'
,'310388'
,'310488'
,'310588'
,'310688'
,'310788'
,'310888'
,'310988'
,'311088'
,'312088'
,'313088'
,'314088'
,'320988'
,'322988'
,'370988'

--- GP1 level adj.
,'350988'
,'351988'
,'391988'
,'371988'
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
,'499988') THEN 1 ELSE 0 END AS TopCost

FROM silver_{ENVIRONMENT}.igsql03.g_l_entry gle
INNER JOIN silver_{ENVIRONMENT}.igsql03.g_l_account ga ON gle.G_LAccountNo_ = ga.No_
                                                      AND gle.Sys_DatabaseName = ga.Sys_DatabaseName
                                                      AND ga.Sys_Silver_IsCurrent=1
                                                      AND gle.Sys_Silver_IsCurrent=1
LEFT JOIN silver_{ENVIRONMENT}.igsql03.dimension_set_entry ven ON gle.Sys_DatabaseName = ven.Sys_DatabaseName
                                                               AND gle.DimensionSetID = ven.DimensionSetID
                                                               AND ven.DimensionCode = 'VENDOR'
                                                               AND ven.Sys_Silver_IsCurrent = 1
LEFT JOIN silver_{ENVIRONMENT}.igsql03.dimension_set_entry region ON gle.Sys_DatabaseName = region.Sys_DatabaseName
                                                               AND gle.DimensionSetID = region.DimensionSetID
                                                               AND region.DimensionCode = 'RPTREGION'
                                                               AND region.Sys_Silver_IsCurrent = 1   
LEFT JOIN sales_doc ON concat(gle.Sys_DatabaseName,gle.DocumentNo_) = sales_doc.No_
LEFT JOIN doc_cost ON concat( CASE WHEN region.DimensionValueCode = 'AT' 
                                  AND gle.Sys_DatabaseName IN('ReportsAT','ReportsDE') 
                                  THEN 'AT1'
                                  WHEN region.DimensionValueCode = 'BE' AND gle.Sys_DatabaseName IN('ReportsNL') THEN 'BE1'
                                  ELSE CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') 
                                   END,gle.DocumentNo_)= doc_cost.DocumentNo_

WHERE gle.Sys_Silver_IsCurrent=1
GROUP BY ALL
"""
)
