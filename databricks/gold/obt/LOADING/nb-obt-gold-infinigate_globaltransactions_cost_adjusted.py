# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use SCHEMA obt

# COMMAND ----------

spark.sql(f"""


WITH sales_doc AS (
    SELECT DISTINCT concat(Sys_DatabaseName,No_) No_ 
                                                  from silver_{ENVIRONMENT}.igsql03.sales_invoice_header
                                                  where Sys_Silver_IsCurrent=1
                                                union all
                                                  SELECT DISTINCT concat(Sys_DatabaseName,No_) No_
                                                  from silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header
                                                  where Sys_Silver_IsCurrent=1
)
,doc_in_cost as(
SELECT DISTINCT
  gle.DocumentNo_,
  CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
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

WHERE gle.Sys_Silver_IsCurrent=1
AND sales_doc.No_ IS NOT NULL  
GROUP BY ALL)

select distinct 
concat(Sys_DatabaseName,DocumentNo_)DocumentNo_
 from doc_in_cost
group by all
having max(GL_Group) = 1

""").createOrReplaceTempView('doc_cost')


# COMMAND ----------

spark.sql(f"""

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
  CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
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

WHERE gle.Sys_Silver_IsCurrent=1
AND sales_doc.No_ IS NOT NULL  
GROUP BY ALL)

select distinct 
concat(Sys_DatabaseName,DocumentNo_)DocumentNo_
 from doc_in_cost
group by all
having max(GL_Group) = 1
) 
SELECT
  gle.DocumentNo_,
  GLE.PostingDate,
  CONCAT(RIGHT(gle.Sys_DatabaseName,2 ),'1') as EntityCode,
  gle.Sys_DatabaseName,
 --ga.Consol_CreditAcc_,
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
LEFT JOIN sales_doc ON concat(gle.Sys_DatabaseName,gle.DocumentNo_) = sales_doc.No_
LEFT JOIN doc_cost ON concat(gle.Sys_DatabaseName,gle.DocumentNo_)= doc_cost.DocumentNo_

WHERE gle.Sys_Silver_IsCurrent=1
GROUP BY ALL""")

# COMMAND ----------

spark.sql(f"""
WITH obt_ve AS (
  SELECT
    CASE
      WHEN g.EntityCode = 'AT1' THEN 'DE1'
      WHEN g.EntityCode = 'BE1' THEN 'NL1'
      ELSE g.EntityCode
    END AS EntityCode,
    Sys_DatabaseName,
    g.VendorCode,
    g.DocumentNo,
    g.TransactionDate as PostingDate,
    SUM(g.RevenueAmount) AS RevenueAmount,
    SUM(g.CostAmount) AS CostAmount
  FROM
    gold_{ENVIRONMENT}.obt.infinigate_globaltransactions g
  WHERE
    SKUInternal NOT like 'IC-%'
  GROUP BY
    ALL
  UNION all
  SELECT
    EntityCode,
    Sys_DatabaseName,
    VendorCode,
    DocumentNo_ as DocumentNo,
    PostingDate,
    0 AS RevenueAmount,
    SUM(CostPostedtoG_L) as CostAmount
  FROM
    gold_{ENVIRONMENT}.obt.value_entry_adjustments
  GROUP BY
    ALL
),
prorata AS (
  SELECT
    *
  FROM
    gold_{ENVIRONMENT}.obt.infinigate_general_ledger
  WHERE
    ProRata = 1
    AND TopCost = 0
),
obt_ve_sum AS (
  SELECT
    EntityCode,
    Sys_DatabaseName,
    DocumentNo,
    PostingDate,
    VendorCode,
    SUM(RevenueAmount + CostAmount) as CostAmount
  FROM
    obt_ve
  GROUP BY
    ALL
),
cost_gap AS (
  SELECT
    obt_sum.EntityCode,
    obt_sum.Sys_DatabaseName,
    obt_sum.DocumentNo,
    obt_sum.VendorCode,
    obt_sum.PostingDate,
    CAST(
      SUM(coalesce(gl_doc.CostAmount, 0)) as DECIMAL(38, 2)
    ) SumCostAmount,
    CAST(
      SUM(
        coalesce(gl_doc.CostAmount, 0) - obt_sum.CostAmount
      ) as DECIMAL(38, 2)
    ) CostAmount_Gap
  FROM
    obt_ve_sum obt_sum
    LEFT JOIN (
      SELECT
        EntityCode,
        Sys_DatabaseName,
        DocumentNo_,
        VendorCode,
        PostingDate,
        SUM(CostAmount) *(-1) CostAmount
      FROM
        prorata
      GROUP BY
        ALL
    ) gl_doc ON obt_sum.EntityCode = gl_doc.EntityCode
    AND obt_sum.DocumentNo = gl_doc.DocumentNo_
    AND obt_sum.Sys_DatabaseName = gl_doc.Sys_DatabaseName
    AND obt_sum.PostingDate = gl_doc.PostingDate
    and obt_sum.VendorCode = gl_doc.VendorCode
  GROUP BY
    ALL
) --- the part not matching sales invoice
,
cost_gap_missed as (
  select
    prorata.EntityCode,
    prorata.Sys_DatabaseName,
    prorata.DocumentNo_,
    coalesce(prorata.VendorCode, 'NaN') VendorCode,
    prorata.PostingDate,
    0 as SumCostAmount,
    CAST(
      SUM(coalesce((prorata.CostAmount) *(-1), 0)) as DECIMAL(38, 2)
    ) CostAmount_Gap
  FROM
    prorata
    left join obt_ve_sum ON prorata.DocumentNo_ = obt_ve_sum.DocumentNo
    AND prorata.EntityCode = obt_ve_sum.EntityCode
    AND prorata.Sys_DatabaseName = obt_ve_sum.Sys_DatabaseName
    AND prorata.PostingDate = obt_ve_sum.PostingDate
    AND prorata.VendorCode = obt_ve_sum.VendorCode
  WHERE
    obt_ve_sum.DocumentNo is null
  group by
    all
),

--Splits the cost gap out between posting dates and assigns the correct cost.
cost_adjustment AS (

         SELECT cg.EntityCode
              ,cg.Sys_DatabaseName
              ,cg.VendorCode
              ,cg.DocumentNo
              ,cg.GL_Doc_PostingDate
              ,CASE WHEN SumCostZeroFlag = 1 THEN cg.CostAmount_Gap / COUNT(*) OVER (PARTITION BY cg.EntityCode, cg.Sys_DatabaseName,cg.DocumentNo)
                    ELSE CostAmount_Gap END AS CostAmount_Gap --If total cost = 0, divide by number of records.
      FROM (
         SELECT cg.EntityCode
                ,cg.Sys_DatabaseName
                ,cg.VendorCode
                ,cg.DocumentNo DocumentNo
                --,COALESCE(gl.PostingDate, obt.TransactionDate) AS GL_Doc_PostingDate
                ,COALESCE(gl.PostingDate, CAST( CAST((CAST(YEAR(cg.PostingDate)AS VARCHAR(4)) + '-' + CAST(MONTH(cg.PostingDate) AS VARCHAR(2)) + '-01') AS VARCHAR(12)) AS DATE)) AS GL_Doc_PostingDate
                --,COUNT(*) OVER (PARTITION BY cg.EntityCode,cg.Sys_DatabaseName,cg.DocumentNo,COALESCE(gl.PostingDate, obt.TransactionDate) gt_count
                ,(COALESCE(TRY_DIVIDE((gl.CostAmount * (-1)), cg.SumCostAmount) ,1)  --Works out what ratio this cost is of total cost,
                            * cg.CostAmount_Gap) --/ gt_count 
                            AS CostAmount_Gap                   --and then multiplys by the cost gap to split cost.

                ,(gl.CostAmount * (-1)) gl_costGap
                , cg.SumCostAmount
                --,ROW_NUMBER() OVER (PARTITION BY cg.EntityCode,cg.Sys_DatabaseName,cg.DocumentNo,COALESCE(gl.PostingDate, obt.TransactionDate)
                            --        ORDER BY ABS(DATE_DIFF(gl.PostingDate,obt.TransactionDate)) ASC )  ca_rn --Used to stop duplication from joining to global_transactions
                  --,ROW_NUMBER() OVER (PARTITION BY cg.EntityCode,cg.Sys_DatabaseName,cg.DocumentNo,GL_Doc_PostingDate
                  --                  ORDER BY ABS(DATE_DIFF(gl.PostingDate,obt.TransactionDate)) ASC )  ca_rn --Used to stop duplication from joining to global_transactions

                 ,CASE WHEN cg.SumCostAmount = 0 THEN 1 ELSE 0 END AS SumCostZeroFlag 
         FROM cost_gap cg
         LEFT JOIN prorata gl ON cg.EntityCode=gl.EntityCode
                              AND cg.DocumentNo = gl.DocumentNo_
                              AND cg.Sys_DatabaseName = gl.Sys_DatabaseName
                              AND gl.ProRata = 1 
                              AND gl.PostingDate = cg.PostingDate
                              AND gl.VendorCode = cg.VendorCode

      /*
      INNER JOIN gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt ON CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                    WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                    ELSE obt.EntityCode
                                    END = cg.EntityCode
                            AND obt.DocumentNo = cg.DocumentNo
                            AND obt.Sys_DatabaseName= cg.Sys_DatabaseName
         */                   
      ) cg 
      --WHERE ca_rn = 1

      UNION ALL
      SELECT 
          EntityCode
          ,Sys_DatabaseName
          ,VendorCode
          ,DocumentNo_
          ,PostingDate as GL_Doc_PostingDate
          ,CostAmount_Gap
      FROM cost_gap_missed

   
   
  ),


--Aggregates value_entry adjustments to be combined in final costs.
value_entry_adjustments AS (
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
      --AND NOT(ve.DocumentNo_= 22506211 AND ve.PostingDate = '2024-07-01T00:00:00.000') --Manual exclusion of ≈50 million record added in error to CH1
      GROUP BY ALL)
,


obt_sum AS (
   SELECT
      obt.EntityCode,
      obt.DocumentNo,
      obt.Sys_DatabaseName,
      obt.VendorCode,
      SUM(obt.RevenueAmount) RevenueAmount_Sum,
      COUNT(obt.LineNo) LineCount
    FROM  gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
    GROUP BY ALL
)
,


gt AS (
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
  ,obt.RevenueAmount   AS RevenueAmount
  ,obt.CostAmount AS CostAmount
  ,obt.GP1  AS GP1
  ,CAST(0.00 AS DECIMAL(20, 4)) AS CostAmount_ValueEntry
  ,CAST(0.00 AS DECIMAL(20,4)) AS Cost_ProRata_Adj
FROM gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
),

ve AS (
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
  ,COALESCE(ve.PostingDate,obt.TransactionDate) AS TransactionDate
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
  ,0.00  AS RevenueAmount
  ,0.00  AS CostAmount
  ,0.00  AS GP1
  ,CAST(COALESCE(VE.CostPostedtoG_L,0) AS DECIMAL(20, 4)) AS CostAmount_ValueEntry
  ,CAST(0.00 AS DECIMAL(20,4)) AS Cost_ProRata_Adj
FROM gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
INNER JOIN value_entry_adjustments ve ON obt.DocumentNo = ve.DocumentNo_
                                     AND obt.LineNo = ve.DocumentLineNo_
                                     AND obt.Sys_DatabaseName = ve.Sys_DatabaseName
                                     AND CASE WHEN obt.EntityCode = 'AT1' THEN 'DE1'
                                              WHEN obt.EntityCode = 'BE1' THEN 'NL1'
                                              ELSE obt.EntityCode END = VE.EntityCode 
),

 ca AS (
  SELECT
    obt.GroupEntityCode,
    obt.EntityCode,
    obt.Sys_DatabaseName,
    obt.DocumentNo,
    obt.LineNo,
    obt.Gen_Bus_PostingGroup,
    obt.MSPUsageHeaderBizTalkGuid,
    obt.Type,
    obt.SalesInvoiceDescription,
    COALESCE(ca.GL_Doc_PostingDate, obt.TransactionDate) AS TransactionDate,
    obt.SalesOrderDate,
    obt.SalesOrderID,
    obt.SalesOrderItemID,
    obt.SKUInternal,
    obt.Gen_Prod_PostingGroup,
    obt.SKUMaster,
    obt.Description,
    obt.ProductTypeInternal,
    obt.ProductTypeMaster,
    obt.CommitmentDuration1Master,
    obt.CommitmentDuration2Master,
    obt.BillingFrequencyMaster,
    obt.ConsumptionModelMaster,
    obt.VendorCode,
    obt.VendorNameInternal,
    obt.VendorNameMaster,
    obt.VendorGeography,
    obt.VendorStartDate,
    obt.ResellerCode,
    obt.ResellerNameInternal,
    obt.ResellerGeographyInternal,
    obt.ResellerStartDate,
    obt.ResellerGroupCode,
    obt.ResellerGroupName,
    obt.ResellerGroupStartDate,
    obt.CurrencyCode,
    0.00 AS RevenueAmount,
    0.00 AS CostAmount,
    0.00 AS GP1,
    0.00 AS CostAmount_ValueEntry 
,CASE
      WHEN COALESCE(obt_sum.RevenueAmount_Sum, 0) = 0 THEN ca.CostAmount_Gap / obt_sum.LineCount --If total revenue = 0, divide by number of rcords introduced by obt.
      ELSE CAST(
        TRY_DIVIDE((obt.RevenueAmount), obt_sum.RevenueAmount_Sum) AS DECIMAL(10, 4)
      ) --Works out what ratio of total revenue this is,
      * ca.CostAmount_Gap --and then multiplys by the cost gap to split cost.
    END AS Cost_ProRata_Adj
  FROM
    gold_{ENVIRONMENT}.OBT.infinigate_globaltransactions obt
    LEFT JOIN (
      SELECT
        EntityCode,
        DocumentNo,
        GL_Doc_PostingDate,
        Sys_DatabaseName,
        VendorCode,
        sum(cost_adjustment.CostAmount_Gap) CostAmount_Gap
      FROM
        cost_adjustment
      GROUP BY ALL
    ) ca ON CASE
      WHEN obt.EntityCode = 'AT1' THEN 'DE1'
      WHEN obt.EntityCode = 'BE1' THEN 'NL1'
      ELSE obt.EntityCode
    END = ca.EntityCode
    AND obt.DocumentNo = ca.DocumentNo
    AND obt.Sys_DatabaseName = ca.Sys_DatabaseName
    AND obt.VendorCode = ca.VendorCode
    LEFT JOIN obt_sum ON obt.EntityCode = obt_sum.EntityCode
    AND obt.DocumentNo = obt_sum.DocumentNo
    AND obt.Sys_DatabaseName = obt_sum.Sys_DatabaseName
    AND obt.VendorCode = obt_sum.VendorCode
  WHERE
    ca.CostAmount_Gap <> 0
  UNION ALL
 SELECT
    'IG' AS GroupEntityCode,
    ca.EntityCode,
    ca.Sys_DatabaseName,
    ca.DocumentNo,
    '' AS LineNo,
    '' AS Gen_Bus_PostingGroup,
    '' AS MSPUsageHeaderBizTalkGuid,
    '' AS Type,
    '' AS SalesInvoiceDescription,
    ca.GL_Doc_PostingDate AS TransactionDate,
    '' AS SalesOrderDate,
    '' AS SalesOrderID,
    '' AS SalesOrderItemID,
    '' AS SKUInternal,
    '' AS Gen_Prod_PostingGroup,
    '' AS SKUMaster,
    '' AS Description,
    '' AS ProductTypeInternal,
    '' AS ProductTypeMaster,
    '' AS CommitmentDuration1Master,
    '' AS CommitmentDuration2Master,
    '' AS BillingFrequencyMaster,
    '' AS ConsumptionModelMaster,
    ca.VendorCode,
    '' AS VendorNameInternal,
    '' AS VendorNameMaster,
    '' AS VendorGeography,
    '' AS VendorStartDate,
    '' AS ResellerCode,
    '' AS ResellerNameInternal,
    '' AS ResellerGeographyInternal,
    '' AS ResellerStartDate,
    '' AS ResellerGroupCode,
    '' AS ResellerGroupName,
    '' AS ResellerGroupStartDate,
    '' AS CurrencyCode,
    0.00 AS RevenueAmount,
    0.00 AS CostAmount,
    0.00 AS GP1,
    0.00 AS CostAmount_ValueEntry,
    sum(ca.CostAmount_Gap) AS Cost_ProRata_Adj
  FROM
    cost_adjustment AS ca
WHERE CONCAT(ca.Sys_DatabaseName,ca.DocumentNo,ca.VendorCode) NOT IN
 (SELECT DISTINCT CONCAT(Sys_DatabaseName,DocumentNo,VendorCode) FROM gold_{ENVIRONMENT}.obt.infinigate_globaltransactions)

  GROUP BY
    ALL
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
  FROM (
    SELECT *
    FROM gt
    UNION ALL
    SELECT *
    FROM ve
    UNION ALL
    SELECT *
    FROM ca
) fc
GROUP BY ALL

""").createOrReplaceTempView('temp')

# COMMAND ----------

spark.sql(f"""
create or replace table infinigate_globaltransactions_cost_adjusted_gl as
with gl_doc_category_sum as (
  SELECT * 
  FROM (
  SELECT DISTINCT
     gle.DocumentNo_,
     gle.GL_Group,
     gle.Sys_DatabaseName,
     gle.EntityCode
  from gold_{ENVIRONMENT}.obt.infinigate_general_ledger gle
  WHERE  GL_Group IS NOT NULL
  ) GROUP BY ALL
  HAVING COUNT(DISTINCT GL_GROUP) = 1
)

SELECT temp.*,
gl_doc_category_sum.GL_Group
 from temp
left join gl_doc_category_sum on
temp.Sys_DatabaseName = gl_doc_category_sum.Sys_DatabaseName
    AND temp.DocumentNo = gl_doc_category_sum.DocumentNo_
""")
