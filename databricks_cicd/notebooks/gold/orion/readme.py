# Databricks notebook source
# MAGIC %md
# MAGIC # Notebooks to process the Sales ERP data for IG Cloud aka Vuzion
# MAGIC # **into the new F&D Model destination ORION**

# COMMAND ----------

# MAGIC %md
# MAGIC **F&D Model**
# MAGIC - Entity & Group
# MAGIC - Resellers & Group
# MAGIC - Vendors & Group
# MAGIC - Dates - Transaction Date, Sales Order Date, 
# MAGIC - Revenue Type - Deferred, One-off
# MAGIC - Currency
# MAGIC - Sales Orders
# MAGIC - Sales Order Item
# MAGIC - Product & Product Type & GP1% 
# MAGIC - Stock Unit (SKU) & SKU Master
# MAGIC - Resource ID ??
# MAGIC - MPN ??
# MAGIC - Comittments
# MAGIC - Billing Frequency Master
# MAGIC - ConsumptionModel
# MAGIC
# MAGIC **Measures**
# MAGIC - Revenue, Cost, Margin, ComittmentValue(?)
# MAGIC - SalesOrderItemID
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimensions Data Flow #
# MAGIC - Create dataframe of distinct names or business keys from source table
# MAGIC - Run a query to identify exact matches using mapping table
# MAGIC - Create dataframe of non-matching items
# MAGIC - Create dataframe of shared dimension ID and name/buskey
# MAGIC - Pass the non-matching and shared dimension dataframes into the matching process
# MAGIC - **Update** the shared dimension table with new members which fall below the lower matching threshold value
# MAGIC - **Update** the mapping table with matches from the matching process where not exists and the newly created shared dimension members
# MAGIC - Write out exceptions i.e. items that are not new, don't exist in mapping and cannot be matched - these will be linked with the Unknown Dimension member 
# MAGIC - Repeat for all dimensions
# MAGIC - (A) Should now have a distinct list of dimension members for each source dimension in a dataframe per dimension
# MAGIC - **Update** the shared dimension tables with the new members
# MAGIC # Facts Data Flow #
# MAGIC - Create a dataframe linking all the measures and all the shared dimension dataframes in (A)
# MAGIC - Write out the Facts table with the dimension ID's for all relevant dims
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Every Dimension will have these columns
# MAGIC
# MAGIC - start_datetime TIMESTAMP
# MAGIC - end_datetime TIMESTAMP
# MAGIC - is_current INT
# MAGIC - Sys_Gold_InsertedDateTime_UTC TIMESTAMP
# MAGIC - Sys_Gold_ModifiedDateTime_UTCTIMESTAMP
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Here is the final SQL that populates globaltransactions from igsql03 data
# MAGIC
# MAGIC cte.**GroupEntityCode**,
# MAGIC
# MAGIC case when cte.Reseller_Country_RegionCode = 'BE' AND cte.EntityCode = 'NL1' THEN 'BE1'
# MAGIC WHEN CTE.Sys_DatabaseName in('ReportsAT','ReportsDE') AND cte.VendorCode ='ZZ_INF' THEN 'DE1' 
# MAGIC when cte.Reseller_Country_RegionCode = 'AT' AND cte.VendorCode NOT IN ('ITG', 'RFT') AND cte.EntityCode = 'DE1' THEN 'AT1' 
# MAGIC ELSE cte.EntityCode END AS **EntityCode**,
# MAGIC
# MAGIC CTE.Sys_DatabaseName,
# MAGIC
# MAGIC cte.DocumentNo,
# MAGIC
# MAGIC cte.LineNo,
# MAGIC
# MAGIC cte.Gen_Bus_PostingGroup,
# MAGIC
# MAGIC cte.MSPUsageHeaderBizTalkGuid,
# MAGIC
# MAGIC cte.Type,
# MAGIC
# MAGIC cte.SalesInvoiceDescription,
# MAGIC
# MAGIC cte.TransactionDate,
# MAGIC
# MAGIC cte.SalesOrderDate,
# MAGIC
# MAGIC cte.SalesOrderID,
# MAGIC
# MAGIC cte.SalesOrderItemID,
# MAGIC
# MAGIC case when msp_usage.ItemNo_ is null then cte.SKUInternal
# MAGIC else msp_usage.ItemNo_ end as **SKUInternal**,
# MAGIC
# MAGIC Gen_Prod_PostingGroup,
# MAGIC
# MAGIC coalesce(datanowarr.SKU, 'NaN') AS SKUMaster,
# MAGIC
# MAGIC cte.Description,
# MAGIC
# MAGIC cte.ProductTypeInternal,
# MAGIC
# MAGIC coalesce(datanowarr.Product_Type, 'NaN') AS ProductTypeMaster,
# MAGIC
# MAGIC coalesce(datanowarr.Commitment_Duration_in_months, 'NaN') AS CommitmentDuration1Master,
# MAGIC
# MAGIC coalesce(datanowarr.Commitment_Duration_Value, 'NaN') AS CommitmentDuration2Master,
# MAGIC
# MAGIC coalesce(datanowarr.Billing_Frequency, 'NaN') AS BillingFrequencyMaster,
# MAGIC
# MAGIC coalesce(datanowarr.Consumption_Model, 'NaN') AS ConsumptionModelMaster,
# MAGIC
# MAGIC cte.VendorCode,
# MAGIC
# MAGIC cte.VendorNameInternal,
# MAGIC
# MAGIC coalesce(datanowarr.Vendor_Name, 'NaN') AS VendorNameMaster,
# MAGIC
# MAGIC cte.EntityCode as VendorGeography,
# MAGIC
# MAGIC case when VendorStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, cte.VendorCode)
# MAGIC else VendorStartDate end as **VendorStartDate**,
# MAGIC
# MAGIC ResellerCode,
# MAGIC
# MAGIC ResellerNameInternal,
# MAGIC
# MAGIC ResellerGeographyInternal,
# MAGIC
# MAGIC case
# MAGIC   when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, ResellerCode)
# MAGIC   else ResellerStartDate
# MAGIC end as **ResellerStartDate**,
# MAGIC
# MAGIC ResellerGroupCode,
# MAGIC
# MAGIC ResellerGroupName,
# MAGIC
# MAGIC case
# MAGIC   when (
# MAGIC     ResellerGroupName = 'No Group'
# MAGIC     or ResellerGroupName is null
# MAGIC   ) then (
# MAGIC     case
# MAGIC       when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, ResellerCode)
# MAGIC       else ResellerStartDate
# MAGIC     end
# MAGIC   )
# MAGIC   else (
# MAGIC     case
# MAGIC       when ResellerStartDate <= '1900-01-01' then min(TransactionDate) OVER(PARTITION BY cte.EntityCode, ResellerGroupName)
# MAGIC       else min(ResellerStartDate) OVER(PARTITION BY cte.EntityCode, ResellerGroupName)
# MAGIC     end
# MAGIC   )
# MAGIC end AS **ResellerGroupStartDate**,
# MAGIC
# MAGIC cte.CurrencyCode,
# MAGIC
# MAGIC cast (
# MAGIC   case WHEN msp_usage.TotalPrice is null THEN RevenueAmount
# MAGIC         when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor > 0 then msp_usage.TotalPrice / cte.CurrencyFactor
# MAGIC         when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor = 0  then msp_usage.TotalPrice
# MAGIC     else RevenueAmount
# MAGIC   end as decimal(10, 2)
# MAGIC ) as **RevenueAmount**,
# MAGIC
# MAGIC cast(case
# MAGIC   when msp_usage.BizTalkGuid is null then CostAmount
# MAGIC   else msp_usage.TotalCostLCY
# MAGIC end as decimal(10, 2)) as **CostAmount**,
# MAGIC
# MAGIC cast((case WHEN msp_usage.TotalPrice is null THEN RevenueAmount
# MAGIC         when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor > 0 then msp_usage.TotalPrice / cte.CurrencyFactor
# MAGIC         when  msp_usage.TotalPrice is NOT null AND cte.CurrencyFactor = 0  then msp_usage.TotalPrice
# MAGIC     else RevenueAmount
# MAGIC   end +
# MAGIC case
# MAGIC   when msp_usage.BizTalkGuid is null then CostAmount
# MAGIC   else msp_usage.TotalCostLCY
# MAGIC end) as decimal(10, 2)) as **GP1**

# COMMAND ----------


