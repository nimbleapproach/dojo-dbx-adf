with NGSInventory as 
(
SELECT 
	INVENTDIMID
	,INVENTTRANSID
FROM SAG_InventTransStaging
WHERE DATAAREAID in( 'NGS1','NNL2')
	AND (STATUSISSUE IN ('1', '3')
		OR (STATUSRECEIPT LIKE '1' AND INVOICERETURNED = 1)) 
GROUP BY INVENTDIMID
	,INVENTTRANSID
),
custadd as 
(
  select	
		CustomerAccountNumber
		,Street1							
	,Street2							
	,Street3							
	,AddressCity						
	,AddressState					
	,AddressPostalCode				
	,AddressCountryISO2				
	from v_CustomerPrimaryPostalAddressSplit
),
logadd as 
(
  select		
  AddressID
  ,CompanyName						
	,Street1							
	,Street2							
	,Street3							
	,City							
	,County							
	,PostalCode						
	,CountryISO2									
	from v_LogisticsPostalAddressSplit
),
DistinctItem_CTE AS 
(
SELECT
	ROW_NUMBER() OVER(PARTITION BY it.ITEMID, it.DATAAREAID ORDER BY it.ITEMID) rn
	,it.DATAAREAID				AS CompanyID
	,it.ITEMID					AS ItemID
	,it.NAME					AS ItemName
	,it.DESCRIPTION				AS ItemDescription
	,it.MODELGROUPID			AS ItemModelGroupID
	,it.ITEMGROUPID				AS ItemGroupID
	,it.PRACTICE				AS Practice
	,it.PRIMARYVENDORID			AS PrimaryVendorID
	,ve.VENDORORGANIZATIONNAME	AS PrimaryVendorName
	,ig.NAME					AS ItemGroupName
	,mg.NAME					AS ItemModelGroupName
	,fd.DESCRIPTION				AS PracticeDescr
FROM SAG_InventTableStaging it WITH (NOLOCK)
	LEFT JOIN VendVendorV2Staging ve WITH (NOLOCK) ON (ve.VENDORACCOUNTNUMBER = it.PRIMARYVENDORID AND ve.DATAAREAID = it.DATAAREAID)
	LEFT JOIN SAG_InventItemGroupStaging ig WITH (NOLOCK) ON (ig.ITEMGROUPID = it.ITEMGROUPID AND ig.DATAAREAID = it.DATAAREAID)
	LEFT JOIN SAG_InventModelGroupStaging mg WITH (NOLOCK) ON (mg.MODELGROUPID = it.MODELGROUPID AND mg.DATAAREAID = it.DATAAREAID)
	LEFT JOIN FinancialDimensionValueEntityStaging fd WITH (NOLOCK) ON (fd.DIMENSIONVALUE = it.PRACTICE AND fd.FINANCIALDIMENSION = 'Practice')
WHERE LEFT(PRIMARYVENDORID,3) = 'VAC'
),
v_DistinctItems_cte as
(
SELECT * FROM DistinctItem_CTE 
WHERE rn = 1
),
distitem as 
(
  select ItemID
	,ItemName
	,ItemGroupName
	,PrimaryVendorName
	,PrimaryVendorID
	,CompanyID
	from v_DistinctItems_cte
)