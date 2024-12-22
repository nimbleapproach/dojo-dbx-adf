with distitem as 
(
select ItemID
	,ItemName
	,ItemGroupName
	,PrimaryVendorName
	,PrimaryVendorID
	,CompanyID
	from v_DistinctItems
),
NGSInventory as 
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
(select	
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
)