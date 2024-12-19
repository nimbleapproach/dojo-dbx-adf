WITH NGSInventory AS 
(
  SELECT 
    INVENTDIMID
    ,INVENTTRANSID
  FROM SAG_InventTransStaging
  --JG
  --WHERE DATAAREAID = 'NGS1'
  WHERE DATAAREAID in( 'NGS1','NNL2')
    AND (STATUSISSUE IN ('1', '3')
      OR (STATUSRECEIPT LIKE '1' AND INVOICERETURNED = 1)) -- added to capture returns 
  GROUP BY INVENTDIMID
    ,INVENTTRANSID
),
custadd AS 
(
  SELECT	
    CustomerAccountNumber
    ,Street1							
    ,Street2							
    ,Street3							
    ,AddressCity						
    ,AddressState					
    ,AddressPostalCode				
    ,AddressCountryISO2				
  FROM v_CustomerPrimaryPostalAddressSplit
),
logadd AS
(
	SELECT		
	AddressID
	,CompanyName						
	,Street1							
	,Street2							
	,Street3							
	,City							
	,County							
	,PostalCode						
	,CountryISO2									
	FROM v_LogisticsPostalAddressSplit
)