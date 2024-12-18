With custadd as 
(SELECT	
	CustomerAccountNumber
	,CustomerName
	,Street1							
	,Street2							
	,Street3							
	,AddressCity						
	,AddressState					
	,AddressPostalCode				
	,AddressCountryISO2				
FROM v_CustomerPrimaryPostalAddressSplit
)