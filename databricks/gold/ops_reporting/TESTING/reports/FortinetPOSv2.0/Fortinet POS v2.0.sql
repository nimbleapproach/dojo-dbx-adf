SELECT 
	Date
	,SerialNumber
	,PartNumber
	,USDDistiUnitBuyPrice
	,FortinetPOSID
	,ResaleCurrency
	,UnitResalePrice
	,ResellerName
	,ResellerCountry
	,EndUserCompanyName
	,EndUserCountry
	,EndUserFirstName
	,EndUserLastName
	,EndUserEmailAddress
	,EndUserPhoneNumber
	,OrderType
	,BusinessType
	,PONumber
	,EndUserAddress
	,EndUserZipCode
	,EndUserCity
	,1 AS Qty
	--,Entity
FROM cte 
ORDER BY Date
OPTION (MAXRECURSION 10000)