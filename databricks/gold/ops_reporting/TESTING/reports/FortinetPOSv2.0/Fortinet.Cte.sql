WITH cte AS (

SELECT 
	it.DATEPHYSICAL									AS Date
	,it.INVENTSERIALID								AS SerialNumber
	,CASE 
		WHEN di.ItemName = 'FORTICARE'
		THEN 'RENEWAL' 
		ELSE di.ItemName 
	END												AS PartNumber
	,ISNULL(pl.PURCHPRICE, sl.SAG_PURCHPRICE)		AS USDDistiUnitBuyPrice
	,sl.SAG_RESELLERVENDORID						AS FortinetPOSID
	,sh.CURRENCYCODE								AS ResaleCurrency
	,sl.SALESPRICE									AS UnitResalePrice
	,ca.CustomerName								AS ResellerName
	,ca.AddressCountryISO2							AS ResellerCountry
	,sh.SAG_EUADDRESS_NAME							AS EndUserCompanyName
	,sh.SAG_EUADDRESS_COUNTRY						AS EndUserCountry
	,SUBSTRING(sh.SAG_EUADDRESS_CONTACT,1,CHARINDEX(' ', sh.SAG_EUADDRESS_CONTACT)) AS EndUserFirstName -- split field on space 
	,SUBSTRING(sh.SAG_EUADDRESS_CONTACT,CHARINDEX(' ', sh.SAG_EUADDRESS_CONTACT)+1, LEN(sh.SAG_EUADDRESS_CONTACT)) AS EndUserLastName -- split field on space
	,sh.SAG_EUADDRESS_EMAIL							AS EndUserEmailAddress
	,''												AS EndUserPhoneNumber -- need to check where this is coming from? 
	,CASE 
		WHEN sp.PurchTableID_InterComp IS NULL 
		THEN 'Fulfilled from existing distributor stock' 
		ELSE 'Back to Back'
	END												AS OrderType
	,sh.SALESORDERTYPE								AS BusinessType
	,sp.PurchTableID_InterComp						AS PONumber
	,sh.SAG_EUADDRESS_STREET1						AS EndUserAddress
	,sh.SAG_EUADDRESS_POSTCODE						AS EndUserZipCode
	,sh.SAG_EUADDRESS_CITY							AS EndUserCity
	,CAST(-1*it.QTY AS decimal)						AS Qty 
FROM SAG_SalesLineV2Staging sl 
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID NOT in( 'NGS1','NNL2')
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT in( 'NGS1','NNL2')
	LEFT JOIN ara.SO_PO_ID_List sp ON sp.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID = right(sp.SalesTableID_InterComp,4)
	LEFT JOIN v_CustomerPrimaryPostalAddressSplit ca ON ca.CustomerAccountNumber = sh.CUSTACCOUNT 
	LEFT JOIN SAG_PurchLineStaging pl ON pl.INVENTTRANSID = sp.PurchLineID_InterComp AND pl.DATAAREAID in ('NGS1','NNL2')
WHERE 
--JG
--sl.DATAAREAID NOT LIKE 'NGS1'
sl.DATAAREAID NOT in( 'NGS1','NNL2')
	AND di.PrimaryVendorName LIKE 'Fortinet%'
	AND it.DATEPHYSICAL BETWEEN @from AND @to
	AND -1*it.Qty > 0 
	AND sh.SALESORDERTYPE NOT LIKE 'Demo' 

UNION ALL
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
	,CAST(Qty - 1 AS decimal)
FROM cte 
WHERE Qty > 1
)
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
	,CAST(1 AS decimal(31,4)) AS Qty
INTO #fortinet_result
FROM cte 
OPTION (MAXRECURSION 10000);
