SELECT
	CAST(it.DATEPHYSICAL as date)	AS [Ship Date]
	,di.ItemName					AS [Product #] 
	,(it.QTY * -1)					AS [Quantity] 
	,it.INVENTSERIALID				AS [S/N]
	,li.PurchTableID_InterComp		AS [PO Number]
	,pl.PURCHPRICE / pl.PURCHQTY	AS [Purchase Price (USD)]
	,pl.PURCHPRICE					AS [Purchase Extended Price] 
	,sh.SALESNAME					AS [Bill To Customer Name]
	,ca.AddressCountryISO2			AS [Bill To Country]
	,pa.CompanyName					AS [Ship To Customer Name]
	,pa.Street1						AS [Ship To Address -1]
	,pa.Street2						AS [Ship To Address -2]
	,pa.County						AS [Ship To County/Province]
	,pa.City						AS [Ship To City]
	,''								AS [Ship To State]
	,pa.CountryISO2					AS [Ship To Country]
	,pa.PostalCode					AS [Ship To Zip Code]
	,sh.SAG_EUADDRESS_NAME			AS [End User Name Details]
	,sh.SAG_EUADDRESS_STREET1		AS [End User Address -1]
	,sh.SAG_EUADDRESS_STREET2		AS [End User Address -2]
	,sh.SAG_EUADDRESS_COUNTY		AS [End User County/Province]
	,sh.SAG_EUADDRESS_CITY			AS [End User City]
	,''								AS [End User State]
	,sh.SAG_EUADDRESS_COUNTRY		AS [End User Country]
	,sh.SAG_EUADDRESS_POSTCODE		AS [End User Postal Code]
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID = sl.DATAAREAID
	LEFT JOIN ara.SO_PO_ID_List li ON li.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID AND di.CompanyID = RIGHT(li.SalesTableID_InterComp, 4)
	LEFT JOIN v_LogisticsPostalAddressSplit pa ON pa.AddressID = sh.DELIVERYPOSTALADDRESS --for shipping address
	LEFT JOIN v_CustomerPrimaryPostalAddressSplit ca ON ca.CustomerAccountNumber = sh.CUSTACCOUNT AND ca.Entity = sh.DATAAREAID
	LEFT JOIN SAG_PurchLineStaging pl ON pl.PURCHID = li.PurchTableID_InterComp AND pl.ITEMID = sl.ITEMID
WHERE sl.DATAAREAID NOT IN ('NGS1', 'NNL2')
	AND it.DATEPHYSICAL BETWEEN @from AND @to
	AND it.STATUSISSUE = '1'
	AND ((di.PrimaryVendorID LIKE 'VAC001208_NGS1')
		OR (di.PrimaryVendorID LIKE 'VAC001208_NNL2'))
	AND sl.SALESSTATUS LIKE '3'