with NuviasCSCData as 
(
SELECT 
	nu.SALESID							AS NuviasSalesId
	,nu.SALESQTY						AS NuivasSalesQty
	,nu.ItemName						AS NuviasVendorSku
	,nu.SAG_RESELLERVENDORID			AS NuviasResellerVendorId
	,nu.INVENTTRANSID					AS NuviasInventTransId
	,nu.SAG_NAVSONUMBER					AS NuviasSoLink
	,nu.SAG_NAVLINENUM					AS NuviasNavLineNum
	,nu.DATEPHYSICAL					AS NuviasShimentDate
	,nu.INVENTSERIALID					AS NuviasSerialNumber
	,nu.SAG_PURCHPRICE					AS NuviasPurchPrice
	,nu.PURCHID							AS NuviasPurchaseOrder
	,nu.DATAAREAID						AS NuviasEntity
	,nu.INVENTLOCATIONID				AS NuviasWarehouse
	,nu.SAG_NAVLINENUM					AS [D365-NavisionLineNumber]
	,nu.PACKINGSLIPID					AS DelveryNoteId
	,nu.SAG_VENDORSTANDARDCOST			AS NuviasVendorStanadardCost
	,nu.PrimaryVendorName				AS NuviasVendorName	
	,nu.SAG_VENDORREFERENCENUMBER		AS VendorReferenceNumber
FROM v_NCSC_NuviasData nu
WHERE (nu.PrimaryVendorName LIKE 'Nokia%') 
),
ExchangeRates as
(
SELECT 
	CONVERT(DATE, STARTDATE) AS StartDate
	,TOCURRENCY
	,FROMCURRENCY
	,RATE
FROM ExchangeRateEntityStaging 
	WHERE CONVERT(DATE, STARTDATE) BETWEEN @from AND @to 
	AND (FROMCURRENCY = 'GBP' AND TOCURRENCY = 'EUR')
)