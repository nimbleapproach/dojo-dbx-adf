SELECT 
	ifg.CUSTOMERINVOICEDATE					AS FinancialDate
	,ncsc.NuviasVendorSku					AS ProductName
	,ncsc.NuviasSerialNumber				AS SerialNumber
	,ifg.QUANTITY							AS Quantity
	,''										AS CustomerAccount	
	,ifg.RESELLERNAME						AS CustomerName   	 
	,ifg.UNITSELLCURRENCY					AS SalesOrderCurrency
	,ifg.UNITSELLPRICE						AS SalesInvoiceUnitPrice
	,''										AS ExchangeRate
	,''										AS ProductUnitPriceUSD 
	,ncsc.NuviasSalesId						AS NuviasSalesId
	,ncsc.VendorReferenceNumber				AS VendorReferenceNumber
FROM ifg.POSData ifg 
	LEFT JOIN NuviasCSCData ncsc ON ncsc.NuviasSoLink = ifg.SALESORDERNUMBER AND ncsc.NuviasNavLineNum = ifg.SALESORDERLINENO AND ncsc.DelveryNoteId = ifg.IGSSHIPMENTNO 
WHERE ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to
	--AND ifg.VENDORRESELLERLEVEL LIKE '%Smart Optics%' -- Removed due to AuoTask ticket [#T20240301.0048] 01/03/2024
	AND ncsc.NuviasVendorSku IS NOT NULL -- added 27/02/24 to stop incorrect sku's and data being presented from the link bettween the Nuvias data and the Infigate data -- Removed due to AuoTask ticket [#T20240301.0048] 01/03/2024
GROUP BY
	ifg.CUSTOMERINVOICEDATE
	,ncsc.NuviasVendorSku
	,ncsc.NuviasSerialNumber
	,ifg.QUANTITY
	,ifg.RESELLERNAME
	,ifg.UNITSELLCURRENCY
	,ifg.UNITSELLPRICE
	,ncsc.NuviasSalesId
	,ncsc.DelveryNoteId
	,ncsc.VendorReferenceNumber
ORDER BY ifg.CUSTOMERINVOICEDATE asc