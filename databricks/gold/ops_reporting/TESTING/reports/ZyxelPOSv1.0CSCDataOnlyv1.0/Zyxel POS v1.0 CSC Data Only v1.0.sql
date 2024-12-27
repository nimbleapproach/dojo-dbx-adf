SELECT
	ifg.CUSTOMERINVOICEDATE								AS [Invoice Date]
	,ifg.MANUFACTURERITEMNUMBER							AS [Part Number]
	,ncsc.SkuDescription								AS [Part Number Description]
	,ifg.SERIALNUMBER									AS [Serial Number]
	,ifg.RESELLERNAME									AS [Reseller Name]
	,ifg.RESELLERCOUNTRYCODE							AS [Reseller Country Code]
	,ifg.VATREGISTRATIONNO								AS [Reseller VAT Number]
	,CONCAT_WS(', ', ifg.RESELLERADDRESS1, ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3)		AS [Reseller Address]
	,ifg.RESELLERZIPCODE								AS [Reseller Zip Code] 
	,ifg.RESELLERADDRESSCITY							AS [Reseller City]
	,ifg.QUANTITY										AS [Quantity]
	,ncsc.VendorStandardCost							AS [Unit Price]
	,(ncsc.VendorStandardCost * ifg.QUANTITY)			AS [Total Price]
	,'€'												AS [Local Currency]
	,ncsc.[D365PurchasId]								AS [POR]
	,ncsc.[D365PurchPrice]								AS [POR Price]
	,ncsc.[D365ExpectedPurchasePrice]					AS [Final Buy Price]
	,ifg.SALESORDERNUMBER								AS [Nav SO]
	,ncsc.[D365Order]										AS [D365 SO]
	,ifg.VENDORCLAIMID									AS [SPQ]
	,ifg.VENDORRESELLERLEVEL							AS [Vendor Reseller Level]
	,ncsc.D365Order
FROM NavData ifg
	LEFT JOIN NuviasCSCData ncsc ON ncsc.[Navision So Number] = ifg.SALESORDERNUMBER AND ncsc.NavisionLineNumber = ifg.SALESORDERLINENO AND ncsc.[D365 Packing Slip Id] = ifg.[IGSShipmentNo]
WHERE ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to
	AND ifg.VATREGISTRATIONNO IS NOT NULL
GROUP BY 
	ifg.CUSTOMERINVOICEDATE
	,ifg.MANUFACTURERITEMNUMBER
	,ncsc.SkuDescription
	,ifg.SERIALNUMBER
	,ifg.RESELLERNAME
	,ifg.RESELLERCOUNTRYCODE
	,ifg.VATREGISTRATIONNO
	,ifg.RESELLERADDRESS1
	,ifg.RESELLERADDRESS2
	,ifg.RESELLERADDRESS3
	,ifg.RESELLERZIPCODE
	,ifg.RESELLERADDRESSCITY
	,ifg.QUANTITY
	,ncsc.VendorStandardCost
	,ncsc.VendorStandardCost  
	,ncsc.D365PurchasId
	,ncsc.[D365PurchPrice]
	,ncsc.[D365ExpectedPurchasePrice]
	,ncsc.[D365Quantity]
	,ifg.SALESORDERNUMBER
	,ncsc.D365Order
	,ifg.VENDORCLAIMID
	,ifg.VENDORRESELLERLEVEL
	,ncsc.D365Order