SELECT 
	'Nuvias Global Services'													AS AccountName
	,ncsc.NuviasPurchaseOrder													AS DistributorPurchaseOrderNumber
	,''																			AS SalesOrderDate
	,ncsc.NuviasSalesId															AS SalesOrderNumber
	,CAST(ifg.CUSTOMERINVOICEDATE as date)										AS InvoiceDate
	,ifg.INVOICENUMBER															AS InvoiceNumber
	,ncsc.NuviasResellerVendorId												AS SandDRef
	,ifg.RESELLERNAME															AS CustomerName
	,ifg.RESELLERADDRESS1 + ifg.RESELLERADDRESS2 + ifg.RESELLERADDRESSCITY		AS CustomerAddress 
	,ifg.RESELLERZIPCODE														AS CustomerPostalCode
	,ifg.RESELLERCOUNTRYCODE													AS CustomerCountry
	,''																			AS Blank1
	,''																			AS Blank2
	,ifg.ENDUSERNAME															AS EndUserName
	,ifg.ENDUSERADDRESS1 + ' ' + ifg.ENDUSERADDRESS2  + ifg.ENDUSERADDRESS3  	AS EndUserAddress
	,ifg.ENDUSERZIPCODE															AS EndUserPostalCode
	,ifg.ENDUSERCOUNTRYCODE														AS EndUserCountry
	,ifg.MANUFACTURERITEMNUMBER													AS PartCode
	,''																			AS ProductDescription
	,ncsc.NuviasSerialNumber													AS ProductSerial
	,ifg.QUANTITY																AS Quantity
	,ncsc.NuviasPurchPrice  * ex.RATE											AS ProductCostEUR
	,''																			AS Blank3
	,''																			AS NokiaRef
	,''																			AS Blank4 
	,ncsc.NuviasSalesId															AS IntercompanySalesOrder

	,ifg.SALESORDERNUMBER														AS NavisionSalesOrderNumber
	,ncsc.VendorReferenceNumber													AS VendorReferenceNumber
FROM ifg.POSData ifg
	LEFT JOIN NuviasCSCData ncsc ON ncsc.NuviasSoLink = ifg.SALESORDERNUMBER AND ncsc.NuviasNavLineNum = ifg.SALESORDERLINENO AND ncsc.DelveryNoteId = ifg.IGSSHIPMENTNO
	LEFT JOIN ExchangeRates ex ON ex.StartDate = CUSTOMERINVOICEDATE
WHERE ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to 
	AND ifg.VENDORRESELLERLEVEL LIKE 'Nokia%'