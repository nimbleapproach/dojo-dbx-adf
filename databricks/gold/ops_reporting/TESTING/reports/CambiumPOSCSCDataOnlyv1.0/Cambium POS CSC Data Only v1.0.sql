SELECT 
	'Infinigate Global Services Ltd'											AS ReportingPartnerName -- Hardcoded
	,ifg.INVOICENUMBER														AS ReportingCompanyInvoiceNumber
	,ifg.CUSTOMERINVOICEDATE												AS ReportingCompanyBillDate 
	,ifg.RESELLERNAME														AS BillToName
	--,ifg.RESELLERADDRESS1 + ' ' + ifg.RESELLERADDRESS2 + ' ' + ifg.RESELLERADDRESS3   				AS BillToAddress 
	,CONCAT_WS(' ', ifg.RESELLERADDRESS1,ifg.RESELLERADDRESS2, ifg.RESELLERADDRESS3) AS BillToAddress
	,ifg.RESELLERADDRESSCITY												AS BillToCity
	,ifg.RESELLERSTATE														AS BillToState 
	,ifg.RESELLERCOUNTRYCODE												AS BillToCountry
	,ifg.RESELLERZIPCODE													AS BillToPostalCode
	,ifg.ENDUSERNAME														AS EndUserName								
	--,ifg.ENDUSERADDRESS1 + ' ' + ifg.ENDUSERADDRESS2 + ' ' + ifg.ENDUSERADDRESS3		AS EndUserAddress
	,CONCAT_WS(' ',ifg.ENDUSERADDRESS1, ifg.ENDUSERADDRESS2, ifg.ENDUSERADDRESS3) AS EndUserAddress 
	,ifg.ENDUSERADDRESSCITY													AS EndUserCity
	,ifg.ENDUSERSTATE														AS EndUserState 
	,ifg.ENDUSERCOUNTRYCODE													AS EndUserCountry
	,ifg.MANUFACTURERITEMNUMBER												AS CambiumPartNumber
	,ifg.UNITSELLCURRENCY													AS InvoiceCurrency
	,''																		AS ExchangeRate
	,''																		AS UnitSalesPriceUSD
	,ifg.UNITSELLPRICE														AS UnitSalesPrice
	,SUM(ifg.QUANTITY)														AS QuantitySold
	,ncsc.VendorReferenceNumber												AS ClaimID
	,(ncsc.VendorStandardCost - ncsc.NuviasPurchPrice) * ifg.QUANTITY			AS ClaimAmount
	,ncsc.NuviasSalesId														AS SalesOrder
	,ncsc.NuviasPurchaseOrder												AS PurchaseOrder -- MW-Added as per ticket 18781 (20/08/2020)
	,ncsc.NuviasPurchPrice													AS PurchasePrice -- MW-Added as per ticket 18781 (20/08/2020)
	,ifg.SALESORDERNUMBER													AS NavisionSalesOrder
FROM ifg.POSData ifg
	LEFT JOIN NuviasCSCData ncsc ON ncsc.NuviasSoLink = ifg.SALESORDERNUMBER AND ncsc.NuviasNavLineNum = ifg.SALESORDERLINENO AND ncsc.DelveryNoteId = ifg.IGSSHIPMENTNO
WHERE 
	ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to
	--AND ifg.VENDORRESELLERLEVEL LIKE '%Cambiumn%'
	and ncsc.PrimaryVendorId IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')

GROUP BY 
	ifg.INVOICENUMBER	
	,ifg.CUSTOMERINVOICEDATE
	,ifg.RESELLERNAME
	,ifg.RESELLERADDRESS1 
	,ifg.RESELLERADDRESS2 
	,ifg.RESELLERADDRESS3
	,ifg.RESELLERADDRESSCITY
	,ifg.RESELLERSTATE
	,ifg.RESELLERCOUNTRYCODE
	,ifg.RESELLERZIPCODE
	,ifg.ENDUSERNAME							
	,ifg.ENDUSERADDRESS1 
	,ifg.ENDUSERADDRESS2
	,ifg.ENDUSERADDRESS3
	,ifg.ENDUSERADDRESSCITY
	,ifg.ENDUSERSTATE
	,ifg.ENDUSERCOUNTRYCODE		
	,ifg.MANUFACTURERITEMNUMBER	
	,ifg.UNITSELLCURRENCY	
	,ifg.UNITSELLPRICE
	,ncsc.VendorReferenceNumber
	,ncsc.NuviasSalesId	
	,ncsc.NuviasPurchaseOrder
	,ncsc.NuviasPurchPrice
	,ifg.SALESORDERNUMBER
	,ncsc.VendorStandardCost 
	,ncsc.NuviasPurchPrice
	,ifg.QUANTITY