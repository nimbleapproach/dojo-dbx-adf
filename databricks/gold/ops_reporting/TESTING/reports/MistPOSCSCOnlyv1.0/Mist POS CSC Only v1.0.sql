SELECT  
	CASE WHEN ncsc.[D365 Warehouse] = 'MAIN2' AND ncsc.[D365 Entity] = 'NNL2'
		THEN '101421538' 
		ELSE '' END												AS DistributerIDNumber 
	,CASE WHEN 1 * ncsc.D365Quantity > 0
		THEN 'POS' 
		WHEN -1 * ncsc.D365Quantity <0 
		THEN 'RD' 
		ELSE '' 
	END															AS DistributerTransactionType
	,CAST(UPPER(ncsc.SerialNumber) AS VARCHAR)					AS ProductSerialNumber
	,ncsc.D365Sku												AS ProductJuniperPartNumber
	,ncsc.D365Quantity											AS ProductQuantity 
	,ncsc.VendorReferenceNumber									AS SpecialPricingAuthorization	
	,ncsc.CostInQuoteCurrency 									AS [NetPOS(ProductUnitPrice)]
	,''															AS ExportLicenceNumber --Not Required
	,ncsc.D365PurchasId											AS DistributorPurchaseOrder  --PO to Mist
	,ncsc.D365Order												AS ResaleSalesOrderNumber 
	,ifg.INVOICENUMBER											AS ResaleInvoiceNumber 
	,ifg.CUSTOMERINVOICEDATE									AS ResaleInvoiceDate
	,ifg.RESELLERPONUMBER										AS ResellerPONumber
	,ncsc.[D365 Var Id]											AS JuniperVARID1
	,''															AS BusinessModel1
	,ifg.ResellerName											AS ResellerVARName
	,ifg.ResellerAddress1										AS ResellerVARAddress1
	,ifg.ResellerAddress2										AS ResellerVARAddress2 
	,ifg.ResellerAddress3										AS ResellerVARAddress3
	,ifg.ResellerAddressCity									AS ResellerVARACity
	,ifg.ResellerState											AS ResellerVARStateProvince
	,ifg.ResellerZipCode										AS ResellerVARPostalCode
	,ifg.ResellerCountryCode									AS ResellerVARCountryCode 
	,''															AS JuniperVARID2 
	,''															AS BusinessModel2 
	,ifg.SHIPTONAME												AS ShipToName
	,ifg.SHIPTOADDRESS1											AS ShipToAddress1
	,ifg.SHIPTOADDRESS2												AS ShipToAddress2
	,ifg.SHIPTOADDRESS3												AS ShipToAddress3
	,ifg.SHIPTOCITY													AS ShipToCity
	,ifg.SHIPTOSTATE												AS ShipToStateProvince
	,ifg.SHIPTOZIPCODE												AS ShipToPostalCode
	,ifg.SHIPTOCOUNTRYCODE											AS ShipToCountryCode 
	,ifg.ENDUSERNAME												AS EndUserName
	,ifg.ENDUSERADDRESS1											AS EndUserAddress1 
	,ifg.ENDUSERADDRESS2											AS EndUserAddress2
	,ifg.ENDUSERADDRESS3											AS EndUserAddress3 
	,ifg.ENDUSERADDRESSCITY											AS EndUserCity
	,ifg.ENDUSERSTATE												AS EndUserStateProvince
	,ifg.ENDUSERZIPCODE												AS EndUserPostalCode
	,ifg.ENDUSERCOUNTRYCODE											AS EndUserCountryCode
	,''																AS DistributorIDNo2 
	,ifg.SALESORDERNUMBER											AS NavisionSaleOrderNumber
	,ncsc.[D365 Warehouse]											AS Warehouse
	,ncsc.[D365 Item Group]											AS ItemGroup
	,CASE WHEN ncsc.[Ship And Debit Flag] = 0 THEN 'No' 
			WHEN ncsc.[Ship And Debit Flag] = 1 THEN 'Yes'		END	AS ShipAndDebit
FROM ifg.POSData ifg
	LEFT OUTER JOIN NuviasCSCData ncsc ON ncsc.NavSoLink = ifg.SALESORDERNUMBER AND ncsc.NavisionLineNumber = ifg.SALESORDERLINENO AND ncsc.[D365 Packing Slip Id] = ifg.IGSSHIPMENTNO
WHERE 
	ncsc.SerialNumber IS NOT NULL 
	AND ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to
GROUP BY 
	ncsc.[D365 Entity]
	,ncsc.[D365 Warehouse]
	,ncsc.D365Quantity
	,ncsc.SerialNumber
	,ncsc.D365Sku
	,ncsc.D365Quantity
	,ncsc.CostInQuoteCurrency
	,ncsc.[D365 Var Id]
	,ncsc.D365PurchPrice
	,ncsc.D365PurchasId
	,ncsc.D365Order
	,ifg.INVOICENUMBER
	,ifg.CUSTOMERINVOICEDATE
	,ifg.RESELLERPONUMBER	
	,ncsc.VendorReferenceNumber
	,ifg.ResellerName
	,ifg.ResellerAddress1
	,ifg.ResellerAddress2
	,ifg.ResellerAddress3
	,ifg.ResellerAddressCity
	,ifg.ResellerState
	,ifg.ResellerZipCode
	,ifg.ResellerCountryCode
	,ifg.SHIPTONAME
	,ifg.SHIPTOADDRESS1
	,ifg.SHIPTOADDRESS2
	,ifg.SHIPTOADDRESS3	
	,ifg.SHIPTOCITY
	,ifg.SHIPTOSTATE
	,ifg.SHIPTOZIPCODE
	,ifg.SHIPTOCOUNTRYCODE
	,ifg.ENDUSERNAME
	,ifg.ENDUSERADDRESS1
	,ifg.ENDUSERADDRESS2	
	,ifg.ENDUSERADDRESS3
	,ifg.ENDUSERADDRESSCITY
	,ifg.ENDUSERSTATE
	,ifg.ENDUSERZIPCODE
	,ifg.ENDUSERCOUNTRYCODE
	,ifg.SALESORDERNUMBER
	,ncsc.[D365 Warehouse]
	,ncsc.[D365 Item Group]
	,ncsc.[Ship And Debit Flag]
	,ifg.QUANTITY	