SELECT 
	CASE WHEN ncsc.NuviasEntity = 'NNL2' AND ncsc.NuviasWarehouse LIKE '%2' 
			THEN '101421538' 
		WHEN ncsc.NuviasEntity = 'NGS1' AND ncsc.NuviasWarehouse LIKE '%5' OR ncsc.NuviasEntity = 'NGS1'  AND ncsc.NuviasWarehouse = 'CORR'
			THEN '101417609'
	END														AS DistributerIDNumber
	,CASE WHEN ifg.ORDERTYPE1 = 'Credit Memo'
			THEN 'RD'
			ELSE 'POS' END									AS DistributerTransactionType
	,UPPER(ifg.SERIALNUMBER)								AS ProductSerialNumber
	,ifg.MANUFACTURERITEMNUMBER								AS ProductJuniperPartNumber
	,ABS(ifg.QUANTITY)										AS ProductQuantity
	,ifg.VENDORADDITIONALDISCOUNT1							AS SpecialPricingAuthorization
	,ncsc.NuviasPurchPrice									AS [NetPOS(ProductUnitPrice)]
	,CAST(ifg.VENDORBUYPRICE as numeric(18,2))				AS InfingateBuy
	,''														AS ExportLicenceNumber 
	,ncsc.NuviasPurchaseOrder								AS DistributorPurchaseOrder  
	,ncsc.NuviasSalesId										AS ResaleSalesOrderNumber
	,ifg.INVOICENUMBER										AS ResaleInvoiceNumber 
	,FORMAT(ifg.CUSTOMERINVOICEDATE, 'dd/MM/yyyy')			AS ResaleInvoiceDate 
	,ifg.RESELLERPONUMBER									AS ResellerPONumber
	,ncsc.VarId												AS JuniperVARID1
	,''														AS BusinessModel1 
	,ifg.ResellerName										AS ResellerVARName
	,ifg.ResellerAddress1									AS ResellerVARAddress1
	,ifg.ResellerAddress2									AS ResellerVARAddress2 
	,ifg.ResellerAddress3									AS ResellerVARAddress3
	,ifg.ResellerAddressCity								AS ResellerVARACity
	,ifg.ResellerState										AS ResellerVARStateProvince
	,ifg.ResellerZipCode									AS ResellerVARPostalCode
	,ifg.ResellerCountryCode								AS ResellerVARCountryCode
	,''														AS JuniperVARID2 
	,''														AS BusinessModel2 
	,ifg.SHIPTONAME											AS ShipToName
	,ifg.SHIPTOADDRESS1										AS ShipToAddress1
	,ifg.SHIPTOADDRESS2										AS ShipToAddress2
	,ifg.SHIPTOADDRESS3										AS ShipToAddress3
	,ifg.SHIPTOCITY											AS ShipToCity
	,ifg.SHIPTOSTATE										AS ShipToStateProvince
	,ifg.SHIPTOZIPCODE										AS ShipToPostalCode
	,ifg.SHIPTOCOUNTRYCODE									AS ShipToCountryCode
	,ifg.ENDUSERNAME										AS EndUserName
	,ifg.ENDUSERADDRESS1									AS EndUserAddress1
	,ifg.ENDUSERADDRESS2									AS EndUserAddress2
	,ifg.ENDUSERADDRESS3									AS EndUserAddress3
	,ifg.ENDUSERADDRESSCITY									AS EndUserCity
	,ifg.ENDUSERSTATE										AS EndUserStateProvince
	,ifg.ENDUSERZIPCODE										AS EndUserPostalCode
	,ifg.ENDUSERCOUNTRYCODE									AS EndUserCountryCode
	,''														AS DistributorIDNo2 
	,ncsc.VendorReferenceNumber								AS VendorReferenceNumber											
	,ifg.SALESORDERNUMBER									AS NavsionSalesOrderNumber
	,ncsc.DelveryNoteId										AS DeliveryNoteId   
FROM ifg.POSData ifg
	LEFT JOIN NuviasCSCData ncsc ON ncsc.NuviasSoLink = ifg.SALESORDERNUMBER AND ncsc.NuviasNavLineNum = ifg.SALESORDERLINENO AND ncsc.DelveryNoteId = ifg.IGSSHIPMENTNO
WHERE ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to
	AND ifg.VENDORRESELLERLEVEL LIKE 'Juniper%'
	AND ncsc.NuviasWarehouse <> 'DD' 
	AND ifg.SHIPANDDEBIT = 1
GROUP BY
	ncsc.NuviasEntity
	,ncsc.NuviasWarehouse
	,ifg.ORDERTYPE1
	,ifg.SERIALNUMBER
	,ifg.MANUFACTURERITEMNUMBER
	,ifg.QUANTITY
	,ncsc.NuviasResellerVendorId
	,ifg.VENDORCLAIMID	
	,ifg.VENDORBUYPRICE
	,ncsc.NuviasPurchaseOrder
	,ifg.SALESORDERNUMBER
	,ifg.INVOICENUMBER
	,ifg.CUSTOMERINVOICEDATE
	,ifg.RESELLERPONUMBER
	,ncsc.VarId	
	,ifg.VENDORADDITIONALDISCOUNT1
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
	,ncsc.NuviasSalesId
	,ncsc.VendorReferenceNumber 
	,ifg.SALESORDERNUMBER
	,ncsc.NuviasPurchPrice
	,ifg.SHIPANDDEBIT
	,ncsc.DelveryNoteId 