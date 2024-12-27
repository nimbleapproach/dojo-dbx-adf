SELECT
	ifg.INVOICENUMBER							AS InvoiceNumber
	,ifg.CUSTOMERINVOICEDATE					AS InvoiceDate
	,ifg.RESELLERPONUMBER						AS PoNumber
	,ncsc.[D365PurchasId]						AS PoToSnwlNumber 
	,ifg.INVOICELINENUMBER						AS LineItem
	,ifg.MANUFACTURERITEMNUMBER					AS VendorPN 
	,ifg.SERIALNUMBER							AS SerialNumber
	,ifg.MANUFACTURERITEMNUMBER					AS SnwlPnSku
	,ncsc.SkuDescription						AS ItemDesc
	,ifg.QUANTITY								AS ItemQty
	,ncsc.VendorStandardCost					AS CurrentDistiCost
	,ncsc.VendorStandardCost * (ifg.QUANTITY)	AS ExtDistiCost
	,ifg.VENDORCLAIMID							AS [SpecialPricingRequest(SPR-XXXXXXX)]
	,ifg.RESELLERNAME							AS BuyingCustomerName
	,ifg.[VARID]								AS [BuyingCustomerPartner-Id]
	,ifg.RESELLERADDRESS1						AS BuyingCustomerAddress1
	,ifg.RESELLERADDRESS2						AS BuyingCustomerAddress2
	,ifg.RESELLERADDRESSCITY					AS BuyingCustomerCity
	,ifg.RESELLERSTATE							AS BuyingCustomerState
	,ifg.RESELLERZIPCODE						AS BuyingCustomerPostalCode
	,ifg.RESELLERCOUNTRYCODE					AS BuyingCustomerCountry
	,''											AS BuyingCustomerCustNumber	
	,ifg.SHIPTONAME								AS ShipToCustomerName
	,ifg.SHIPTOADDRESS1							AS ShipToCustomerAddress1
	,ifg.SHIPTOADDRESS2							AS ShipToCustomerAddress2
	,ifg.SHIPTOCITY								AS ShipToCustomerCity
	,ifg.SHIPTOSTATE							AS ShipToCustomerState
	,ifg.SHIPTOZIPCODE							AS ShipToCustomerPostalCode
	,ifg.SHIPTOCOUNTRYCODE						AS ShipToCustomerCountry
	,ifg.ENDUSERNAME							AS EndUserCompanyName
	,ifg.ENDCUSTOMERFIRSTNAME					AS EndUserFirstName
	,ifg.ENDCUSTOMERLASTNAME					AS EndUserLastName
	,ifg.ENDUSERADDRESS1						AS EndUserAddress1
	,ifg.ENDUSERADDRESS2						AS EndUserAddress2
	,ifg.ENDUSERADDRESSCITY						AS EndUserCity
	,ifg.ENDUSERSTATE							AS EndUserState
	,ifg.ENDUSERZIPCODE							AS EndUserPostalCode
	,ifg.ENDUSERCOUNTRYCODE						AS EndUserCountry
	,ifg.ENDCUSTOMERTELEPHONENUMBER				AS EndUserTelephoneNumber
	,ifg.ENDCUSTOMEREMAILADDRESS				AS EndUserEmailAddress

FROM NavData ifg
	LEFT JOIN NuviasCSCData ncsc ON ncsc.[Navision So Number] = ifg.SALESORDERNUMBER AND ncsc.NavisionLineNumber = ifg.SALESORDERLINENO --AND ncsc.[D365 Packing Slip Id] = ifg.[IGSSHIPMENTNO] 
WHERE  
	ifg.CUSTOMERINVOICEDATE BETWEEN @from AND @to
GROUP BY 
	ifg.INVOICENUMBER
	,ncsc.D365ShipDate
	,ifg.RESELLERPONUMBER
	,ncsc.[D365PurchasId]
	,ifg.INVOICELINENUMBER
	,ifg.MANUFACTURERITEMNUMBER
	,ncsc.SkuDescription
	,ifg.SERIALNUMBER
	,ifg.MANUFACTURERITEMNUMBER
	,ifg.QUANTITY
	,ncsc.VendorStandardCost
	,ncsc.VendorStandardCost * (ifg.QUANTITY)
	,ifg.VENDORCLAIMID
	,ifg.RESELLERNAME
	,ifg.[VARID]
	,ncsc.VendorReferenceNumber
	,ifg.RESELLERADDRESS1
	,ifg.RESELLERADDRESS2
	,ifg.RESELLERADDRESSCITY
	,ifg.RESELLERSTATE
	,ifg.RESELLERZIPCODE
	,ifg.RESELLERCOUNTRYCODE

	,ifg.SHIPTONAME
	,ifg.SHIPTOADDRESS1
	,ifg.SHIPTOADDRESS2
	,ifg.SHIPTOCITY
	,ifg.SHIPTOSTATE
	,ifg.SHIPTOZIPCODE
	,ifg.SHIPTOCOUNTRYCODE
	,ifg.ENDUSERNAME
	,ifg.ENDCUSTOMERFIRSTNAME
	,ifg.ENDCUSTOMERLASTNAME
	,ifg.ENDUSERADDRESS1
	,ifg.ENDUSERADDRESS2
	,ifg.ENDUSERADDRESSCITY	
	,ifg.ENDUSERSTATE
	,ifg.ENDUSERZIPCODE
	,ifg.ENDUSERCOUNTRYCODE
	,ifg.ENDCUSTOMERTELEPHONENUMBER
	,ifg.ENDCUSTOMEREMAILADDRESS
	,ifg.CUSTOMERINVOICEDATE
	,ifg.SALESORDERLINENO