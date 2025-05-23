SELECT	
	it.INVOICEID								AS InvoiceNumber
	,it.DATEPHYSICAL							AS InvoiceDate
	,sh.CUSTOMERREF								AS PoNumber
	,sp.PurchTableID_InterComp					AS PoToSnwlNumber 
	,sl.LINENUM									AS LineItem			
	,di.ItemName								AS VendorPN 
	,it.INVENTSERIALID							AS SerialNumber
	,di.ItemName								AS SnwlPnSku
	,di.ItemDescription							AS ItemDesc
	,(-1*it.QTY)								AS ItemQty
	,sl.SAG_VENDORSTANDARDCOST					AS CurrentDistiCost 
	,sl.SAG_VENDORSTANDARDCOST * (-1*it.QTY)	AS ExtDistiCost
	,sl.SAG_VENDORREFERENCENUMBER				AS [SpecialPricingRequest(SPR-XXXXXXX)]
	,sh.SALESNAME								AS BuyingCustomerName
	,sl.SAG_RESELLERVENDORID					AS [BuyingCustomerPartner-Id]	
	,ca.Street1									AS BuyingCustomerAddress1
	,ca.Street2									AS BuyingCustomerAddress2
	,ca.AddressCity								AS BuyingCustomerCity
	,ca.AddressState							AS BuyingCustomerState
	,ca.AddressPostalCode						AS BuyingCustomerPostalCode
	,ca.AddressCountryISO2						AS BuyingCustomerCountry
	,sh.CUSTACCOUNT								AS BuyingCustomerCustNumber	
	,pa.CompanyName								AS ShipToCustomerName
	,pa.Street1									AS ShipToCustomerAddress1
	,pa.Street2									AS ShipToCustomerAddress2
	,pa.City									AS ShipToCustomerCity
	,pa.County									AS ShipToCustomerState
	,pa.PostalCode								AS ShipToCustomerPostalCode
	,pa.CountryISO2								AS ShipToCustomerCountry
	,sh.SAG_EUADDRESS_NAME						AS EndUserCompanyName
	,SAG_EUADDRESS_CONTACT
	,CASE WHEN CHARINDEX(' ', SAG_EUADDRESS_CONTACT) = 0 
			THEN SAG_EUADDRESS_CONTACT 
				ELSE SUBSTRING(SAG_EUADDRESS_CONTACT, 1, CHARINDEX(' ', SAG_EUADDRESS_CONTACT) - 1)	END AS EndUserFirstName
	,CASE WHEN CHARINDEX(' ', SAG_EUADDRESS_CONTACT) = 0 
			THEN SAG_EUADDRESS_CONTACT 
				ELSE SUBSTRING(SAG_EUADDRESS_CONTACT, CHARINDEX(' ', SAG_EUADDRESS_CONTACT) + 1, LEN(SAG_EUADDRESS_CONTACT)) END AS EndUserLastName
	,sh.SAG_EUADDRESS_STREET1					AS EndUserAddress1
	,sh.SAG_EUADDRESS_STREET2					AS EndUserAddress2
	,sh.SAG_EUADDRESS_CITY						AS EndUserCity
	,sh.SAG_EUADDRESS_COUNTY					AS EndUserState
	,sh.SAG_EUADDRESS_POSTCODE					AS EndUserPostalCode
	,sh.SAG_EUADDRESS_COUNTRY					AS EndUserCountry
	,''											AS EndUserTelephoneNumber 
	,sh.SAG_EUADDRESS_EMAIL						AS EndUserEmailAddress
	,sh.SALESID
	,sl.INVENTTRANSID 
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID 
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT IN ('NGS1', 'NNL2')
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID AND di.CompanyID = CASE WHEN sl.DATAAREAID = 'NUK1' THEN 'NGS1' WHEN sl.DATAAREAID != 'NUK1' THEN 'NNL2' END
	LEFT JOIN ara.SO_PO_ID_List sp ON sp.SalesLineID_Local = sl.INVENTTRANSID 
	LEFT JOIN NGSInventory ng ON ng.INVENTTRANSID = sp.SalesLineID_Local 
	LEFT JOIN SAG_InventDimStaging id ON id.INVENTDIMID	= ng.INVENTDIMID
	LEFT JOIN v_CustomerPrimaryPostalAddressSplit ca ON ca.CustomerAccountNumber = sh.CUSTACCOUNT 
	LEFT JOIN v_LogisticsPostalAddressSplit pa ON pa.AddressID = sh.DELIVERYPOSTALADDRESS
WHERE sl.DATAAREAID NOT IN ('NGS1', 'NNL2')
	AND ((di.PrimaryVendorID = 'VAC001400_NGS1')
		OR (di.PrimaryVendorID = 'VAC001401_NGS1') 
		OR (di.PrimaryVendorID = 'VAC001443_NGS1') 
		OR (di.PrimaryVendorID = 'VAC001400_NNL2')
		OR (di.PrimaryVendorID = 'VAC001401_NNL2') 
		OR (di.PrimaryVendorID = 'VAC001443_NNL2'))
	AND it.DATEPHYSICAL BETWEEN @from AND @to