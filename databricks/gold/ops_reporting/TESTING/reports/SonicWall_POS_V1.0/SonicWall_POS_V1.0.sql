SELECT	
	it.INVOICEID								AS InvoiceNumber
	,it.DATEPHYSICAL							AS InvoiceDate
	,sh.CUSTOMERREF								AS PoNumber
	--,sl.INVENTTRANSID
	,sp.PurchTableID_InterComp					AS PoToSnwlNumber 
	,sl.LINENUM									AS LineItem
	--,sl.SALESSTATUS					
	,di.ItemName								AS VendorPN 
	,it.INVENTSERIALID							AS SerialNumber
	,di.ItemName								AS SnwlPnSku
	,di.ItemDescription							AS ItemDesc
	,(-1*it.QTY)								AS ItemQty
	,sl.SAG_VENDORSTANDARDCOST					AS CurrentDistiCost 
	,sl.SAG_VENDORSTANDARDCOST * (-1*it.QTY)	AS ExtDistiCost
	,sl.SAG_VENDORREFERENCENUMBER				AS [SpecialPricingRequest(SPR-XXXXXXX)]
	--Sell to details Customer
	,sh.SALESNAME								AS BuyingCustomerName
	,sl.SAG_RESELLERVENDORID					AS [BuyingCustomerPartner-Id]	
	,ca.Street1									AS BuyingCustomerAddress1
	,ca.Street2									AS BuyingCustomerAddress2
	,ca.AddressCity								AS BuyingCustomerCity
	,ca.AddressState							AS BuyingCustomerState
	,ca.AddressPostalCode						AS BuyingCustomerPostalCode
	,ca.AddressCountryISO2						AS BuyingCustomerCountry
	,sh.CUSTACCOUNT								AS BuyingCustomerCustNumber	
	--ship to details
	,pa.CompanyName								AS ShipToCustomerName
	,pa.Street1									AS ShipToCustomerAddress1
	,pa.Street2									AS ShipToCustomerAddress2
	,pa.City									AS ShipToCustomerCity
	,pa.County									AS ShipToCustomerState
	,pa.PostalCode								AS ShipToCustomerPostalCode
	,pa.CountryISO2								AS ShipToCustomerCountry
	--End Customer Details
	,sh.SAG_EUADDRESS_NAME						AS EndUserCompanyName
	,SAG_EUADDRESS_CONTACT
	--,SUBSTRING(SAG_EUADDRESS_CONTACT, 1, CHARINDEX(' ', SAG_EUADDRESS_CONTACT) - 1)						AS EndUserFirstName
	,CASE WHEN CHARINDEX(' ', SAG_EUADDRESS_CONTACT) = 0 
			THEN SAG_EUADDRESS_CONTACT 
				ELSE SUBSTRING(SAG_EUADDRESS_CONTACT, 1, CHARINDEX(' ', SAG_EUADDRESS_CONTACT) - 1)	END AS EndUserFirstName
	--,SUBSTRING(SAG_EUADDRESS_CONTACT, CHARINDEX(' ', SAG_EUADDRESS_CONTACT) + 1, LEN(SAG_EUADDRESS_CONTACT))	AS EndUserLastName
	,CASE WHEN CHARINDEX(' ', SAG_EUADDRESS_CONTACT) = 0 
			THEN SAG_EUADDRESS_CONTACT 
				ELSE SUBSTRING(SAG_EUADDRESS_CONTACT, CHARINDEX(' ', SAG_EUADDRESS_CONTACT) + 1, LEN(SAG_EUADDRESS_CONTACT)) END AS EndUserLastName
	,sh.SAG_EUADDRESS_STREET1					AS EndUserAddress1
	,sh.SAG_EUADDRESS_STREET2					AS EndUserAddress2
	,sh.SAG_EUADDRESS_CITY						AS EndUserCity
	,sh.SAG_EUADDRESS_COUNTY					AS EndUserState
	,sh.SAG_EUADDRESS_POSTCODE					AS EndUserPostalCode
	,sh.SAG_EUADDRESS_COUNTRY					AS EndUserCountry
	,''											AS EndUserTelephoneNumber --not available we capture just an email? 
	,sh.SAG_EUADDRESS_EMAIL						AS EndUserEmailAddress
	,sh.SALESID
	,sl.INVENTTRANSID 
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID 
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT IN ('NGS1', 'NNL2')
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID AND di.CompanyID = CASE WHEN sl.DATAAREAID = 'NUK1' THEN 'NGS1' WHEN sl.DATAAREAID != 'NUK1' THEN 'NNL2' END
	LEFT JOIN ara.SO_PO_ID_List sp ON sp.SalesLineID_Local = sl.INVENTTRANSID 
	LEFT JOIN NGSInventory ng ON ng.INVENTTRANSID = sp.SalesLineID_Local --Using Temp Table
	LEFT JOIN SAG_InventDimStaging id ON id.INVENTDIMID	= ng.INVENTDIMID
	LEFT JOIN v_CustomerPrimaryPostalAddressSplit ca ON ca.CustomerAccountNumber = sh.CUSTACCOUNT 
	LEFT JOIN v_LogisticsPostalAddressSplit pa ON pa.AddressID = sh.DELIVERYPOSTALADDRESS
WHERE sl.DATAAREAID NOT IN ('NGS1', 'NNL2')
	--AND sl.SAG_SHIPANDDEBIT = '1'
	AND ((di.PrimaryVendorID = 'VAC001400_NGS1')
		OR (di.PrimaryVendorID = 'VAC001401_NGS1') --MSP Acount
		OR (di.PrimaryVendorID = 'VAC001443_NGS1') --add after change on 09/02/2024 [T20240209.0047]
		OR (di.PrimaryVendorID = 'VAC001400_NNL2')
		OR (di.PrimaryVendorID = 'VAC001401_NNL2') --MSP Account
		OR (di.PrimaryVendorID = 'VAC001443_NNL2'))--add after change on 09/02/2024 [T20240209.0047]
	AND it.DATEPHYSICAL BETWEEN @from AND @to
	--AND sl.SALESSTATUS IN (2 ,3)
	--AND sl.SALESID = 'SO00137344_NUK1'
--GROUP BY