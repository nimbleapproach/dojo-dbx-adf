SELECT 
	'Infinigate Global Services'			AS DistributorName
	,''										AS ResellerNumber
	,sh.SALESNAME							AS ResellerName
	,CONCAT_WS(', ', ca.Street1, ca.Street2, ca.Street3) AS ResellerAddress
	,ca.AddressCity							AS ResellerCity
	,''										AS ResellerState
	,ca.AddressPostalCode					AS ResellerPostalCode
	,ca.AddressCountryISO2					AS ResellerCountryCode
	,''										AS ResellerContactName
	,''										AS ResellerPhoneNumber
	,''										AS EndUserNumber
	,sh.SAG_EUADDRESS_NAME					AS EndUserName
	,CONCAT_WS(', ', sh.SAG_EUADDRESS_STREET1, sh.SAG_EUADDRESS_STREET2)	AS EndUserAddress
	,sh.SAG_EUADDRESS_CITY					AS EndUserCity
	,sh.SAG_EUADDRESS_COUNTY				AS EndUserState
	,sh.SAG_EUADDRESS_POSTCODE				AS EndUserPostalCode
	,sh.SAG_EUADDRESS_COUNTRY				AS EndUserCountryCode
	,sh.SAG_EUADDRESS_CONTACT				AS EndUserContactName
	,''										AS EndUserPhoneNumber
	,sh.SAG_EUADDRESS_EMAIL					AS EnduserEmail					---Maybe they need the email not phone as we dont store it.
	,sh.CUSTOMERREF							AS ResellerPONumber
	,it.DATEPHYSICAL						AS InvoiceDate
	,di.ItemName							AS PartNumber
	--,sl.ITEMID								AS NuviasPartNumber
	,di.ItemDescription						AS PartDescription
	,it.INVENTSERIALID						AS SerialNumber
	,(-1*it.QTY)							AS Quantity
	,''										AS Amount
	,sl.SAG_VENDORREFERENCENUMBER			AS DANumber
	,sh.CURRENCYCODE						AS CurrencyCode
	,''										AS CountryOfPurchase
	,it.INVOICEID							AS DistributorInvoiceNumber
	,''										AS Custom1
	,''										AS Custom2
	
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID 
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT IN ( 'NGS1','NNL2')
	LEFT JOIN ara.SO_PO_ID_List sp ON sp.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID = 'NGS1' 
	LEFT JOIN NGSInventory ng ON ng.INVENTTRANSID = sp.SalesLineID_InterComp --Using Temp Table
	LEFT JOIN SAG_InventDimStaging id ON id.INVENTDIMID	= ng.INVENTDIMID
	LEFT JOIN custadd ca ON ca.CustomerAccountNumber = sh.CUSTACCOUNT --Using Temp Table
	LEFT JOIN logadd pa ON pa.AddressID = sh.DELIVERYPOSTALADDRESS --Using Temp Table
WHERE --sl.SALESID = 'SO00112767_NUK1'
	sl.DATAAREAID NOT IN( 'NGS1','NNL2')
	--AND sl.SAG_SHIPANDDEBIT = '1'
	AND di.PrimaryVendorID IN ('VAC001044_NGS1')
	--AND id.INVENTLOCATIONID NOT LIKE 'DD'
	AND it.DATEPHYSICAL BETWEEN @from AND @to
--Using physical movement date as parameter, current report uses invoice date for AKA ship date field? 

GROUP BY
	sh.SALESNAME
	,CONCAT_WS(', ', ca.Street1, ca.Street2, ca.Street3)
	,ca.AddressCity	
	,ca.AddressPostalCode
	,ca.AddressCountryISO2	
	,sh.SAG_EUADDRESS_NAME
	,CONCAT_WS(', ', sh.SAG_EUADDRESS_STREET1, sh.SAG_EUADDRESS_STREET2)
	,sh.SAG_EUADDRESS_CITY
	,sh.SAG_EUADDRESS_COUNTY
	,sh.SAG_EUADDRESS_POSTCODE
	,sh.SAG_EUADDRESS_COUNTRY
	,sh.SAG_EUADDRESS_CONTACT
	,sh.SAG_EUADDRESS_EMAIL
	,sh.CUSTOMERREF
	,it.DATEPHYSICAL
	,di.ItemName
	,di.ItemDescription
	,it.INVENTSERIALID
	,(-1*it.QTY)
	,sl.SAG_VENDORREFERENCENUMBER	
	,sh.CURRENCYCODE
	,it.INVOICEID