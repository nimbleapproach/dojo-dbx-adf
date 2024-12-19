SELECT 
	CASE WHEN id.INVENTLOCATIONID LIKE '%2' AND id.DATAAREAID = 'NNL2' --add post Rome Go Live (MW)
		THEN '101421538' -- Venlo ID
		WHEN id.INVENTLOCATIONID LIKE'%5' AND id.DATAAREAID = 'NGS1'
		THEN '101417609' -- UK ID 
		WHEN id.INVENTLOCATIONID LIKE'CORR%'AND id.DATAAREAID = 'NGS1'
		THEN '101417609' --Added to pick up random name convention for Corrective Warehouse in NGS1
		WHEN id.INVENTLOCATIONID LIKE '%2' AND id.DATAAREAID = 'NGS1' 
		THEN '101456761' --NGS1 Venlo ID
		--ELSE '' --Removed as it was causing duplicates 24/10/2023. 
	END									AS DistributerIDNumber 
	,CASE WHEN -1*it.QTY > 0
		THEN 'POS' 
		WHEN -1*it.QTY <0 
		THEN 'RD' 
		ELSE '' 
	END									AS DistributerTransactionType
	,CAST(UPPER(it.INVENTSERIALID) AS VARCHAR)			AS ProductSerialNumber
	,di.ItemName						AS ProductJuniperPartNumber
	,ABS(-1*it.QTY)				AS ProductQuantity   
	,sl.SAG_VENDORREFERENCENUMBER		AS SpecialPricingAuthorization
	,sl.SAG_PURCHPRICE					AS [NetPOS(ProductUnitPrice)] --Total Price in US Dollars Initially Paid By Distributor Minus andy Claims Credits
	,''									AS ExportLicenceNumber --Not Required
	,sp.PurchTableID_InterComp			AS DistributorPurchaseOrder  --PO to Juniper
	,sl.SALESID							AS ResaleSalesOrderNumber
	,it.INVOICEID						AS ResaleInvoiceNumber 
	,it.DATEPHYSICAL					AS ResaleInvoiceDate --AKA ShipDate (DD-MON-YYYY)
	,sh.CUSTOMERREF						AS ResellerPONumber 
	,sl.SAG_RESELLERVENDORID			AS JuniperVARID1
	,''									AS BusinessModel1
	,sh.SALESNAME						AS ResellerVARName
	,ca.Street1							AS ResellerVARAddress1
	,ca.Street2							AS ResellerVARAddress2 
	,ca.Street3							AS ResellerVARAddress3
	,ca.AddressCity						AS ResellerVARACity
	,ca.AddressState					AS ResellerVARStateProvince
	,ca.AddressPostalCode				AS ResellerVARPostalCode
	,ca.AddressCountryISO2				AS ResellerVARCountryCode 
	,''									AS JuniperVARID2 --Required If Applicable
	,''									AS BusinessModel2 -- Required if Applicable
	,pa.CompanyName						AS ShipToName
	,pa.Street1							AS ShipToAddress1
	,pa.Street2							AS ShipToAddress2
	,pa.Street3							AS ShipToAddress3
	,pa.City							AS ShipToCity
	,pa.County							AS ShipToStateProvince
	,pa.PostalCode						AS ShipToPostalCode
	,pa.CountryISO2						AS ShipToCountryCode 
	,sh.SAG_EUADDRESS_NAME				AS EndUserName
	,sh.SAG_EUADDRESS_STREET1			AS EndUserAddress1 
	,sh.SAG_EUADDRESS_STREET2			AS EndUserAddress2
	,''									AS EndUserAddress3 
	,sh.SAG_EUADDRESS_CITY				AS EndUserCity
	,sh.SAG_EUADDRESS_COUNTY			AS EndUserStateProvince
	,sh.SAG_EUADDRESS_POSTCODE			AS EndUserPostalCode
	,sh.SAG_EUADDRESS_COUNTRY			AS EndUserCountryCode
	,''									AS DistributorIDNo2 --Required if Applicable
	,'>>>'								AS '>>>'
	,id.INVENTLOCATIONID				AS Warehouse
	,di.ItemGroupName					AS ItemGroup
	--,id.INVENTLOCATIONID + ' ' + id.DATAAREAID AS Concat1
	--,sl.ITEMID
--INTO #finalbeforereport
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID 
	--JG
	--LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT LIKE 'NGS1'
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT in( 'NGS1','NNL2')
	LEFT JOIN ara.SO_PO_ID_List sp ON sp.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN distitem di ON di.ItemID = sl.ITEMID and di.CompanyID = right(sp.SalesTableID_InterComp,4) --Using Temp Table
	LEFT JOIN NGSInventory ng ON ng.INVENTTRANSID = sp.SalesLineID_InterComp --Using Temp Table
	LEFT JOIN SAG_InventDimStaging id ON id.INVENTDIMID	= ng.INVENTDIMID
	LEFT JOIN custadd ca ON ca.CustomerAccountNumber = sh.CUSTACCOUNT --Using Temp Table
	LEFT JOIN logadd pa ON pa.AddressID = sh.DELIVERYPOSTALADDRESS --Using Temp Table
WHERE 
--JG
--sl.DATAAREAID NOT LIKE 'NGS1'
sl.DATAAREAID NOT in( 'NGS1','NNL2')
	--AND sl.SAG_SHIPANDDEBIT = '1'
	AND ((di.PrimaryVendorName LIKE 'Juniper%')
		AND (di.PrimaryVendorID != 'VAC000904_NGS1')-- Added to remove Mist product where vendor name was changed to "Juniper" (02/09/2020 MW - Connectwise #201748)
		AND (di.PrimaryVendorID != 'VAC000904_NNL2')
		AND (di.PrimaryVendorID != 'VAC001110_NGS1')
		AND (di.PrimaryVendorID != 'VAC001110_NNL2')) 
	AND id.INVENTLOCATIONID NOT LIKE 'DD'
	AND it.DATEPHYSICAL BETWEEN @from AND @to

GROUP BY
	id.INVENTLOCATIONID
	,it.INVENTTRANSID --added due to missing items with no serials MW 21/05/2020
	,it.QTY
	,it.INVENTSERIALID
	,di.ItemName
	,sl.SAG_VENDORREFERENCENUMBER
	,sl.SAG_PURCHPRICE
	,sp.PurchTableID_InterComp 
	,sl.SALESID
	,it.INVOICEID
	,it.DATEPHYSICAL
	,sh.CUSTOMERREF
	,sl.SAG_RESELLERVENDORID
	,sh.SALESNAME
	,ca.Street1
	,ca.street2
	,ca.street3
	,ca.ADDRESSCITY
	,ca.ADDRESSSTATE
	,ca.AddressPostalCode
	,ca.AddressCountryISO2
	,pa.CompanyName
	,pa.Street1
	,pa.street2 
	,pa.Street3
	,pa.CITY
	,pa.County
	,pa.PostalCode	
	,pa.CountryISO2
	,sh.SAG_EUADDRESS_NAME
	,sh.SAG_EUADDRESS_STREET1
	,sh.SAG_EUADDRESS_STREET2
	,sh.SAG_EUADDRESS_CITY
	,sh.SAG_EUADDRESS_COUNTY
	,sh.SAG_EUADDRESS_POSTCODE
	,sh.SAG_EUADDRESS_COUNTRY
	,di.ItemGroupName
	,id.DATAAREAID