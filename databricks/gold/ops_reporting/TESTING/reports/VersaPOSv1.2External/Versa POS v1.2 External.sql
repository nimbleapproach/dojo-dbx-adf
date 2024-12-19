SELECT 
	it.INVENTSERIALID				AS SerialNumber
	,di.ItemName					AS VersaModelNumber
	,sh.SAG_EUADDRESS_NAME			AS CustomerName
	,CASE WHEN sh.SAG_EUADDRESS_STREET1 = '' AND sh.SAG_EUADDRESS_STREET2 = '' AND sh.SAG_EUADDRESS_CITY = '' AND sh.SAG_EUADDRESS_COUNTY = '' AND sh.SAG_EUADDRESS_POSTCODE = '' 
			THEN '' 
			WHEN sh.SAG_EUADDRESS_STREET2 = '' 
			THEN sh.SAG_EUADDRESS_STREET1 +  ', ' + sh.SAG_EUADDRESS_CITY + ', ' + sh.SAG_EUADDRESS_COUNTY + ', ' + sh.SAG_EUADDRESS_POSTCODE 
			ELSE sh.SAG_EUADDRESS_STREET1 + ', ' + sh.SAG_EUADDRESS_STREET2 + ', ' + sh.SAG_EUADDRESS_CITY + ', ' + sh.SAG_EUADDRESS_COUNTY + ', ' + sh.SAG_EUADDRESS_POSTCODE
		END								AS CustomerAddress
	,it.DATEPHYSICAL				AS ShippingDate
	,SUBSTRING(di.PrimaryVendorName, 1, CHARINDEX(' ', di.PrimaryVendorName)-1)		AS OEMName
	,CASE WHEN di.PrimaryVendorName	LIKE 'Advantech%' 
		THEN di.ItemDescription
			ELSE di.ItemName 
	END								AS OEMPartNumber
	,sh.SAG_EUADDRESS_COUNTRY		AS Country 
	,li.PurchTableID_InterComp		AS PONumber 
	,sh.SAG_EUADDRESS_EMAIL			AS CustomerContactEmail 
	,sh.SAG_RESELLEREMAILADDRESS	AS PartnerEmail 
	,sl.SALESID						AS SalesOrder --For reference
	,sl.ITEMID						AS ItemId	
	,CASE WHEN RIGHT(li.SalesTableID_InterComp, 4) = 'NGS1' THEN 'United Kingdom'
		WHEN RIGHT(li.SalesTableID_InterComp, 4) = 'NNL2' THEN 'Netherlands'			END AS ShippingLegalEntity
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID = sl.DATAAREAID
	LEFT JOIN ara.SO_PO_ID_List li ON li.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID AND di.CompanyID = RIGHT(li.SalesTableID_InterComp, 4)
WHERE sl.DATAAREAID NOT IN ('NGS1', 'NNL2')
	AND it.DATEPHYSICAL BETWEEN @from AND @to
	AND it.STATUSISSUE = '1'
	--AND ((di.PrimaryVendorID LIKE 'VAC001208_NGS1') --Jabil 
	AND ((di.PrimaryVendorID LIKE 'VAC000850_NGS1') --Versa
		--OR (di.PrimaryVendorID LIKE 'VAC001068_NGS1') --Advantech Removed due to  T20240418.0066 update on 18/06/2024 
		--OR (di.PrimaryVendorID LIKE 'VAC001208_NNL2') --Jabil NNL2
		OR (di.PrimaryVendorID LIKE 'VAC000850_NNL2')) --Versa NNL2
		--OR (di.PrimaryVendorID LIKE 'VAC001068_NNL2') --Advantech NNL2 Removed due to  T20240418.0066 update on 18/06/2024 
		--OR (di.PrimaryVendorID LIKE 'VAC001070_NGS1') -- Neeco S.R.O. Removed due to  T20240418.0066 update on 18/06/2024 
		--OR (di.PrimaryVendorID LIKE 'VAC001070_NNL2')) -- Neeco S.R.O. NNL2 Removed due to  T20240418.0066 update on 18/06/2024 
	--AND di.ItemGroupID IN ('Hardware', 'Accessory')
	AND sl.SALESSTATUS LIKE '3'