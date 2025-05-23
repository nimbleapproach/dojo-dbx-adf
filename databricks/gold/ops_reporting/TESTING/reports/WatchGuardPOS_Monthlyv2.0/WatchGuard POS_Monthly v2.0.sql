SELECT 
	sl.DATAAREAID								AS Entity
	,it.DATEFINANCIAL							AS InvoiceDate
	,di.ItemName								AS ItemID
	,di.ItemDescription							AS PartCodeDescription
	,di.ItemGroupName							AS PartCodeCategory
	,-1*it.QTY									AS Qty
	,sl.SAG_RESELLERVENDORID					AS PartnerID
	,cu.ORGANIZATIONNAME						AS BillToName
	,cu.ADDRESSCOUNTRYREGIONISOCODE				AS BillToCountry
	,cu.ADDRESSZIPCODE							AS BillToPostCode
	,ad.DESCRIPTION								AS ShipToName
	,ad.COUNTRYREGIONID							AS ShipToCountry
	,ad.ZIPCODE									AS ShipToPostCode
	,it.INVENTSERIALID							AS SerialNumber
	,sl.SAG_VendorStandardCost					AS MSPUnitCost
	,sl.SAG_VendorStandardCost * (-1*it.Qty)	AS MSPTotalCost
	,sl.SAG_VENDORREFERENCENUMBER				AS VendorPromotion
	,sl.SALESID									AS NuviasSalesOrderNumber
	,sh.SAG_EUADDRESS_NAME						AS EndCustomerName
	,sh.SAG_EUADDRESS_POSTCODE					AS EndCustomerPostCode
	,sh.SAG_EUADDRESS_COUNTRY					AS EndCustomerCountry
	,sl.SAG_PURCHPRICE							AS NuviasNetUnitBuy
	,sl.SAG_PURCHPRICE * (-1*it.QTY)			AS NuivasNetTotalBuy
	,sh.CUSTOMERREF								AS CustomerPO
	,li.PurchTableID_InterComp					AS DistiPurchaseOrder
FROM SAG_SalesLineV2Staging sl
		LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT in ('NGS1','NNL2')
		LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID NOT in ('NGS1','NNL2')
	LEFT JOIN SAG_LogisticsPostalAddressBaseStaging ad ON ad.ADDRESSRECID = sh.DELIVERYPOSTALADDRESS
	LEFT JOIN CustCustomerV3Staging cu ON cu.CUSTOMERACCOUNT = sh.CUSTACCOUNT AND cu.DATAAREAID = sh.DATAAREAID
	LEFT JOIN ara.SO_PO_ID_List li ON li.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID =
		CASE WHEN SL.DATAAREAID = 'NUK1' 
			THEN 'NGS1'
				ELSE 'NNL2' END
	WHERE (it.STATUSISSUE IN ('1', '3')
		OR (it.STATUSRECEIPT LIKE '1' AND it.INVOICERETURNED = 1))
	AND di.PrimaryVendorName LIKE 'WatchGuard%'
	AND it.DATEFINANCIAL BETWEEN @from and @to
	AND it.PACKINGSLIPRETURNED <> 1  