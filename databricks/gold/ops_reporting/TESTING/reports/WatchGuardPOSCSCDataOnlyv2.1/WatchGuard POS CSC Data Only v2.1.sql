SELECT 
	ncsc.[D365CustomerAccount(InfiningateEntity)]		AS InfinigateCustomerAccount
	,ifg.CUSTOMERINVOICEDATE							AS InvoiceDate 
	,ifg.MANUFACTURERITEMNUMBER							AS ItemID
	,ncsc.SkuDescription								AS PartCodeDescription
	,ncsc.[D365 Item Group]								AS PartCodeCategory
	--,ifg.QUANTITY										AS Qty
	,CASE WHEN ifg.SERIALNUMBER	IS NULL THEN SUM(ifg.QUANTITY) ELSE ifg.QUANTITY END		AS Qty
	,ncsc.[D365 Var Id]									AS PartnerID
	,ifg.RESELLERNAME									AS BillToName
	,ifg.RESELLERCOUNTRYCODE							AS BillToCountry
	,ifg.RESELLERZIPCODE								AS BillToPostCode
	,ifg.SHIPTONAME										AS ShipToName
	,ifg.SHIPTOCOUNTRYCODE								AS ShipToCountry
	,ifg.SHIPTOZIPCODE									AS ShipToPostCode
	--,ncsc.SerialNumber									AS SerialNumber
	,ifg.SERIALNUMBER									AS SerialNumber
	,ncsc.VendorStandardCost							AS MSPUnitCost
	,ncsc.VendorStandardCost * (ncsc.D365Quantity)		AS MSPTotalCost
	,ncsc.VendorReferenceNumber							AS VendorPromotion
	,ncsc.D365Order										AS NuviasSalesOrderNumber
	,ifg.SALESORDERNUMBER								AS InfinigateSalesOrderNumber
	,ifg.ENDUSERNAME									AS EndCustomerName
	,ifg.ENDUSERZIPCODE									AS EndCustomerPostCode
	,ifg.ENDUSERCOUNTRYCODE								AS EndCustomerCountry
	,ncsc.NuviasExpectBuyInQuoteCurrency				AS NuviasNetUnitBuy
	,ncsc.NuviasExpectBuyInQuoteCurrency * ncsc.D365Quantity	AS NuivasNetTotalBuy
	,ifg.RESELLERPONUMBER								AS CustomerPO
	,ncsc.D365PurchasId									AS DistiPurchaseOrder
	,ncsc.VendorReferenceNumber							AS VendorReferenceNumber
FROM NavData ifg
	LEFT JOIN NuviasCSCData ncsc ON ncsc.[Navision So Number] = ifg.SALESORDERNUMBER AND ncsc.NavisionLineNumber = ifg.SALESORDERLINENO AND ncsc.[D365 Packing Slip Id] = ifg.IGSShipmentNo
WHERE 
	((DATEADD(day, -1, ifg.CUSTOMERINVOICEDATE) >= CAST(ncsc.D365ShipDate as date))
		OR (DATEADD(day, 0, ifg.CUSTOMERINVOICEDATE) = CAST(ncsc.D365ShipDate as date))
		OR (DATEADD(day, +1, ifg.CUSTOMERINVOICEDATE) <= CAST(ncsc.D365ShipDate as date)))
	-- AND ncsc.[D365CustomerAccount(InfiningateEntity)] IN (@CustAccount)
GROUP BY  
	ncsc.[D365CustomerAccount(InfiningateEntity)]
	,ifg.CUSTOMERINVOICEDATE
	,ifg.MANUFACTURERITEMNUMBER
	,ncsc.SkuDescription
	,ncsc.[D365 Item Group]
	,ifg.QUANTITY
	,ncsc.[D365 Var Id]
	,ifg.RESELLERNAME
	,ifg.RESELLERCOUNTRYCODE
	,ifg.RESELLERZIPCODE
	,ifg.SHIPTONAME
	,ifg.SHIPTOCOUNTRYCODE
	,ifg.SHIPTOZIPCODE
	--,ncsc.SerialNumber
	,ifg.SERIALNUMBER
	,ncsc.VendorStandardCost
	,ncsc.VendorReferenceNumber	
	,ncsc.D365Order
	,ifg.SALESORDERNUMBER
	,ifg.ENDUSERNAME
	,ifg.ENDUSERZIPCODE
	,ifg.ENDUSERCOUNTRYCODE
	,ncsc.NuviasExpectBuyInQuoteCurrency
	,ncsc.D365Quantity
	,ifg.RESELLERPONUMBER
	,ncsc.D365PurchasId
	,ncsc.VendorReferenceNumber

UNION ALL

SELECT 
	ncsc.[D365CustomerAccount(InfiningateEntity)]		AS InfinigateCustomerAccount
	,ifg.CUSTOMERINVOICEDATE							AS InvoiceDate
	,ifg.MANUFACTURERITEMNUMBER							AS ItemID
	,ncsc.SkuDescription								AS PartCodeDescription
	,ncsc.[D365 Item Group]								AS PartCodeCategory
--	,ifg.QUANTITY									AS Qty
	,CASE WHEN ifg.SERIALNUMBER	IS NULL THEN SUM(ifg.QUANTITY) ELSE ifg.QUANTITY END		AS Qty
	,ncsc.[D365 Var Id]									AS PartnerID
	,ifg.RESELLERNAME									AS BillToName
	,ifg.RESELLERCOUNTRYCODE							AS BillToCountry
	,ifg.RESELLERZIPCODE								AS BillToPostCode
	,ifg.SHIPTONAME										AS ShipToName
	,ifg.SHIPTOCOUNTRYCODE								AS ShipToCountry
	,ifg.SHIPTOZIPCODE									AS ShipToPostCode
	--,ncsc.SerialNumber									AS SerialNumber
	,ifg.SERIALNUMBER									AS SerialNumber
	,ncsc.VendorStandardCost							AS MSPUnitCost
	,ncsc.VendorStandardCost * (ncsc.D365Quantity)		AS MSPTotalCost
	,ncsc.VendorReferenceNumber							AS VendorPromotion
	,ncsc.D365Order										AS NuviasSalesOrderNumber
	,ifg.SALESORDERNUMBER								AS InfinigateSalesOrderNumber
	,ifg.ENDUSERNAME									AS EndCustomerName
	,ifg.ENDUSERZIPCODE									AS EndCustomerPostCode
	,ifg.ENDUSERCOUNTRYCODE								AS EndCustomerCountry
	,ncsc.NuviasExpectBuyInQuoteCurrency				AS NuviasNetUnitBuy
	,ncsc.NuviasExpectBuyInQuoteCurrency * ncsc.D365Quantity	AS NuivasNetTotalBuy
	,ifg.RESELLERPONUMBER								AS CustomerPO
	,ncsc.D365PurchasId									AS DistiPurchaseOrder
	,ncsc.VendorReferenceNumber							AS VendorReferenceNumber
FROM NavData ifg
	FULL OUTER JOIN NuviasCSCData ncsc ON ncsc.[Navision So Number] = ifg.SALESORDERNUMBER AND ncsc.NavisionLineNumber = ifg.SALESORDERLINENO AND ncsc.[D365 Packing Slip Id] = ifg.[IGSShipmentNo]
WHERE 
	ncsc.[D365CustomerAccount(InfiningateEntity)] IS NULL
GROUP BY  
	ncsc.[D365CustomerAccount(InfiningateEntity)]
	,ifg.CUSTOMERINVOICEDATE
	,ifg.MANUFACTURERITEMNUMBER
	,ncsc.SkuDescription
	,ncsc.[D365 Item Group]
	,ifg.QUANTITY
	,ncsc.[D365 Var Id]
	,ifg.RESELLERNAME
	,ifg.RESELLERCOUNTRYCODE
	,ifg.RESELLERZIPCODE
	,ifg.SHIPTONAME
	,ifg.SHIPTOCOUNTRYCODE
	,ifg.SHIPTOZIPCODE
	--,ncsc.SerialNumber
	,ifg.SERIALNUMBER
	,ncsc.VendorStandardCost
	,ncsc.VendorReferenceNumber	
	,ncsc.D365Order
	,ifg.SALESORDERNUMBER
	,ifg.ENDUSERNAME
	,ifg.ENDUSERZIPCODE
	,ifg.ENDUSERCOUNTRYCODE
	,ncsc.NuviasExpectBuyInQuoteCurrency
	,ncsc.D365Quantity
	,ifg.RESELLERPONUMBER
	,ncsc.D365PurchasId
	,ncsc.VendorReferenceNumber