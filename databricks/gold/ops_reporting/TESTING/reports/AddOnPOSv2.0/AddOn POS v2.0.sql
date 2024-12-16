SELECT 
	sl.DATAAREAID									AS Company
	,it.DATEPHYSICAL								AS InvoiceDate
	,sh.SALESNAME									AS CustomerName
	,sh.CUSTACCOUNT									AS CustomerAccount
	,pa.COUNTRYREGIONID								AS ShipToCountry
	,sh.CUSTOMERREF									AS ResellerPONumber 
	,sl.SALESID										AS SalesOrderNumber
	,it.INVOICEID									AS InvoiceNumber
	,di.ItemName									AS Product
	,-1*it.QTY										AS Quantity 
	,sl.CURRENCYCODE								AS CurrencyCode
	,CASE WHEN sl.CURRENCYCODE = 'USD' 
		THEN sl.salesPrice
		WHEN sl.CURRENCYCODE = 'GBP' 
		THEN sl.SALESPRICE * ex2.RATE
		ELSE sl.SALESPRICE / ex.RATE
		END											AS SalesPrice
	,sl.SAG_VENDORREFERENCENUMBER					AS NSDAuthorisation
	,sp.PurchTableID_InterComp						AS DistriPurchasOrder
	,case
	when pl.LINEAMOUNT = 0 and  pl.PURCHQTY = 0 then 	sl.SAG_PURCHPRICE		
	else pl.LINEAMOUNT / pl.PURCHQTY	
	end AS POBuyPrice
	,case  when pl.LINEAMOUNT = 0 and  pl.PURCHQTY = 0 then (-1*it.QTY)	*sl.SAG_PURCHPRICE
	else (pl.LINEAMOUNT / pl.PURCHQTY) * (-1*it.QTY)	
	end AS POTotalBuyPrice
	,sl.SAG_PURCHPRICE								AS ExpectedVendorBuyPrice
	,(pl.PURCHPRICE - sl.SAG_PURCHPRICE) * (-1*it.QTY) AS ClaimAmount
	,'>>>' as smth
	,it.INVENTSERIALID								AS SerialNumber	
	,sp.SalesTableID_InterComp						AS IntercompanySalesOrder
	, case when  pl.LINEAMOUNT = 0 and  pl.PURCHQTY = 0 then 'PO QTY Zero'
	else ''
	end
	as 'Check'
FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID NOT in( 'NGS1' ,'NNL2')
		LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID AND sh.DATAAREAID NOT in( 'NGS1' ,'NNL2')
	LEFT JOIN ara.SO_PO_ID_List sp ON sp.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID = right(sp.SalesTableID_InterComp,4)
	LEFT JOIN SAG_LogisticsPostalAddressBaseStaging pa ON pa.ADDRESSRECID = sh.DELIVERYPOSTALADDRESS
	LEFT JOIN ExchangeRates ex ON ex.StartDate = CONVERT(DATE, it.DATEPHYSICAL) AND ex.TOCURRENCY = sl.CURRENCYCODE 
	LEFT JOIN ExchangeRates2 ex2 ON ex2.StartDate = CONVERT(DATE, it.DATEPHYSICAL) AND ex2.FROMCURRENCY = sl.CURRENCYCODE
	LEFT JOIN SAG_PurchLineStaging pl ON pl.INVENTTRANSID = sp.PurchLineID_InterComp AND pl.DATAAREAID in( 'NGS1' ,'NNL2')
WHERE 
 sl.DATAAREAID NOT in ('NGS1','NNL2')
 	AND di.PrimaryVendorName LIKE 'Prolabs%'
	--comment
	AND it.DATEPHYSICAL BETWEEN @from AND @to