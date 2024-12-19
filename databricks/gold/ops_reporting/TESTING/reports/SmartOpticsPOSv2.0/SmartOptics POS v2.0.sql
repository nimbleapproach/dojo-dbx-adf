SELECT 
	it.DATEPHYSICAL											AS FinancialDate 
	,di.ItemName											AS ProductName
	,it.INVENTSERIALID										AS SerialNumber
	,-1*it.QTY												AS Quantity
	,sh.CUSTACCOUNT											AS CustomerAccount
	,pa.ADDRESSDESCRIPTION									AS CustomerName
	,sl.CURRENCYCODE										AS SalesOrderCurrency
	,sl.SALESPRICE * (-1*it.QTY)							AS SalesInvoiceUnitPrice
	,CASE WHEN sl.CURRENCYCODE = 'USD'
			THEN sl.SALESPRICE * (-1*it.QTY)
		WHEN sl.CURRENCYCODE = 'GBP' 
			THEN (sl.SALESPRICE * (-1*it.QTY)) * ex2.RATE
		ELSE (sl.SALESPRICE * (-1*it.QTY)) / ex.RATE
	END														AS SalesInvoiceUnitPriceUSD
	,sl.SAG_PURCHPRICE * (-1*it.Qty)						AS ProductUnitPriceUSD
													-- Add calulcated fields from margin USD, USD invoice price - Product Unit Price USD
	,CASE WHEN sl.CURRENCYCODE = 'USD' THEN 1 
		ELSE ISNULL(ex.rate,ex2.RATE)	END					AS ExchangeRate
	--,sl.SALESID												AS SalesOrder -- Added for Reference
	
FROM SAG_SalesLineV2Staging sl 
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID
		LEFT JOIN CustomerPostalAddressStaging pa ON pa.CUSTOMERACCOUNTNUMBER = sh.INVOICEACCOUNT AND pa.ISPRIMARY = '1' and pa.DATAAREAID = sl.DATAAREAID--Removing Duplicates?
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID
	LEFT JOIN ExchangeRates ex ON ex.StartDate = CONVERT(DATE,it.DATEPHYSICAL) AND ex.TOCURRENCY = sl.CURRENCYCODE
	LEFT JOIN ExchangeRates2 ex2 ON ex2.StartDate = CONVERT(DATE,it.DATEPHYSICAL) AND ex2.FROMCURRENCY = sl.CURRENCYCODE
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID =
	case 
		when sl.DATAAREAID = 'NUK1' then 'NGS1'
		else 'NNL2'
		end
	
	--and di.CompanyID = right(it.invoiceID,4)
WHERE

--JG
--sl.DATAAREAID NOT LIKE 'NGS1'
sl.DATAAREAID NOT in ('NGS1','NNL2')
	AND di.PrimaryVendorName LIKE 'Smart Optics%'
	AND it.DATEPHYSICAL BETWEEN @from AND @to