SELECT 
	'Infinigate Global Services Ltd'						AS ReportingPartnerName -- Hardcoded
	,ct.INVOICEID											AS ReportingCompanyInvoiceNumber
	,ct.INVOICEDATE											AS ReportingCompanyBillDate 
	,pa.ADDRESSDESCRIPTION									AS BillToName
	,pa.ADDRESSSTREET										AS BillToAddress 
	,pa.ADDRESSCITY											AS BillToCity
	,'NA'													AS BillToState 
	,pa.ADDRESSCOUNTRYREGIONISOCODE							AS BillToCountry
	,pa.ADDRESSZIPCODE										AS BillToPostalCode
	,sh.SAG_EUADDRESS_NAME									AS EndUserName
	,sh.SAG_EUADDRESS_STREET1 + ' ' + sh.SAG_EUADDRESS_STREET2 	AS EndUserAddress
	--,sh.SAG_EUADDRESS_STREET2								AS EndUserStreet2 --Combined with Street 1 as per ticket 15751 MW 20/05/20
	,sh.SAG_EUADDRESS_CITY									AS EndUserCity
	,'NA'													AS EndUserState --Only works for UK addresses
	,sh.SAG_EUADDRESS_COUNTRY								AS EndUserCountry
	,di.ItemName											AS CambiumPartNumber
	,ct.CURRENCYCODE										AS InvoiceCurrency --remove for report
	,CASE WHEN ct.CURRENCYCODE = 'USD' 
			THEN 1 
		ELSE ISNULL(ex.RATE, ex2.RATE)
	END														AS ExchangeRate
	,CASE WHEN ct.CURRENCYCODE = 'USD'
			THEN ct.SALESPRICE
		WHEN ct.CURRENCYCODE = 'GBP' 
			THEN ct.SALESPRICE * ex2.RATE
		ELSE ct.SALESPRICE / ex.RATE
	END														AS UnitSalesPriceUSD
	,ct.SALESPRICE											AS UnitSalesPrice
	,ct.QTY													AS QuantitySold
	,SAG_VENDORREFERENCENUMBER								AS ClaimID
	,CASE WHEN di.PrimaryVendorID LIKE 'VAC001014%' 
		THEN (sl.SAG_VENDORSTANDARDCOST - sl.SAG_PURCHPRICE) * ct.QTY 
		ELSE 0 END											AS ClaimAmount
	,sl.SALESID												AS SalesOrder
	,po.PurchTableID_InterComp								AS PurchaseOrder -- MW-Added as per ticket 18781 (20/08/2020)
	,pl.PURCHPRICE											AS PurchasePrice -- MW-Added as per ticket 18781 (20/08/2020)
	,po.SalesTableID_InterComp								AS IntercompanySalesOrder
	,di.PrimaryVendorID
FROM SAG_SalesLineV2Staging sl 
--JG
	--LEFT JOIN SAG_CustInvoiceTransStaging ct ON ct.INVENTTRANSID = sl.INVENTTRANSID AND ct.DATAAREAID NOT LIKE 'NGS1' 
	LEFT JOIN SAG_CustInvoiceTransStaging ct ON ct.INVENTTRANSID = sl.INVENTTRANSID AND ct.DATAAREAID NOT in( 'NGS1' ,'NNL2')
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID
	LEFT JOIN CustomerPostalAddressStaging pa ON pa.CUSTOMERACCOUNTNUMBER = sh.INVOICEACCOUNT AND pa.ISPRIMARY = '1' and pa.DATAAREAID = sl.DATAAREAID--Removing Duplicates?
	LEFT JOIN ExchangeRates ex ON ex.StartDate = CONVERT(DATE,ct.INVOICEDATE) AND ex.TOCURRENCY = ct.CURRENCYCODE
	LEFT JOIN ExchangeRates2 ex2 ON ex2.StartDate = CONVERT(DATE,ct.INVOICEDATE) AND ex2.FROMCURRENCY = ct.CURRENCYCODE
	LEFT JOIN ara.SO_PO_ID_List po ON po.SalesLineID_Local = sl.INVENTTRANSID -- MW-Added as per ticket 18781 (20/08/2020)
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID = right(po.SalesTableID_InterComp,4)
			--case 
		--	when ct.DATAAREAID = 'NUK1' then 'NGS1'
		--	else 'NNL2'
		--	end
	LEFT JOIN SAG_PurchLineStaging pl ON pl.INVENTTRANSID = po.PurchLineID_InterComp -- MW-Added as per ticket 18781 (20/08/2020)
	--JG NNL2 change
WHERE sl.DATAAREAID NOT in( 'NGS1','NNL2')
		--sl.DATAAREAID != 'NGS1'
	--AND sl.SAG_SHIPANDDEBIT = '1' 
	AND di.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
	AND ct.INVOICEDATE BETWEEN @from AND @to