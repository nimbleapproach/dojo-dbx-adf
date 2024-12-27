SELECT 
	'Zycko Ltd'												AS AccountName 
	,li.PurchTableID_InterComp								AS DistributorPurchaseOrderNumber
	,sh.SAG_CREATEDDATETIME									AS SalesOrderDate
	,sl.SALESID												AS SalesOrderNumber
	,it.DATEPHYSICAL										AS InvoiceDate
	,it.INVOICEID											AS InvoiceNumber
	,sl.SAG_VENDORREFERENCENUMBER							AS SandDRef
	,pa.ADDRESSDESCRIPTION									AS CustomerName
	,pa.ADDRESSSTREET + ' ' + pa.ADDRESSCITY				AS CustomerAddress 
	,pa.ADDRESSZIPCODE										AS CustomerPostalCode
	,pa.ADDRESSCOUNTRYREGIONISOCODE							AS CustomerCountry
	,''														AS Blank1
	,''														AS Blank2
	,sh.SAG_EUADDRESS_NAME									AS EndUserName
	,sh.SAG_EUADDRESS_STREET1 + ' ' + sh.SAG_EUADDRESS_STREET2  + ' ' + sh.SAG_EUADDRESS_CITY  	AS EndUserStreet1
	,sh.SAG_EUADDRESS_POSTCODE								AS EndUserPostalCode
	,sh.SAG_EUADDRESS_COUNTRY								AS EndUserCountry
	,di.ItemName											AS PartCode
	,di.ItemDescription										AS ProductDescription
	,it.INVENTSERIALID										AS ProductSerial
	,-1*it.QTY												AS Quantity
	,sl.SAG_NGS1POBUYPRICE * ex.RATE										AS ProductCostEUR
	,''														AS Blank3
	,''														AS NokiaRef
	,''														AS Blank4 
	,li.SalesTableID_InterComp								AS IntercompanySalesOrder
	
FROM SAG_SalesLineV2Staging sl 
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID
	LEFT JOIN CustomerPostalAddressStaging pa ON pa.CUSTOMERACCOUNTNUMBER = sh.INVOICEACCOUNT AND pa.ISPRIMARY = '1'  and pa.DATAAREAID = sl.DATAAREAID--Removing Duplicates?
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID AND it.DATAAREAID = sl.DATAAREAID
	LEFT JOIN ara.SO_PO_ID_List li ON li.SalesLineID_Local = sl.INVENTTRANSID 
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID and di.CompanyID = right(li.SalesTableID_InterComp,4)
	LEFT JOIN ExchangeRates ex ON ex.StartDate = CONVERT(DATE, it.DATEPHYSICAL)
WHERE
sl.DATAAREAID NOT in( 'NGS1','NNL2')
	AND di.PrimaryVendorName LIKE 'Nokia%'
	AND it.DATEPHYSICAL BETWEEN @from AND @to