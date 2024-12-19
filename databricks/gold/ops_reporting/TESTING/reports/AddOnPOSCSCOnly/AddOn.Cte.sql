with NuviasCSCData as 
(
  SELECT
	nuv.CUSTACCOUNT								AS [D365CustomerAccount(InfiningateEntity)]
	,nuv.SALESID								AS [D365Order] 
	,nuv.CUSTOMERREF							AS [D365OrderRef] --								
	,nuv.ITEMID									AS [D365ItemId]
	,nuv.ItemName								AS [D365Sku]
	,nuv.DATEPHYSICAL							AS [D365ShipDate]
	,nuv.INVENTSERIALID							AS [SerialNumber] --
	,nuv.PrimaryVendorName						AS [VendorName]
	,SUM((-1 * nuv.QTY))						AS [D365Quantity]
	,nuv.LINEAMOUNT								AS [Inter-Company Sell To Navision]
	,nuv.SAG_NAVPONUMBER						AS [Navision Po Number]
	,nuv.SAG_NAVSONUMBER						AS [Navision So Number]	
	,nuv.INVOICEID								AS [D365 Invoice ID] 
	,nuv.STATUSISSUE							AS [Inventory Status]
	,nuv.PACKINGSLIPID							AS [D365 Packing Slip Id]
	,nuv.INVENTLOCATIONID						AS [D365 Warehouse] 
	,nuv.ItemGroupName							AS [D365 Item Group]
	,nuv.SAG_SHIPANDDEBIT						AS [Ship And Debit Flag]
	,nuv.PURCHID								AS [D365PurchasId]
	,nuv.PURCHPRICE								AS [D365PurchPrice]
	,nuv.SAG_VENDORREFERENCENUMBER				AS [VendorReferenceNumber]
	,nuv.SAG_PURCHPRICE							AS [D365ExpectedPurchasePrice]
	,nuv.SAG_NAVLINENUM							AS [NavisionLineNumber]
FROM v_NCSC_NuviasData nuv
	-- WHERE nuv.CUSTACCOUNT IN  (@InfiningateCustomer)
		WHERE nuv.PrimaryVendorName LIKE 'Prolabs%'
GROUP BY 
	nuv.SALESID
	,nuv.CUSTOMERREF
	,nuv.ITEMID	
	,nuv.CUSTACCOUNT	
	,nuv.ItemName
	,nuv.SAG_RESELLERVENDORID
	,nuv.INVENTTRANSID
	,nuv.SAG_NAVSONUMBER
	--,nsl.SAG_NAVLINENUM
	,nuv.DATEPHYSICAL
	,nuv.INVENTSERIALID
	,nuv.PrimaryVendorName
	--,nuv.QTY
	,nuv.LINEAMOUNT
	,nuv.SAG_NAVPONUMBER
	,nuv.SAG_NAVSONUMBER
	,nuv.INVOICEID
	,nuv.STATUSISSUE
	,nuv.PACKINGSLIPID
	,nuv.INVENTLOCATIONID
	,nuv.ItemGroupName
	,nuv.SAG_SHIPANDDEBIT
	,nuv.PURCHID
	,nuv.SAG_PURCHPRICE
	,nuv.PurchTableID_Local
	,nuv.DATAAREAID
	,nuv.INVENTLOCATIONID
	,nuv.PACKINGSLIPID
	,nuv.PURCHID	
	,nuv.PURCHPRICE	
	,nuv.SAG_VENDORREFERENCENUMBER	
	,nuv.SAG_PURCHPRICE	
	,nuv.SAG_NAVLINENUM	
),
NavisionData as
(
  SELECT 
	[INFINIGATEENTITY]
      ,[ORDERTYPE1]
      ,[ORDERTYPE2]
      ,[SALESORDERNUMBER]
      ,[INVOICENUMBER]
      ,[CUSTOMERINVOICEDATE]
      ,[INVOICELINENUMBER]
      ,[INFINIGATEITEMNUMBER]
      ,[MANUFACTURERITEMNUMBER]
      ,[QUANTITY]
      ,[VENDORCLAIMID]
      ,[VENDORRESELLERLEVEL]
      ,[VENDORADDITIONALDISCOUNT1]
      ,[VENDORADDITIONALDISCOUNT2]
      ,[PRODUCTLISTPRICE]
      ,[PURCHASECURRENCY]
      ,[UNITSELLPRICE]
      ,[UNITSELLCURRENCY]
      ,[POBUYPRICE]
      ,[VENDORBUYPRICE]
      ,[RESELLERPONUMBER]
      ,[VENDORRESELLERREFERENCE]
      ,[RESELLERNAME]
      ,[RESELLERADDRESS1]
      ,[RESELLERADDRESS2]
      ,[RESELLERADDRESS3]
      ,[RESELLERADDRESSCITY]
      ,[RESELLERSTATE]
      ,[RESELLERZIPCODE]
      ,[RESELLERCOUNTRYCODE]
      ,[SHIPTONAME]
      ,[SHIPTOADDRESS1]
      ,[SHIPTOADDRESS2]
      ,[SHIPTOADDRESS3]
      ,[SHIPTOCITY]
      ,[SHIPTOSTATE]
      ,[SHIPTOZIPCODE]
      ,[SHIPTOCOUNTRYCODE]
      ,[ENDUSERNAME]
      ,[ENDUSERADDRESS1]
      ,[ENDUSERADDRESS2]
      ,[ENDUSERADDRESS3]
      ,[ENDUSERADDRESSCITY]
      ,[ENDUSERSTATE]
      ,[ENDUSERZIPCODE]
      ,[ENDUSERCOUNTRYCODE]
      ,[SHIPANDDEBIT]
      ,[PURCHASEORDERNUMBER]
      ,[SALESORDERLINENO]
      ,[SERIALNUMBER]
      ,[IGSShipmentNo]
      ,[CREATEDDATETIME]
FROM ifg.POSData 
),
Final as
(
  SELECT	
	ncsc.[D365CustomerAccount(InfiningateEntity)]	AS [InfinigateOrderCompany] --D365 Order Account
	,''												AS [NavisionAccount]
	,nav.CUSTOMERINVOICEDATE						AS [InvoiceDate]
	,nav.RESELLERNAME								AS [CustomerName]
	,''												AS [CustomerAccount]
	,nav.SHIPTOCOUNTRYCODE							AS [ShipToCountry]
	,nav.RESELLERPONUMBER							AS [ResellerPONumber]
	,nav.SALESORDERNUMBER							AS [SalesOrderNumber]
	,nav.INVOICENUMBER								AS [InvoiceNumber]
	--,ncsc.NuviasInventTransId						AS [InventtransID] -- add for reference, not to be shown in report
	--,''											AS [NGS/NNL2SalesOrder] 
	,ncsc.D365Sku									AS [Product]
	,nav.QUANTITY									AS [Quantity]
	,nav.UNITSELLCURRENCY							AS [CurrencyCode]
	,nav.UNITSELLPRICE 								AS [SalesPrice]
	,ncsc.VendorReferenceNumber						AS [NSDAuthorisation]
	,ncsc.D365PurchasId								AS [DistriPurchasOrder]
	,ncsc.D365PurchPrice							AS [PoBuyPrice]
	,ncsc.D365PurchPrice * ncsc.D365Quantity		AS [PoTotalBuyPrice]	
	,ncsc.D365ExpectedPurchasePrice					AS [ExpectedVendorBuyPrice]
	,ncsc.D365PurchPrice - ncsc.D365ExpectedPurchasePrice		AS [ClaimAmount]
	,ncsc.VendorReferenceNumber						AS [VendorReferenceNumber]
	,'Nothing Needed After'							AS [NothingNeededAfter]
	,ncsc.SerialNumber								AS [SerialNumber]
	--,''											AS [IntercompanySalesOrder] --not need anymore as order are being place directly in NNL2 for CSC
	,ncsc.D365Order									AS [CSCOrderNumber]
	,''												AS [PONumber]
	,''												AS [Check]
	--,ncsc.NuviasNavLineNum							AS [D365NavLine]
	,nav.CUSTOMERINVOICEDATE						AS [NavisionInvoiceDate]
FROM NavisionData nav
	RIGHT OUTER JOIN NuviasCSCData ncsc ON ncsc.[Navision So Number] = nav.SALESORDERNUMBER AND ncsc.NavisionLineNumber = nav.SALESORDERLINENO AND ncsc.[D365 Packing Slip Id] = nav.[IGSShipmentNo]
WHERE 
	nav.CUSTOMERINVOICEDATE BETWEEN @from AND @to
	-- AND ncsc.[D365CustomerAccount(InfiningateEntity)] IN (@InfiningateCustomer)
GROUP BY 
	ncsc.[D365CustomerAccount(InfiningateEntity)]	
	,nav.CUSTOMERINVOICEDATE
	,nav.RESELLERNAME
	,nav.SHIPTOCOUNTRYCODE
	,nav.RESELLERPONUMBER
	,nav.SALESORDERNUMBER
	,nav.INVOICENUMBER
	,ncsc.D365Sku
	,nav.QUANTITY
	,nav.UNITSELLCURRENCY
	,nav.UNITSELLPRICE
	,ncsc.D365PurchasId
	,ncsc.D365PurchPrice
	,ncsc.D365ExpectedPurchasePrice
	,ncsc.D365Quantity
	,ncsc.D365Order
	,ncsc.VendorReferenceNumber
	,ncsc.NavisionLineNumber
	,ncsc.SerialNumber
)