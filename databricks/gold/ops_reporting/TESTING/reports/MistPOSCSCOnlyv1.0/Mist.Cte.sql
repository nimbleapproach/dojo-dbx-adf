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
	,nuv.ItemName								AS [Juniper Part Number]
	,nuv.PrimaryVendorName						AS [VendorName]
	,nuv.QTY									AS [D365Quantity]
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
	,nuv.SAG_NAVSONUMBER						AS [NavSoLink]
	,nuv.DATAAREAID								AS [D365 Entity]
	,nuv.SAG_RESELLERVENDORID					AS [D365 Var Id]
	,nuv.SAG_UNITCOSTINQUOTECURRENCY			AS [CostInQuoteCurrency]
FROM v_NCSC_NuviasData nuv
		WHERE nuv.PrimaryVendorID = 'VAC000904_NNL2'
GROUP BY 
	nuv.SALESID
	,nuv.CUSTOMERREF
	,nuv.ITEMID	
	,nuv.CUSTACCOUNT	
	,nuv.ItemName
	,nuv.SAG_RESELLERVENDORID
	,nuv.INVENTTRANSID
	,nuv.SAG_NAVSONUMBER
	,nuv.DATEPHYSICAL
	,nuv.INVENTSERIALID
	,nuv.PrimaryVendorName
	,nuv.QTY
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
	,nuv.SAG_UNITCOSTINQUOTECURRENCY
)
