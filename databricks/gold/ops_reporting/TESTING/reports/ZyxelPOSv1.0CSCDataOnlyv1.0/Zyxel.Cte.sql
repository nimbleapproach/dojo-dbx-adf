with NuviasCSCData as
(
SELECT 
	nuv.CUSTACCOUNT								AS [D365CustomerAccount(InfiningateEntity)]
	,nuv.SALESID								AS [D365Order] 
	,nuv.CUSTOMERREF							AS [D365OrderRef] --								
	,nuv.ITEMID									AS [D365ItemId]
	,nuv.ItemName								AS [D365Sku]
	,nuv.ItemDescription						AS [SkuDescription]
	,nuv.DATEPHYSICAL							AS [D365ShipDate]
	,nuv.INVENTSERIALID							AS [SerialNumber] --
	,nuv.ItemName								AS [Juniper Part Number]
	,nuv.PrimaryVendorName						AS [VendorName]
	,SUM(nuv.QTY)								AS [D365Quantity]
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
	,nuv.SAG_VENDORSTANDARDCOST					AS [VendorStandardCost]
	,ROUND(nuv.SAG_NGS1STANDARDBUYPRICE, 2)		AS [StandardBuyPrice]
	,ROUND(nuv.SAG_NGS1POBUYPRICE, 2)			AS [NuviasPoBuyPrice]
	,nuv.SAG_UNITCOSTINQUOTECURRENCY			AS [NuviasExpectBuyInQuoteCurrency]
FROM v_NCSC_NuviasData nuv
-- WHERE nuv.CUSTACCOUNT IN (@CustAccount)
	WHERE nuv.DATEPHYSICAL BETWEEN DATEADD(day, -31, @from) AND DATEADD(day, +1, @to)
	AND nuv.PrimaryVendorName LIKE 'Zyxel%'
GROUP BY 
	nuv.SALESID
	,nuv.CUSTOMERREF
	,nuv.ITEMID	
	,nuv.CUSTACCOUNT	
	,nuv.ItemName
	,nuv.ItemDescription
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
	,nuv.SAG_VENDORSTANDARDCOST
	,nuv.SAG_NGS1STANDARDBUYPRICE
	,nuv.SAG_NGS1POBUYPRICE
	,nuv.SAG_UNITCOSTINQUOTECURRENCY
),
NavData as
(
  SELECT 
	nav.INFINIGATEENTITY
      ,nav.ORDERTYPE1
      ,nav.ORDERTYPE2
      ,nav.SALESORDERNUMBER
      ,nav.INVOICENUMBER
      ,nav.CUSTOMERINVOICEDATE
      ,nav.INVOICELINENUMBER
      ,nav.INFINIGATEITEMNUMBER
      ,nav.MANUFACTURERITEMNUMBER
      ,nav.QUANTITY
      ,nav.VENDORCLAIMID
      ,nav.VENDORRESELLERLEVEL
      ,nav.VENDORADDITIONALDISCOUNT1
      ,nav.VENDORADDITIONALDISCOUNT2
      ,nav.PRODUCTLISTPRICE
      ,nav.PURCHASECURRENCY
      ,nav.UNITSELLPRICE
      ,nav.UNITSELLCURRENCY
      ,nav.POBUYPRICE
      ,nav.VENDORBUYPRICE
      ,nav.RESELLERPONUMBER
      ,nav.VENDORRESELLERREFERENCE
      ,nav.RESELLERNAME
      ,nav.RESELLERADDRESS1
      ,nav.RESELLERADDRESS2
      ,nav.RESELLERADDRESS3
      ,nav.RESELLERADDRESSCITY
      ,nav.RESELLERSTATE
      ,nav.RESELLERZIPCODE
	  ,nav.RESELLERCOUNTRYCODE
      ,nav.SHIPTONAME
      ,nav.SHIPTOADDRESS1
      ,nav.SHIPTOADDRESS2
      ,nav.SHIPTOADDRESS3
      ,nav.SHIPTOCITY
      ,nav.SHIPTOSTATE
      ,nav.SHIPTOZIPCODE
      ,nav.SHIPTOCOUNTRYCODE
      ,nav.ENDUSERNAME
      ,nav.ENDUSERADDRESS1
      ,nav.ENDUSERADDRESS2
      ,nav.ENDUSERADDRESS3
      ,nav.ENDUSERADDRESSCITY
      ,nav.ENDUSERSTATE
      ,nav.ENDUSERZIPCODE
      ,nav.ENDUSERCOUNTRYCODE
      ,nav.SHIPANDDEBIT
      ,nav.PURCHASEORDERNUMBER
      ,nav.CREATEDDATETIME
      ,nav.SALESORDERLINENO
      ,nav.SERIALNUMBER
      ,nav.[IGSShipmentNo]
      ,nav.[VARID]
	  ,nav.[RESELLERFIRSTNAME]
	  ,nav.[RESELLERLASTNAME]
	  ,nav.[RESLLERTELEPHONENUMBER]
	  ,nav.[RESELLEREMAILADDRESS]
	  ,nav.[ENDCUSTOMERFIRSTNAME]
	  ,nav.[ENDCUSTOMERLASTNAME]
	  ,nav.[ENDCUSTOMERTELEPHONENUMBER]
	  ,nav.[ENDCUSTOMEREMAILADDRESS]
	  ,nav.[RESELLERID]
	  ,nav.[MANUFACTURERPARTNERNO]
	  ,nav.[VATREGISTRATIONNO]
FROM v_NCSC_NavisionData nav
	LEFT JOIN v_DistinctItems di ON di.ItemName = nav.MANUFACTURERITEMNUMBER AND di.CompanyID = 'NNL2'
WHERE 
	nav.CUSTOMERINVOICEDATE BETWEEN @from AND @to
	AND di.PrimaryVendorName LIKE 'Zyxel%'
)
