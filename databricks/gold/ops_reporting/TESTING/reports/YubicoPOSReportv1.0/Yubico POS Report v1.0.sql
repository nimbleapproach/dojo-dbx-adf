SELECT 
	'Infinigate UK Ltd'									AS [Distributor(Account Name)]
	,'infinigate.co.uk'									AS [Distributor (Domain)]
	,'GB'												AS [Distribtutor (Country)]
	,st.SALESNAME										AS [Reseller (AccountName)]
	,''													AS [Reseller (Domain)]
	,ca.AddressCountryISO2								AS [Reseller (Country)]
	,st.SAG_EUADDRESS_NAME								AS [End Customer (Account Name)]
	,''													AS [End Customer (Domain)]
	,st.SAG_EUADDRESS_COUNTRY							AS [End Customer (Country)]
	,st.CUSTOMERREF										AS [PO]
	,FORMAT(st.SAG_CREATEDDATETIME, 'dd-MM-yyyy')		AS [Order Date]
	,di.ItemName										AS [Product]
	,CAST(SUM((it.QTY * -1)) as int)					AS [Quantity]
	--,SalesTableID_InterComp
FROM SAG_SalesTableStaging st
	LEFT JOIN SAG_SalesLineV2Staging sl ON sl.SALESID = st.SALESID
	LEFT JOIN SAG_InventTransStaging it ON it.INVENTTRANSID = sl.INVENTTRANSID
	LEFT JOIN v_CustomerPrimaryPostalAddressSplit ca ON ca.CustomerAccountNumber = st.CUSTACCOUNT 
	LEFT JOIN ara.SO_PO_ID_List li ON li.SalesLineID_Local = sl.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = sl.ITEMID AND di.CompanyID = right(li.SalesTableID_InterComp,4)
WHERE st.DATAAREAID NOT IN ('NGS1', 'NNL2')
	AND it.DATEPHYSICAL BETWEEN @from AND @to
	AND di.PrimaryVendorID = 'VAC001358_NGS1'
GROUP BY
	st.SALESNAME
	,ca.AddressCountryISO2
	,st.SAG_EUADDRESS_NAME
	,st.SAG_EUADDRESS_COUNTRY
	,st.CUSTOMERREF
	,FORMAT(st.SAG_CREATEDDATETIME, 'dd-MM-yyyy')
	,di.ItemName