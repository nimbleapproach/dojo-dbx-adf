/*
##############################
Name: Sophos POS Report
Description: Sophos POS Report
Created By: Mark Walton
Date: 26/03/2024
Version: 1.0 - Created based on specification from ticket T20240319.0056
###############################
*/

--##### DECLARE PARAMETERS #####
--DECLARE @from DATETIME = '2024-04-23'
--DECLARE @to DATETIME = '2024-05-01'

-- ###### MAIN QUERY ######
SELECT
	sl.SALESID													AS [Pos Reference Number]
	,CAST(it.DATEPHYSICAL as date)								AS [Transaction Date]
	,di.ItemName												AS [Sophos Sku Code]
	,''															AS [Partner Sku Code]
	,SUM((-1 * it.QTY))											AS [Sales Quantity]
	----,it.INVENTSERIALID										AS [Serial Number]
	,STUFF((SELECT ', ',  its.INVENTSERIALID
			FROM SAG_SalesLineV2Staging sll
				LEFT JOIN SAG_InventTransStaging its ON its.INVENTTRANSID = sl.INVENTTRANSID AND its.DATAAREAID = sl.DATAAREAID
			WHERE
				its.INVENTTRANSID = sll.INVENTTRANSID
				AND its.ItemID = sll.ITEMID
				AND sl.SALESID = sll.SALESID
			FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')  AS [Serial Number(s)]
	,sh.PURCHORDERFORMNUM										AS [Special Pricing Identifier if applicable]
	,''															AS [Price Per Unit For This Deal]
	,sh.CURRENCYCODE											AS [Currency]
	,''															AS [Extended Price For This Deal]
	,pa.CustomerName											AS [Reseller Name]
	,CONCAT_WS(', ', pa.Street1, pa.Street2)					AS [Reseller Address]
	,pa.AddressCity												AS [Reseller City]
	,''															AS [Reseller State]
	,pa.AddressPostalCode										AS [Reseller Zip Code]
	,pa.AddressCountryISO2										AS [Reseller Country]
	,SUBSTRING(CAST(op.Contact_Name as nvarchar), 1,PATINDEX('% %', CAST(op.Contact_Name as nvarchar)))										AS [Reseller Contact - First Name]
	,SUBSTRING(CAST(op.Contact_Name as nvarchar), PATINDEX('% %', CAST(op.Contact_Name as nvarchar)), LEN(CAST(op.Contact_Name as nvarchar)))	AS [Reseller Contact - Last Name]
	,co.EmailAddress											AS [Reseller Contact - E-mail Address]
	,sh.SAG_EUADDRESS_NAME										AS [End-Customer Name]
	,CONCAT_WS(', ', sh.SAG_EUADDRESS_STREET1, sh.SAG_EUADDRESS_STREET2)	AS [End-Customer Address]
	,sh.SAG_EUADDRESS_CITY										AS [End-Customer City]
	,sh.SAG_EUADDRESS_COUNTY									AS [End-Customer State]
	,sh.SAG_EUADDRESS_POSTCODE									AS [End-Customer ZIP Code]
	,sh.SAG_EUADDRESS_COUNTRY									AS [End-Customer Country]
	,SUBSTRING(sh.SAG_EUADDRESS_CONTACT, 1 ,PATINDEX('% %', sh.SAG_EUADDRESS_CONTACT))								AS [End-Customer Contact – First Name ]
	,SUBSTRING(sh.SAG_EUADDRESS_CONTACT, PATINDEX('% %', sh.SAG_EUADDRESS_CONTACT), LEN(sh.SAG_EUADDRESS_CONTACT))	AS [End-Customer Contact – Last Name]
	,sh.SAG_EUADDRESS_EMAIL										AS [End-Customer Contact – E-Mail Address]
	,''															AS [Comments]

FROM SAG_SalesLineV2Staging sl
	LEFT JOIN SAG_InventTransStaging it  ON sl.INVENTTRANSID = it.INVENTTRANSID
	LEFT JOIN v_DistinctItems di ON di.ItemID = it.ITEMID
		AND di.CompanyID =
		CASE
		WHEN sl.DATAAREAID = 'NUK1' THEN 'NGS1'
		ELSE 'NNL2'
		END
	LEFT JOIN SAG_SalesTableStaging sh ON sh.SALESID = sl.SALESID
	LEFT JOIN v_CustomerPrimaryPostalAddressSplit pa ON pa.CustomerAccountNumber = sh.INVOICEACCOUNT
	LEFT JOIN [crm].[Account] ac ON ac.PartyNumber = sh.CUSTACCOUNT
	LEFT JOIN [ora].[Oracle_Opportunities] op ON sales_order = sl.SALESID
	LEFT JOIN [ora].[Oracle_Contacts] co ON CONCAT_WS(' ', CAST(co.FirstName as nvarchar), CAST(co.LastName as nvarchar)) = CAST(op.Contact_Name as nvarchar)
WHERE
	sl.DATAAREAID NOT IN( 'NGS1' ,'NNL2')
	AND sl.SALESSTATUS IN ('1', '2', '3') -- MW 20/06/2023 added status 1 to capture part shipments
	AND it.DATEPHYSICAL BETWEEN @from AND @to
	AND sl.SAG_SHIPANDDEBIT = '1'
	AND di.PrimaryVendorID IN ('VAC001461_NGS1', 'VAC001461_NNL2')
	AND di.ItemGroupID = 'Hardware'
GROUP BY
	sl.SALESID
	,CAST(it.DATEPHYSICAL as date)
	,di.ItemName
	,sl.INVENTTRANSID
	,sl.DATAAREAID
	,sh.PURCHORDERFORMNUM
	,sh.CURRENCYCODE
	,pa.CustomerName
	,CONCAT_WS(', ', pa.Street1, pa.Street2)
	,pa.AddressCity
	,pa.AddressPostalCode
	,pa.AddressCountryISO2
	,SUBSTRING(CAST(op.Contact_Name as nvarchar), 1,PATINDEX('% %', CAST(op.Contact_Name as nvarchar)))
	,SUBSTRING(CAST(op.Contact_Name as nvarchar), PATINDEX('% %', CAST(op.Contact_Name as nvarchar)), LEN(CAST(op.Contact_Name as nvarchar)))
	,co.EmailAddress
	,sh.SAG_EUADDRESS_NAME
	,CONCAT_WS(', ', sh.SAG_EUADDRESS_STREET1, sh.SAG_EUADDRESS_STREET2)
	,sh.SAG_EUADDRESS_CITY
	,sh.SAG_EUADDRESS_COUNTY
	,sh.SAG_EUADDRESS_POSTCODE
	,sh.SAG_EUADDRESS_COUNTRY
	,sh.SAG_EUADDRESS_CONTACT
	,sh.SAG_EUADDRESS_EMAIL
	,co.ContactIsPrimaryForAccount
	,co.CreationDate