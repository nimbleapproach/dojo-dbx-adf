with NuviasCSCData as 
(
  SELECT
	ncsc.SALESID					AS NuviasSalesId
	,ncsc.SALESQTY					AS NuivasSalesQty
	,ncsc.ItemName					AS NuviasVendorSku
	,ncsc.SAG_RESELLERVENDORID		AS NuviasResellerVendorId
	,ncsc.INVENTTRANSID				AS NuviasInventTransId
	,ncsc.SAG_NAVSONUMBER			AS NuviasSoLink
	,ncsc.SAG_NAVLINENUM			AS NuviasNavLineNum
	,ncsc.DATEPHYSICAL				AS NuviasDatePhysical
	,ncsc.INVENTSERIALID			AS NuviasSerialNumber
	,ncsc.SAG_PURCHPRICE			AS NuviasPurchPrice
	,ncsc.PURCHID					AS NuviasPurchaseOrder
	,ncsc.DATAAREAID				AS NuviasEntity
	,ncsc.INVENTLOCATIONID			AS NuviasWarehouse
	,ncsc.WMSLOCATIONID				AS NuviasWarehouseLocation
	,ncsc.PACKINGSLIPID				AS DelveryNoteId
	,ncsc.SAG_RESELLERVENDORID		AS VarId
	,ncsc.SAG_VENDORREFERENCENUMBER	AS VendorReferenceNumber
	,ncsc.PrimaryVendorID			AS PrimaryVendorId
	,ncsc.SAG_VENDORSTANDARDCOST	AS VendorStandardCost
FROM v_NCSC_NuviasData ncsc
-- WHERE ncsc.CUSTACCOUNT IN (@CustAccount) 
	WHERE ncsc.PrimaryVendorID IN ('VAC001014_NGS1', 'VAC001144_NGS1', 'VAC001014_NNL2', 'VAC001144_NNL2')
)