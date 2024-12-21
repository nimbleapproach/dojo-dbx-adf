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
FROM v_NCSC_NuviasData ncsc
	WHERE ((ncsc.PrimaryVendorName LIKE 'Juniper%')
		AND (ncsc.PrimaryVendorID != 'VAC000904_NGS1')
		AND (ncsc.PrimaryVendorID != 'VAC000904_NNL2')
		AND (ncsc.PrimaryVendorID != 'VAC001110_NGS1')
		AND (ncsc.PrimaryVendorID != 'VAC001110_NNL2'))
)