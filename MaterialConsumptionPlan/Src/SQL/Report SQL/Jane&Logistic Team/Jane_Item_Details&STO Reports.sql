-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Description:
--
--  This file just use for sto report and item master
--
-- Revision History:
--
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-12-24  Moyue           	STO report and item master
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Item details
SELECT 
PLANT,
MATERIAL,
STRATEGY_GRP,
PROC_TYPE,
VENDOR_KEY,
VENDOR_ITEM,
LEAD_TIME,
PDT,
GRT
FROM INV_SAP_PP_OPT_X WHERE PLANT = '5040';

SELECT * FROM VIEW_INV_SAP_STO_GRCOPEN;
DROP VIEW VIEW_INV_SAP_STO_GRCOPEN;
CREATE VIEW VIEW_INV_SAP_STO_GRCOPEN AS
SELECT STO_OPEN.STO,
  STO_OPEN.STO_TYPE,
  STO_OPEN.ITEM,
  STO_OPEN.MATERIAL,
  ITEM_BASIC.CATALOG_DASH,
  ITEM_BASIC.MATL_TYPE,
  ITEM_BASIC.MAT_DESC,
  ITEM_BASIC.MRP_TYPE,
  ITEM_BASIC.PROD_BU,
  STO_OPEN.PLANT,
  STO_OPEN.ORDER_QTY,
  NVL(STO_OPEN.OPEN_QTY,0) AS OPEN_QTY,
  STO_OPEN.ORDER_CREATE_DATE,
  STO_OPEN.COMMITTED_DATE,
  STO_OPEN.DELIVERY_DATE,
  STO_OPEN.RECEIVE_DATE,
  STO_OPEN.LAST_GI_DATE,
  STO_OPEN.SPC_PROC_KEY,
  ITEM_BASIC.PDT,
  ITEM_BASIC.GRT,
  STO_OPEN.UNIT_COST,
  STO_OPEN.ORDER_VALUE,
  STO_OPEN.CURRENCY,
  STO_OPEN.STO_VENDOR,
  ITEM_BASIC.PROD_HIERARCHY_18
FROM
  (SELECT OPN_STO.STO_ID,
    OPN_STO.ID,
    OPN_STO.V_ITEM,
    OPN_STO.STO,
    OPN_STO.STO_TYPE,
    OPN_STO.ITEM,
    OPN_STO.MATERIAL,
    OPN_STO.PLANT,
    OPN_STO.ORDER_QTY,
    OPN_STO.ORDER_CREATE_DATE,
    OPN_STO.COMMITTED_DATE,
    OPN_STO.DELIVERY_DATE,
    OPN_STO.RECEIVE_DATE,
    OPN_STO.LAST_GI_DATE,
    OPN_STO.SPC_PROC_KEY,
    OPN_STO.UNIT_COST,
    OPN_STO.ORDER_VALUE,
    OPN_STO.CURRENCY,
    OPN_STO.STO_VENDOR,
    PO_OPEN_QTY.OPEN_QTY
  FROM
    (SELECT EBELNPURCHDOCNO
      ||'_'
      ||MATERIALID AS STO_ID,
      MATERIALID
      ||'_'
      ||PLANTID AS ID,
      MATERIALID
      ||'_'
      ||SUBSTR(LIFNR_VENDORNO,2,5) AS V_ITEM,
      EBELNPURCHDOCNO              AS STO,
      BSART_PURCHDOCTYPE           AS STO_TYPE,
      EBELPPURCHITEMNO             AS ITEM,
      MATERIALID                   AS MATERIAL,
      PLANTID                      AS PLANT,
      MENGESCHEDULEDQTY            AS ORDER_QTY,
      BEDATSCHEDULELINEORDERDATE   AS ORDER_CREATE_DATE,
      COMMITTED_DATE               AS COMMITTED_DATE,
      EINDTPURCHITEMDELIVDATE      AS DELIVERY_DATE,
      SLFDTSTATDELIVERYDATE        AS RECEIVE_DATE,
      LAST_SHIP_DATE               AS LAST_GI_DATE,
      SPC_PROC_KEY                 AS SPC_PROC_KEY,
      UNIT_COST                    AS UNIT_COST,
      NETWRNETORDERVALUE           AS ORDER_VALUE,
      WAERS_CURRENCYKEY            AS CURRENCY,
      SUBSTR(LIFNR_VENDORNO,2,5)   AS STO_VENDOR
    FROM INV_SAP_PP_PO_HISTORY where PLANTID in ('5040','5050')
    )OPN_STO
  LEFT JOIN
    --PO_OPEN_QTY
    (
    SELECT EBELNPURCHDOCNO
      ||'_'
      ||MATERIALID AS PO_ID,
      MATERIALID,
      PLANTID,
      SUM(COMMITTEDQTY) AS OPEN_QTY
    FROM INV_SAP_PP_PO_HISTORY_ALL 
    WHERE DELIVERYCOMPLETE IS NULL
    GROUP BY EBELNPURCHDOCNO
      ||'_'
      ||MATERIALID,
      MATERIALID,
      PLANTID
    )PO_OPEN_QTY
  ON PO_OPEN_QTY.PO_ID = OPN_STO.STO_ID
  )STO_OPEN
LEFT JOIN
  (SELECT ID,
    CATALOG_DASH,
    MAT_DESC,
    MRP_TYPE,
    PROD_BU,
    MATL_TYPE,
    PDT,
    GRT,
    SUBSTR(PROD_HIERARCHY,1,18) AS PROD_HIERARCHY_18
  FROM INV_SAP_PP_OPT_X
  )ITEM_BASIC
ON ITEM_BASIC.ID = STO_OPEN.ID;