-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2015-1-27  Moyue           	SO STATS.
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------


  
  
  
--CREATE CALCULATE TABLE FOR SALES ORDER STATISTICS
--Statics of Sales Order
CREATE VIEW VIEW_INV_SAP_IMSO_LTIN13TMP AS
SELECT ID,
  MATERIAL,
  PLANT,
  LEAD_TIME,
  PASS_DUE             AS PASS_DUE_QTY,
  (LT_OPEN             - PASS_DUE) AS LT_OPEN_QTY,
  (LT_WEEKS13_OPEN - LT_OPEN)  AS LT_WEEKS13_OPEN_QTY,
  OUT_WEEKS13_OPEN                     AS OUT_WEEKS13_OPEN_QTY
FROM
  (SELECT ID,
    MATERIAL,
    PLANT,
    LEAD_TIME,
    SUM(PASS_DUE)            AS PASS_DUE,
    SUM(LT_OPEN)             AS LT_OPEN,
    SUM(LT_WEEKS13_OPEN) AS LT_WEEKS13_OPEN,
    SUM(OUT_WEEKS13_OPEN)        AS OUT_WEEKS13_OPEN
  FROM
    (SELECT ID,
      SALES_DOC,
      MATERIAL,
      PLANT,
      LEAD_TIME,
      CASE
        WHEN COMMITTED_DATE < SYSDATE - 1
        THEN OPEN_QTY
        ELSE 0
      END PASS_DUE,
      CASE
        WHEN COMMITTED_DATE < SYSDATE + LEAD_TIME
        THEN OPEN_QTY
        ELSE 0
      END LT_OPEN,
      CASE
        WHEN COMMITTED_DATE < SYSDATE + 91
        THEN OPEN_QTY
        ELSE 0
      END LT_WEEKS13_OPEN,
      CASE
        WHEN COMMITTED_DATE > SYSDATE + 91
        THEN OPEN_QTY
        ELSE 0
      END OUT_WEEKS13_OPEN
    FROM
      (SELECT SALES_OPEN.ID,
        SALES_OPEN.SALES_DOC,
        SALES_OPEN.MATERIAL,
        SALES_OPEN.PLANT,
        SALES_OPEN.OPEN_QTY,
        SALES_OPEN.COMMITTED_DATE,
        ITEM_LT.LEAD_TIME
      FROM (
        (SELECT 
        MATERIAL||'_'||PLANT
        AS ID,
          SALESDOC AS SALES_DOC,
          MATERIAL,
          PLANT,
          OPEN_QTY,
          MAX_COMMIT_DATE AS COMMITTED_DATE
        FROM INV_SAP_SALES_VBAK_VBAP_VBUP
        )SALES_OPEN
      LEFT JOIN
        (SELECT ID, MATERIAL, LEAD_TIME FROM VIEW_INV_SAP_PP_OPT_X
        )ITEM_LT
      ON SALES_OPEN.ID = ITEM_LT.ID)
      )WHERE LEAD_TIME < 91
    )
  GROUP BY ID,
    MATERIAL,
    PLANT,
    LEAD_TIME
  );

--CREATE CALCULATE TABLE FOR SALES ORDER STATISTICS
--Statics of Sales Order
DROP VIEW VIEW_INV_SAP_IMSO_LTOUT13TMP;
CREATE VIEW VIEW_INV_SAP_IMSO_LTOUT13TMP AS
SELECT ID,
  MATERIAL,
  PLANT,
  LEAD_TIME,
  PASS_DUE             AS PASS_DUE_QTY,
  (LT_OPEN             - PASS_DUE) AS LT_OPEN_QTY,
  0  AS LT_WEEKS13_OPEN_QTY,
  OUT_WEEKS13_OPEN                     AS OUT_WEEKS13_OPEN_QTY
FROM
  (SELECT ID,
    MATERIAL,
    PLANT,
    LEAD_TIME,
    SUM(PASS_DUE)            AS PASS_DUE,
    SUM(LT_OPEN)             AS LT_OPEN,
    SUM(OUT_WEEKS13_OPEN)        AS OUT_WEEKS13_OPEN
  FROM
    (SELECT ID,
      SALES_DOC,
      MATERIAL,
      PLANT,
      LEAD_TIME,
      CASE
        WHEN COMMITTED_DATE < SYSDATE - 1
        THEN OPEN_QTY
        ELSE 0
      END PASS_DUE,
      CASE
        WHEN COMMITTED_DATE < SYSDATE + LEAD_TIME
        THEN OPEN_QTY
        ELSE 0
      END LT_OPEN,
      CASE
        WHEN COMMITTED_DATE > SYSDATE + LEAD_TIME
        THEN OPEN_QTY
        ELSE 0
      END OUT_WEEKS13_OPEN
    FROM
      (SELECT SALES_OPEN.ID,
        SALES_OPEN.SALES_DOC,
        SALES_OPEN.MATERIAL,
        SALES_OPEN.PLANT,
        SALES_OPEN.OPEN_QTY,
        SALES_OPEN.COMMITTED_DATE,
        ITEM_LT.LEAD_TIME
      FROM (
        (SELECT 
        MATERIAL||'_'||PLANT
        AS ID,
          SALESDOC AS SALES_DOC,
          MATERIAL,
          PLANT,
          OPEN_QTY,
          MAX_COMMIT_DATE AS COMMITTED_DATE
        FROM INV_SAP_SALES_VBAK_VBAP_VBUP
        )SALES_OPEN
      LEFT JOIN
        (SELECT ID, MATERIAL, LEAD_TIME FROM VIEW_INV_SAP_PP_OPT_X
        )ITEM_LT
      ON SALES_OPEN.ID = ITEM_LT.ID)
      )WHERE LEAD_TIME > 91
    )
  GROUP BY ID,
    MATERIAL,
    PLANT,
    LEAD_TIME
  );
  
  
  
  
  
  
  
SELECT REC_DATE_DATE,
  PLANT,
  STRATEGY_GRP,
  MRP_CONTROLLER,
  PROD_BU,
  MATL_TYPE,
  TOT_MIN_INV_VAL,
  TOT_TARGET_INV_VAL,
  TOT_MAX_INV_VAL,
  TOT_OH_VAL,
  TOT_MIN_SIT_VAL,
  TOT_WK13_USAGE_QTY_VAL FROM VIEW_INV_REP_WK WHERE REC_DATE_DATE BETWEEN TO_CHAR(SYSDATE-192) AND TO_CHAR(SYSDATE)
  
  
  SELECT SNAPSHOT_DATE
        ||'_'
        ||MATERIAL
        ||'_'
        ||PLANT AS ID,
        STOCK_IN_TRANSIT_QTY
      FROM INV_SAP_INV_SUM_STATS
  
delete FROM INV_SAP_INV_REC
        WHERE  REC_DATE BETWEEN TO_CHAR(SYSDATE -1) AND TO_CHAR(SYSDATE)
        