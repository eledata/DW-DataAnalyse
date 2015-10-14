-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-12-24  Moyue           	SO Hst Stats view
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
--So hst local version.  

CREATE TABLE INV_SAP_ITEM_SO_STAT AS
SELECT * FROM VIEW_INV_SAP_IMSO_LTIN13TMP;

TRUNCATE TABLE INV_SAP_ITEM_SO_STAT;
INSERT INTO INV_SAP_ITEM_SO_STAT SELECT * FROM VIEW_INV_SAP_IMSO_LTIN13TMP;
INSERT INTO INV_SAP_ITEM_SO_STAT SELECT * FROM VIEW_INV_SAP_IMSO_LTOUT13TMP;

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
  
--Update the SO STAT Add in TOT_Open
CREATE VIEW VIEW_INV_SAP_ITEM_SO_STAT_U AS
 SELECT TOT_OPEN_NOCM.ID,
  TOT_OPEN_NOCM.MATERIAL,
  TOT_OPEN_NOCM.PLANT,
  TOT_OPEN_NOCM.TOT_OPEN_QTY,
  TOT_OPEN_NOCM.TOT_NO_COMMITTED_DATE_QTY,
  SO_STAT.LEAD_TIME,
  SO_STAT.PASS_DUE_QTY,
  SO_STAT.LT_OPEN_QTY,
  SO_STAT.LT_WEEKS13_OPEN_QTY,
  SO_STAT.OUT_WEEKS13_OPEN_QTY
FROM
  (SELECT TOT_OPEN.ID AS ID,
    TOT_OPEN.MATERIAL,
    TOT_OPEN.PLANT,
    TOT_OPEN.TOT_OPEN_QTY,
    NO_COMMITTED_DATE.TOT_NO_COMMITTED_DATE_QTY
  FROM
    (SELECT MATERIAL
      ||'_'
      || PLANT AS ID,
      MATERIAL,
      PLANT,
      NVL(SUM(OPEN_QTY),0) AS TOT_OPEN_QTY
    FROM INV_SAP_SALES_VBAK_VBAP_VBUP
    GROUP BY MATERIAL,
      PLANT
    )TOT_OPEN
  LEFT JOIN
    (
    SELECT MATERIAL
      ||'_'
      || PLANT AS ID,
      MATERIAL,
      PLANT,
      NVL(SUM(OPEN_QTY),0) AS TOT_NO_COMMITTED_DATE_QTY
    FROM INV_SAP_SALES_VBAK_VBAP_VBUP
    WHERE MAX_COMMIT_DATE IS NULL
    GROUP BY MATERIAL,
      PLANT
    )NO_COMMITTED_DATE
  ON NO_COMMITTED_DATE.ID = TOT_OPEN.ID
  )TOT_OPEN_NOCM
LEFT JOIN
  (SELECT ID,
    MATERIAL,
    PLANT,
    LEAD_TIME,
    PASS_DUE_QTY,
    LT_OPEN_QTY,
    LT_WEEKS13_OPEN_QTY,
    OUT_WEEKS13_OPEN_QTY
  FROM INV_SAP_ITEM_SO_STAT
  )SO_STAT
ON TOT_OPEN_NOCM.ID = SO_STAT.ID;


