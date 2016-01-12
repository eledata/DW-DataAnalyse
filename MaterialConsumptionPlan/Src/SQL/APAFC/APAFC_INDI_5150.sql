-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2015-8-6  Moyue           	  5150.
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

CREATE TABLE INV_SAP_MCBZ_IND AS SELECT * FROM INV_SAP_RCCP_INDREQ_THU;

TRUNCATE TABLE INV_SAP_MCBZ_IND;


SELECT ID,
MATERIAL,
CATALOG_DASH,
PLANT,
STRATEGY_GRP,
OH_QTY,
UNIT,
PROD_BU,
PDT,
GRT,
TOT_REQ_WEEK_0,
TOT_REQ_WEEK_1,
TOT_REQ_WEEK_2,
TOT_REQ_WEEK_3,
TOT_REQ_WEEK_4,
TOT_REQ_WEEK_5,
TOT_REQ_WEEK_6,
TOT_REQ_WEEK_7,
TOT_REQ_WEEK_8,
TOT_REQ_WEEK_9,
TOT_REQ_WEEK_10,
TOT_REQ_WEEK_11,
TOT_REQ_WEEK_12
FROM VIEW_INV_SAP_5150_ZB;




DROP VIEW VIEW_INV_SAP_5150_ZB;
CREATE VIEW VIEW_INV_SAP_5150_ZB AS
SELECT RCCP_BASIC_IND.ID         AS ID,
          RCCP_BASIC_IND.MATERIAL         AS MATERIAL,
          RCCP_BASIC_IND.CATALOG          AS CATALOG_DASH,
          RCCP_BASIC_IND.PLANT            AS PLANT,
          RCCP_BASIC_IND.STRATEGY_GRP     AS STRATEGY_GRP,
          RCCP_BASIC_IND.OH_QTY           AS OH_QTY,
          RCCP_BASIC_IND.UNIT             AS UNIT,
          RCCP_BASIC_IND.PROD_BU          AS PROD_BU,
          RCCP_BASIC_IND.PDT          AS PDT,
          RCCP_BASIC_IND.GRT          AS GRT,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_0     AS TOT_REQ_WEEK_0,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_1     AS TOT_REQ_WEEK_1,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_2     AS TOT_REQ_WEEK_2,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_3     AS TOT_REQ_WEEK_3,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_4     AS TOT_REQ_WEEK_4,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_5     AS TOT_REQ_WEEK_5,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_6     AS TOT_REQ_WEEK_6,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_7     AS TOT_REQ_WEEK_7,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_8     AS TOT_REQ_WEEK_8,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_9     AS TOT_REQ_WEEK_9,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_10    AS TOT_REQ_WEEK_10,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_11    AS TOT_REQ_WEEK_11,
          REQ_MCBZ_TOT.TOT_REQ_WEEK_12    AS TOT_REQ_WEEK_12
        FROM
          (SELECT
              ID,
              MATERIAL,
              CATALOG,
              PLANT,
              STRATEGY_GRP,
              OH_QTY,
              PROD_BU,
              UNIT,
              PDT,
              GRT
          FROM VIEW_INV_SAP_5150_ZBB
          )RCCP_BASIC_IND
        LEFT JOIN
          (SELECT MATERIAL
            ||'_'
            ||PLANT AS ID,
            PLANT,
            MATERIAL,
            SUM(IND_REQ_WK_0)  AS TOT_REQ_WEEK_0,
            SUM(IND_REQ_WK_1)  AS TOT_REQ_WEEK_1,
            SUM(IND_REQ_WK_2)  AS TOT_REQ_WEEK_2,
            SUM(IND_REQ_WK_3)  AS TOT_REQ_WEEK_3,
            SUM(IND_REQ_WK_4)  AS TOT_REQ_WEEK_4,
            SUM(IND_REQ_WK_5)  AS TOT_REQ_WEEK_5,
            SUM(IND_REQ_WK_6)  AS TOT_REQ_WEEK_6,
            SUM(IND_REQ_WK_7)  AS TOT_REQ_WEEK_7,
            SUM(IND_REQ_WK_8)  AS TOT_REQ_WEEK_8,
            SUM(IND_REQ_WK_9)  AS TOT_REQ_WEEK_9,
            SUM(IND_REQ_WK_10) AS TOT_REQ_WEEK_10,
            SUM(IND_REQ_WK_11) AS TOT_REQ_WEEK_11,
            SUM(IND_REQ_WK_12) AS TOT_REQ_WEEK_12
          FROM INV_SAP_RCCP_INDREQ_ZB
          WHERE ELEMENT_INDICATOR = 'PP'
          GROUP BY PLANT,
            MATERIAL
          )REQ_MCBZ_TOT
        ON REQ_MCBZ_TOT.ID = RCCP_BASIC_IND.ID;
        
        
CREATE TABLE INV_SAP_5150_ZB AS SELECT PLANT, MATERIAL FROM INV_SAP_PP_OPT_X WHERE MATERIAL = '1756-IB16 A';
truncate table INV_SAP_RCCP_INDREQ_ZB;

DROP VIEW VIEW_INV_SAP_5150_ZBB;
CREATE VIEW VIEW_INV_SAP_5150_ZBB AS
SELECT
  ZB.ID         AS ID,
  ZB.MATERIAL         AS MATERIAL,
  ZB.CATALOG          AS CATALOG,
  ZB.PLANT            AS PLANT,
  ZB.SG               AS STRATEGY_GRP,
  INV_OPTX.TOTAL_OH_QTY  AS OH_QTY,
  ZB.BU               AS PROD_BU,
  ZB.PDT              AS PDT,
  ZB.UNIT              AS UNIT,
  ZB.GRT              AS GRT
FROM
  ( SELECT MATERIAL||'_'||PLANT AS ID, PLANT, MATERIAL, CATALOG, SG, PDT,GRT,BU,UNIT FROM INV_SAP_5150_ZB
  )ZB
LEFT JOIN
  (SELECT MATERIALID||'_'||PLANTID AS ID, TOTAL_OH_QTY FROM INV_SAP_INVENTORY_BY_PLANT
  )INV_OPTX
ON INV_OPTX.ID = ZB.ID;

CREATE TABLE INV_SAP_RCCP_INDREQ_ZB AS
SELECT * FROM INV_SAP_RCCP_INDREQ_ZB;


SELECT ID        AS ID,
    MIN_INV         AS MIN_INV,
    TARGET_INV      AS TARGET_INV,
    MAX_INV         AS MAX_INV
  FROM INV_SAP_PP_OPT_X WHERE STRATEGY_GRP = '40' AND  PLANT   IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140')
