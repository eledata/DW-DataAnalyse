-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2015-6-25  Moyue           	  Inv Report
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Weekly
drop view VIEW_INV_REP_WK;
CREATE VIEW VIEW_INV_REP_WK AS
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
  TOT_WK13_USAGE_QTY_VAL
FROM
  (SELECT 
    REC_DATE_DATE,
    REC_DATE_WK,
    PLANT,
    STRATEGY_GRP,
    MRP_CONTROLLER,
    PROD_BU,
    MATL_TYPE,
    SUM(MIN_INV_VAL)      AS TOT_MIN_INV_VAL,
    SUM(TARGET_INV_VAL)   AS TOT_TARGET_INV_VAL,
    SUM(MAX_INV_VAL)      AS TOT_MAX_INV_VAL,
    SUM(OH_VAL)           AS TOT_OH_VAL,
    SUM(SIT_VAL)          AS TOT_MIN_SIT_VAL,
    SUM(WK13_USAGE_QTY_VAL) AS TOT_WK13_USAGE_QTY_VAL
  FROM
    (
    SELECT 
      ID,
      INV_ID,
      REC_DATE_DATE,
      REC_DATE_WK,
      PLANT,
      MATERIAL,
      STRATEGY_GRP,
      PROD_BU,
      MATL_TYPE,
      MIN_INV_VAL,
      TARGET_INV_VAL,
      MAX_INV_VAL,
      OH_VAL,
      MRP_CONTROLLER,
      WK13_USAGE_QTY_VAL,
      SIT_VAL
    FROM
    (SELECT PP_INV.ID                                                  AS ID,
      PP_INV.INV_ID                                                    AS INV_ID,
      PP_INV.REC_DATE_DATE                                             AS REC_DATE_DATE,
      PP_INV.REC_DATE_WK                                               AS REC_DATE_WK,
      PP_INV.PLANT                                                     AS PLANT,
      PP_INV.MATERIAL                                                  AS MATERIAL,
      PP_INV.STRATEGY_GRP                                              AS STRATEGY_GRP,
      PP_INV.PROD_BU                                                   AS PROD_BU,
      PP_INV.MATL_TYPE                                                 AS MATL_TYPE,
      PP_INV.MIN_INV_VAL                                               AS MIN_INV_VAL,
      PP_INV.TARGET_INV_VAL                                            AS TARGET_INV_VAL,
      PP_INV.MAX_INV_VAL                                               AS MAX_INV_VAL,
      PP_INV.OH_VAL                                                    AS OH_VAL,
      PP_INV.MRP_CONTROLLER                                            AS MRP_CONTROLLER,
      CEIL(CEIL(PP_INV.WK13_USAGE_QTY)*CEIL(PP_INV.UNIT_COST))       AS WK13_USAGE_QTY_VAL,
      CEIL(CEIL(INV_SIT.STOCK_IN_TRANSIT_QTY)*CEIL(PP_INV.UNIT_COST)) AS SIT_VAL
    FROM
      (SELECT INV_REC.ID        AS ID,
        INV_REC.INV_ID          AS INV_ID,
        INV_REC.REC_DATE_DATE   AS REC_DATE_DATE,
        INV_REC.REC_DATE_WK     AS REC_DATE_WK,
        INV_REC.PLANT           AS PLANT,
        INV_REC.MATERIAL        AS MATERIAL,
        INV_REC.STRATEGY_GRP    AS STRATEGY_GRP,
        INV_REC.PROD_BU         AS PROD_BU,
        INV_REC.UNIT_COST         AS UNIT_COST,
        INV_REC.MATL_TYPE       AS MATL_TYPE,
        INV_REC.MIN_INV_VAL     AS MIN_INV_VAL,
        INV_REC.TARGET_INV_VAL  AS TARGET_INV_VAL,
        INV_REC.MAX_INV_VAL     AS MAX_INV_VAL,
        INV_REC.OH_VAL          AS OH_VAL,
        PP_OPT.MRP_CONTROLLER   AS MRP_CONTROLLER,
        PP_OPT.REC_DATE_USG   AS REC_DATE_USG,
        PP_OPT.WK13_USAGE_QTY   AS WK13_USAGE_QTY
      FROM
        (SELECT INV_ID,
          ID,
          REC_DATE                       AS REC_DATE_DATE,
          TO_CHAR(REC_DATE,'D')          AS REC_DATE_WK,
          PLANTID                        AS PLANT,
          MATERIAL                       AS MATERIAL,
          STRATEGY_GRP                   AS STRATEGY_GRP,
          PROD_BU                        AS PROD_BU,
          UNIT_COST                      AS UNIT_COST,
          MATL_TYPE                      AS MATL_TYPE,
          MIN_INV_VAL                    AS MIN_INV_VAL,
          TARGET_INV_VAL                 AS TARGET_INV_VAL,
          MAX_INV_VAL                    AS MAX_INV_VAL,
          OH_VAL                         AS OH_VAL
        FROM INV_SAP_INV_REC WHERE PLANTID = '5040' AND REC_DATE BETWEEN TO_CHAR(SYSDATE-3) AND TO_CHAR(SYSDATE)
        and OH_QTY <> 0
        )INV_REC
      LEFT JOIN
        (
        SELECT 
          WK13_USG.REC_DATE_USG||'_'||PP_OP.ID AS PP_ID,
          PP_OP.ID,
          PP_OP.MATERIAL,
          PP_OP.PLANT,
          PP_OP.PROD_BU,
          PP_OP.MRP_CONTROLLER,
          WK13_USG.WK13_USAGE_QTY,
          WK13_USG.REC_DATE_USG
          FROM
        (SELECT ID,
          MATERIAL,
          PLANT,
          PROD_BU,
          MRP_CONTROLLER
        FROM INV_SAP_PP_OPT_X
        )PP_OP
        LEFT JOIN
        (
        SELECT
          REC_DATE_USG,
          ID,
          MATERIAL,
          WK13_USAGE_QTY
         FROM INV_SAP_PP_SO_13WKUSG 
        )WK13_USG
        ON WK13_USG.ID = PP_OP.ID
        )PP_OPT
      ON PP_OPT.PP_ID = INV_REC.INV_ID
      )PP_INV
    LEFT JOIN
      (SELECT SNAPSHOT_DATE
        ||'_'
        ||MATERIAL
        ||'_'
        ||PLANT AS ID,
        STOCK_IN_TRANSIT_QTY
      FROM INV_SAP_INV_SUM_STATS
      )INV_SIT
    ON PP_INV.INV_ID = INV_SIT.ID
    )WHERE REC_DATE_WK = '3'
    )
  GROUP BY REC_DATE_DATE,
    REC_DATE_WK,
    PLANT,
    STRATEGY_GRP,
    PROD_BU,
    MRP_CONTROLLER,
    MATL_TYPE
  )
WHERE REC_DATE_DATE BETWEEN TO_CHAR(SYSDATE - 182) AND TO_CHAR(SYSDATE);



SELECT DISTINCT REC_DATE_DATE from VIEW_INV_REP_WK;
SELECT DISTINCT REC_DATE FROM INV_SAP_INV_REC WHERE REC_DATE BETWEEN TO_CHAR(SYSDATE - 2) AND TO_CHAR(SYSDATE - 1);
CREATE TABLE INV_SAP_INV_REC_29 AS
SELECT * FROM INV_SAP_INV_REC WHERE REC_DATE BETWEEN TO_CHAR(SYSDATE - 2) AND TO_CHAR(SYSDATE - 1);
SELECT * FROM INV_SAP_INV_REC_29;



	DECLARE
    INSERT_TABLE_INV_REC  VARCHAR2(5000);
    BEGIN
      INSERT_TABLE_INV_REC := 'INSERT INTO INV_SAP_INV_REC
      SELECT
      DISTINCT
      INV.INV_ID                                                                           AS INV_ID,
      INV.ID                                                                               AS ID,
      INV.REC_DATE                                                                         AS REC_DATE,
      INV.PLANTID                                                                          AS PLANTID,
      INV.MATERIAL                                                                       AS MATERIAL,
      PP.CATALOG_DASH                                                                      AS CATALOG_DASH,
      PP.SAFETY_STOCK                                                                      AS SAFETY_STOCK,
      PP.UNIT_COST                                                                         AS UNIT_COST,
      PP.STRATEGY_GRP                                                                      AS STRATEGY_GRP,
      PP.PROD_BU                                                                           AS PROD_BU,
      PP.PROD_FAM                                                                          AS PROD_FAM,
      PP.MATL_TYPE                                                                         AS MATL_TYPE,
      NVL(INV.OH_QTY,0)                                                                    AS OH_QTY,
      NVL(PP.MIN_INV,0)                                                                    AS MIN_INV,
      NVL(PP.TARGET_INV,0)                                                                 AS TARGET_INV,
      NVL(PP.MAX_INV,0)                                                                    AS MAX_INV,
      (NVL(INV.OH_QTY,0)    *NVL(PP.UNIT_COST,0))                                          AS OH_VAL,
      (NVL(PP.MIN_INV,0)    *NVL(PP.UNIT_COST,0))                                          AS MIN_INV_VAL,
      (NVL(PP.TARGET_INV,0) *NVL(PP.UNIT_COST,0))                                          AS TARGET_INV_VAL,
      (NVL(PP.MAX_INV,0)    *NVL(PP.UNIT_COST,0))                                          AS MAX_INV_VAL,
      (NVL(INV.OH_QTY,0)    *NVL(PP.UNIT_COST,0) - NVL(PP.MAX_INV,0) *NVL(PP.UNIT_COST,0)) AS OVER_MAX_VAL
    FROM
      (SELECT 
        SYSDATE - 2||''_''||MATERIAL||''_''||PLANTID AS INV_ID,
        MATERIAL||''_''||PLANTID AS ID,
        SYSDATE - 2 AS REC_DATE,
        MATERIAL,
        PLANTID,
        OH_QTY
      FROM INV_SAP_INV_REC_29 
      )INV
    LEFT JOIN
      (SELECT ID,
        MATERIAL,
        CATALOG_DASH,
        PLANT,
        SAFETY_STOCK,
        UNIT,
        UNIT_COST,
        STRATEGY_GRP,
        PROD_BU,
        PROD_FAM,
        MATL_TYPE,
        MIN_INV,
        TARGET_INV,
        MAX_INV,
        LEAD_TIME
      FROM INV_SAP_PP_OPT_X
      )PP
    ON PP.ID = INV.ID';
    EXECUTE IMMEDIATE INSERT_TABLE_INV_REC;
    END;



SELECT 
      *
      FROM INV_PPXST_MARAPR_2016 WHERE REC_DATE BETWEEN TO_CHAR(SYSDATE - 20) AND TO_CHAR(SYSDATE + 2)
      
      
CREATE TABLE INV_PPXST_MAYJUN_2016 AS SELECT * FROM INV_PPXST_MARAPR_2016 WHERE REC_DATE BETWEEN TO_CHAR(SYSDATE - 18) AND TO_CHAR(SYSDATE + 2);

SELECT  * FROM INV_PPXST_JANFEB_2016;
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
  TOT_WK13_USAGE_QTY_VAL FROM VIEW_INV_REP_WK WHERE REC_DATE_DATE BETWEEN TO_CHAR(SYSDATE-10) AND TO_CHAR(SYSDATE)
