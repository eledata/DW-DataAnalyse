-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2015-11-17    Moyue           	Stock Out Comments Rep
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Stock out comments report.
SELECT 
  DISTINCT
  STOCKOUT.UPDATE_WK, 
  STOCKOUT.ID,
  STOCKOUT.PLANT,
  STOCKOUT.MATERIAL,
  STOCKOUT.MRP_CONTROLLER,
  STOCKOUT.MON,
  STOCKOUT.TUE,
  STOCKOUT.WED,
  STOCKOUT.THU,
  STOCKOUT.FRI,
  COMT.PLANNER, COMT.COMMENTS
FROM
  (SELECT TO_CHAR(UPLOADDATE,'iw') AS UPDATE_WK,
    ID,
    PLANT,
    MATERIAL,
    MRP_CONTROLLER,
    MON,
    TUE,
    WED,
    THU,
    FRI
  FROM INV_SAP_STOCK_OUT_REPSTORE WHERE UPLOADDATE BETWEEN TO_CHAR(SYSDATE-30) AND TO_CHAR(SYSDATE - 1)
  )STOCKOUT
LEFT JOIN
  (SELECT ID, COMMENTS, PLANNER, TO_CHAR(LAST_UPDATE_DATE,'iw') AS CM_UPDATE_WK FROM INV_SAP_STKOUT_COMMENTS
  )COMT
ON COMT.ID = STOCKOUT.ID AND COMT.CM_UPDATE_WK = STOCKOUT.UPDATE_WK;


--Split 5 weeks.
SELECT 
  DISTINCT
  MM_ID_OTTF.ID,
  MM_ID_OTTF.PLANNER,
  MM_ID_OTTF.WK_ONE_COMMENTS,
  MM_ID_OTTF.WK_TWO_COMMENTS,
  MM_ID_OTTF.WK_THREE_COMMENTS,
  MM_ID_OTTF.WK_FOUR_COMMENTS,
  WK_FIVE.COMMENTS AS WK_FIVE_COMMENTS
FROM
  (SELECT MM_ID_OTT.ID,
    MM_ID_OTT.PLANNER,
    MM_ID_OTT.WK_ONE_COMMENTS,
    MM_ID_OTT.WK_TWO_COMMENTS,
    MM_ID_OTT.WK_THREE_COMMENTS,
    WK_FOUR.COMMENTS AS WK_FOUR_COMMENTS
  FROM
    (SELECT MM_ID_OT.ID,
      MM_ID_OT.PLANNER,
      MM_ID_OT.WK_ONE_COMMENTS,
      MM_ID_OT.WK_TWO_COMMENTS,
      WK_THREE.COMMENTS AS WK_THREE_COMMENTS
    FROM
      (SELECT MM_ID_ONE.ID,
        MM_ID_ONE.PLANNER,
        MM_ID_ONE.WK_ONE_COMMENTS,
        WK_TWO.COMMENTS AS WK_TWO_COMMENTS
      FROM
        (SELECT MM_ID.ID,
          MM_ID.PLANNER,
          WK_ONE.COMMENTS AS WK_ONE_COMMENTS
        FROM
          (SELECT ID,
            PLANNER
          FROM INV_SAP_STKOUT_COMMENTS
          WHERE LAST_UPDATE_DATE BETWEEN TO_CHAR(SYSDATE-38) AND TO_CHAR(SYSDATE - 11)
          )MM_ID
        LEFT JOIN
          (SELECT ID,
            COMMENTS,
            CM_UPDATE_WK
          FROM
            (SELECT ID,
              COMMENTS,
              TO_CHAR(LAST_UPDATE_DATE,'iw') AS CM_UPDATE_WK
            FROM INV_SAP_STKOUT_COMMENTS
            WHERE LAST_UPDATE_DATE BETWEEN TO_CHAR(SYSDATE-38) AND TO_CHAR(SYSDATE - 11)
            )
          WHERE CM_UPDATE_WK = TO_CHAR(SYSDATE-38,'iw')
          )WK_ONE
        ON WK_ONE.ID = MM_ID.ID
        )MM_ID_ONE
      LEFT JOIN
        (SELECT ID,
          COMMENTS
        FROM
          (SELECT ID,
            COMMENTS,
            TO_CHAR(LAST_UPDATE_DATE,'iw') AS CM_UPDATE_WK
          FROM INV_SAP_STKOUT_COMMENTS
          WHERE LAST_UPDATE_DATE BETWEEN TO_CHAR(SYSDATE-38) AND TO_CHAR(SYSDATE - 11)
          )
        WHERE CM_UPDATE_WK = TO_CHAR(SYSDATE-38,'iw') + 1
        )WK_TWO
      ON MM_ID_ONE.ID = WK_TWO.ID
      )MM_ID_OT
    LEFT JOIN
      (SELECT ID,
        COMMENTS
      FROM
        (SELECT ID,
          COMMENTS,
          TO_CHAR(LAST_UPDATE_DATE,'iw') AS CM_UPDATE_WK
        FROM INV_SAP_STKOUT_COMMENTS
        WHERE LAST_UPDATE_DATE BETWEEN TO_CHAR(SYSDATE-38) AND TO_CHAR(SYSDATE - 11)
        )
      WHERE CM_UPDATE_WK = TO_CHAR(SYSDATE-38,'iw') + 2
      )WK_THREE
    ON MM_ID_OT.ID = WK_THREE.ID
    )MM_ID_OTT
  LEFT JOIN
    (SELECT ID,
      COMMENTS
    FROM
      (SELECT ID,
        COMMENTS,
        TO_CHAR(LAST_UPDATE_DATE,'iw') AS CM_UPDATE_WK
      FROM INV_SAP_STKOUT_COMMENTS
      WHERE LAST_UPDATE_DATE BETWEEN  TO_CHAR(SYSDATE-38) AND TO_CHAR(SYSDATE - 11)
      )
    WHERE CM_UPDATE_WK = TO_CHAR(SYSDATE-38,'iw') + 3
    )WK_FOUR
  ON MM_ID_OTT.ID = WK_FOUR.ID
  )MM_ID_OTTF
LEFT JOIN
  (SELECT ID,
    COMMENTS
  FROM
    (SELECT ID,
      COMMENTS,
      TO_CHAR(LAST_UPDATE_DATE,'iw') AS CM_UPDATE_WK
    FROM INV_SAP_STKOUT_COMMENTS
    WHERE LAST_UPDATE_DATE BETWEEN  TO_CHAR(SYSDATE-38) AND TO_CHAR(SYSDATE - 11)
    )
  WHERE CM_UPDATE_WK = TO_CHAR(SYSDATE-38,'iw') + 4
  )WK_FIVE
ON MM_ID_OTTF.ID = WK_FIVE.ID;



--Whole month stock status...
SELECT *
FROM VIEW_INV_SAP_STOCK_OUT_REP_FX
WHERE REC_DATE BETWEEN TRUNC(ADD_MONTHS(SYSDATE,-1),'mm') AND LAST_DAY(ADD_MONTHS(SYSDATE,-1))
AND STRATEGY_GRP = '40'
AND MATL_TYPE IN ('ZTG','ZFG')
AND MRP_TYPE IN ('PD')
AND PLANT IN ('5040','5100','5110','5070','5200','5140');


SELECT * FROM INV_SAP_SALES_HST WHERE PLANTID = '5110' AND LINECREATIONDATE BETWEEN TO_CHAR(SYSDATE - 500) AND TO_CHAR(SYSDATE + 5)
