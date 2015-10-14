--IN TRAINST TIME
SELECT *
FROM INV_SAP_LIKP_LIPS_DAILY
WHERE REFERENCE_DOC_TRIM IN
  (SELECT DISTINCT EBELNPURCHDOCNO
  FROM INV_SAP_PP_PO_HISTORY
  WHERE BUDATPOSTINGDATE BETWEEN TO_CHAR(SYSDATE - 381) AND TO_CHAR(SYSDATE - 321)
  AND TOTALRECIEVEDYQTY IS NOT NULL
  AND PO_SO_SA_FLAG     IN ('STO')
  AND DELIVERYCOMPLETE  IS NOT NULL
  AND PLANTID           IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140')
  )
            
            
SELECT * FROM INV_SAP_LIKP_LIPS_DAILY where  REFERENCE_DOC_TRIM= '6302232348'
SELECT * FROM INV_SAP_PP_PO_HISTORY WHERE EBELNPURCHDOCNO = '6302232348'


SELECT *
FROM
  (SELECT MATERIALID
    ||'_'
    ||(PLANTID) AS ID,
    MATERIALID,
    PLANTID,
    EBELNPURCHDOCNO,
    ETENRPURCHDELIVSCHLINE,
    BUDATPOSTINGDATE,
    LAST_SHIP_DATE,
    STRATEGY_GRP,
    LIFNR_VENDORNO,
    (BUDATPOSTINGDATE - LAST_SHIP_DATE) AS TRANSIT_DAYS
  FROM INV_SAP_PP_PO_HISTORY
  WHERE BUDATPOSTINGDATE BETWEEN TO_CHAR(SYSDATE - 370) AND TO_CHAR(SYSDATE - 2)
  AND TOTALRECIEVEDYQTY     IS NOT NULL
  AND PO_SO_SA_FLAG         IN ('STO')
  AND DELIVERYCOMPLETE      IS NOT NULL
  AND PLANTID               IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140')
  AND ETENRPURCHDELIVSCHLINE = '1'
  AND LIFNR_VENDORNO = 'V1090'
  )PO
LEFT JOIN
  (SELECT MATERIALID
    ||'_'
    ||(PLANT) AS ID,
    PDT
  FROM
    (SELECT MATERIALID,
      (PLANTID - 1) AS PLANT,
      PDT
    FROM INV_SAP_PP_OPTIMIZATION
    )
  WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140')
  )PP
ON PP.ID = PO.ID



