--AUTHOR: HUANG MOYUE
--DATE: 6/29/2014
--DES: AUS REPORTS AUTOMATION

---REPORT 1


---REPORT 2
SELECT 
  DELI.PLANTID,
  DELI.DELIVERY,
  DELI.DELIVERY_ITEM,
  DELI.CREATED_BY,
  DELI.CREATED_TIME,
  DELI.CREATED_ON_DATE,
  DELI.MATERIAL_AVAIL_DATE,
  DELI.CHANGED_ON_DATE,
  DELI.PLANNED_GI_DATE,
  DELI.ACT_GI_DATE,
  DELI.DWQ_SRC_EXTRACT_DATE,
  DELI.MATERIALID,
  DELI.MVMT_TYPE,
  DELI.DELIVERY_QTY_SUOM,
  DELI.BASE_UOM,
  DELI.DELIVERY_TYPE,
  DELI.DELIVERY_PRIORITY,
  SODE.SALESDOC,
  SODE.SALESDOCITEM,
  SODE.MATERIAL,
  SODE.MAX_COMMIT_DATE
FROM
  (SELECT REFERENCE_DOC_TRIM
    ||'_'
    || REFERENCE_DOC_ITEM_TRIM AS SO_ID,
    PLANTID,
    DELIVERY,
    DELIVERY_ITEM,
    CREATED_BY,
    CREATED_TIME,
    CREATED_ON_DATE,
    MATERIAL_AVAIL_DATE,
    CHANGED_ON_DATE,
    PLANNED_GI_DATE,
    ACT_GI_DATE,
    DWQ_SRC_EXTRACT_DATE,
    MATERIALID,
    MVMT_TYPE,
    DELIVERY_QTY_SUOM,
    BASE_UOM,
    DELIVERY_TYPE,
    DELIVERY_PRIORITY
  FROM INV_SAP_LIKP_LIPS_DAILY
  WHERE PLANTID = '5070'
  AND CREATED_ON_DATE BETWEEN TO_CHAR(SYSDATE -5) AND TO_CHAR(SYSDATE)
  AND DELIVERY = '8014687957'
  )DELI
LEFT JOIN
  (SELECT SALESDOC
    ||'_'
    || SALESDOCITEM AS SO_ID,
    SALESDOC,
    SALESDOCITEM,
    MATERIAL,
    MAX_COMMIT_DATE
  FROM INV_SAP_SALES_VBAK_VBAP_VBUP
  WHERE PLANT = '5070'
  )SODE
ON SODE.SO_ID = DELI.SO_ID;







SELECT DELI.PLANTID,
  DELI.DELIVERY,
  DELI.DELIVERY_ITEM,
  DELI.CREATED_BY,
  DELI.CREATED_TIME,
  TO_CHAR(DELI.CREATED_ON_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.MATERIAL_AVAIL_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.CHANGED_ON_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.PLANNED_GI_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.ACT_GI_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.DWQ_SRC_EXTRACT_DATE,'MM/DD/YYYY'),
  TO_CHAR(REC_DATE,'MM/DD/YYYY'),
  DELI.MVMT_TYPE,
  DELI.DELIVERY_QTY_SUOM,
  DELI.BASE_UOM,
  DELI.DELIVERY_TYPE,
  DELI.DELIVERY_PRIORITY,
  SODE.SALESDOC,
  SODE.SALESDOCITEM,
  SODE.MATERIAL,
  SODE.MAX_COMMIT_DATE
FROM
  (SELECT REFERENCE_DOC_TRIM
    ||'_'
    || REFERENCE_DOC_ITEM_TRIM AS SO_ID,
    PLANTID,
    DELIVERY,
    DELIVERY_ITEM,
    CREATED_BY,
    CREATED_TIME,
    CREATED_ON_DATE,
    MATERIAL_AVAIL_DATE,
    CHANGED_ON_DATE,
    PLANNED_GI_DATE,
    ACT_GI_DATE,
    DWQ_SRC_EXTRACT_DATE,
    MATERIALID,
    MVMT_TYPE,
    DELIVERY_QTY_SUOM,
    BASE_UOM,
    DELIVERY_TYPE,
    DELIVERY_PRIORITY
  FROM INV_SAP_LIKP_LIPS_DAILY
  WHERE PLANTID    = '5070'
  AND ACT_GI_DATE IS NULL
  AND CREATED_ON_DATE BETWEEN TO_CHAR(SYSDATE - 15) AND TO_CHAR(SYSDATE)
  )DELI
LEFT JOIN
  (SELECT SALESDOC
    ||'_'
    || SALESDOCITEM AS SO_ID,
    SALESDOC,
    SALESDOCITEM,
    MATERIAL,
    MAX_COMMIT_DATE
  FROM INV_SAP_SALES_VBAK_VBAP_VBUP
  WHERE PLANT = '5070'
  )SODE
ON SODE.SO_ID = DELI.SO_ID









"
SELECT DELI.PLANTID,
  DELI.DELIVERY,
  DELI.DELIVERY_ITEM,
  DELI.CREATED_BY,
  DELI.CREATED_TIME,
  TO_CHAR(DELI.CREATED_ON_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.MATERIAL_AVAIL_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.CHANGED_ON_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.PLANNED_GI_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.ACT_GI_DATE,'MM/DD/YYYY'),
  TO_CHAR(DELI.DWQ_SRC_EXTRACT_DATE,'MM/DD/YYYY'),
  DELI.MVMT_TYPE,
  DELI.DELIVERY_QTY_SUOM,
  DELI.BASE_UOM,
  DELI.DELIVERY_TYPE,
  DELI.DELIVERY_PRIORITY,
  SODE.SALESDOC,
  SODE.SALESDOCITEM,
  SODE.MATERIAL,
  TO_CHAR(SODE.MAX_COMMIT_DATE,'MM/DD/YYYY')
FROM
  (SELECT REFERENCE_DOC_TRIM
    ||'_'
    || REFERENCE_DOC_ITEM_TRIM AS SO_ID,
    PLANTID,
    DELIVERY,
    DELIVERY_ITEM,
    CREATED_BY,
    CREATED_TIME,
    CREATED_ON_DATE,
    MATERIAL_AVAIL_DATE,
    CHANGED_ON_DATE,
    PLANNED_GI_DATE,
    ACT_GI_DATE,
    DWQ_SRC_EXTRACT_DATE,
    MATERIALID,
    MVMT_TYPE,
    DELIVERY_QTY_SUOM,
    BASE_UOM,
    DELIVERY_TYPE,
    DELIVERY_PRIORITY
  FROM INV_SAP_LIKP_LIPS_DAILY
  WHERE PLANTID    = '5070'
  AND ACT_GI_DATE IS NULL
  AND CREATED_ON_DATE BETWEEN TO_CHAR(SYSDATE - 15) AND TO_CHAR(SYSDATE)
  )DELI
LEFT JOIN
  (SELECT SALESDOC
    ||'_'
    || SALESDOCITEM AS SO_ID,
    SALESDOC,
    SALESDOCITEM,
    MATERIAL,
    MAX_COMMIT_DATE
  FROM INV_SAP_SALES_VBAK_VBAP_VBUP
  WHERE PLANT = '5070'
  )SODE
ON SODE.SO_ID = DELI.SO_ID"
