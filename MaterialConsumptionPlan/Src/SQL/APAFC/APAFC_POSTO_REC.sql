


SELECT * FROM 
( 
SELECT  
  STO.ID,  
  STO.LIFNR_VENDORNO, 
  STO.PLANTID, 
  STO.MATERIALID, 
  STO.EBELNPURCHDOCNO, 
  STO.ETENRPURCHDELIVSCHLINESA, 
  STO.BEDATSCHEDULELINEORDERDATE, 
  STO.BUDATPOSTINGDATE, 
  OPT.STRATEGY_GRP 
FROM 
( 
SELECT  
  MATERIALID ||'_'||substr(LIFNR_VENDORNO, 2, 4) AS ID,  
  LIFNR_VENDORNO, 
  PLANTID, 
  MATERIALID, 
  EBELNPURCHDOCNO, 
  ETENRPURCHDELIVSCHLINESA, 
  BEDATSCHEDULELINEORDERDATE, 
  BUDATPOSTINGDATE 
FROM INV_SAP_PP_PO_HISTORY_ALL 
WHERE BUDATPOSTINGDATE BETWEEN TRUNC(ADD_MONTHS(SYSDATE,-1),'mm') AND LAST_DAY(ADD_MONTHS(SYSDATE,-1))
AND TOTALRECIEVEDYQTY IS NOT NULL 
AND PO_SO_SA_FLAG     IN ('STO') 
AND DELIVERYCOMPLETE  IS NOT NULL 
AND LIFNR_VENDORNO IN ('V5200')  
AND PLANTID           IN ('5120', '5160', '5190') 
AND ETENRPURCHDELIVSCHLINESA = '1' 
)STO 
LEFT JOIN 
( 
SELECT ID, STRATEGY_GRP FROM INV_SAP_PP_OPT_X 
)OPT 
ON OPT.ID = STO.ID) 
WHERE STRATEGY_GRP = '40';

SELECT * FROM 
( 
SELECT  
  STO.ID,  
  STO.LIFNR_VENDORNO, 
  STO.PLANTID, 
  STO.MATERIALID, 
  STO.EBELNPURCHDOCNO, 
  STO.ETENRPURCHDELIVSCHLINESA, 
  STO.BEDATSCHEDULELINEORDERDATE, 
  STO.BUDATPOSTINGDATE, 
  OPT.STRATEGY_GRP 
FROM 
( 
SELECT  
  MATERIALID ||'_'||substr(LIFNR_VENDORNO, 2, 4) AS ID,  
  LIFNR_VENDORNO, 
  PLANTID, 
  MATERIALID, 
  EBELNPURCHDOCNO, 
  ETENRPURCHDELIVSCHLINESA, 
  BEDATSCHEDULELINEORDERDATE, 
  BUDATPOSTINGDATE 
FROM INV_SAP_PP_PO_HISTORY_ALL 
WHERE BUDATPOSTINGDATE BETWEEN TRUNC(ADD_MONTHS(SYSDATE,-1),'mm') AND LAST_DAY(ADD_MONTHS(SYSDATE,-1))
AND TOTALRECIEVEDYQTY IS NOT NULL 
AND PO_SO_SA_FLAG     IN ('STO') 
AND DELIVERYCOMPLETE  IS NOT NULL 
AND LIFNR_VENDORNO IN ('V1180','V1090')  
AND PLANTID           IN ('5040', '5050', '5070','5110', '5140', '5200','5100') 
AND ETENRPURCHDELIVSCHLINESA = '1' 
)STO 
LEFT JOIN 
( 
SELECT ID, STRATEGY_GRP FROM INV_SAP_PP_OPT_X 
)OPT 
ON OPT.ID = STO.ID) 
WHERE STRATEGY_GRP = '40';
