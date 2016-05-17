-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-4-16   Moyue           	large Order Capture...
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Dig Inv for non-stock item(have free-stock), item value > 10K.
--Want to check demand from AP and global open sto, not ship yet.
--See the oppotini...

SELECT MATERIAL,PLANT,
  COUNT(MATERIAL) AS OPEN_LINE_COUNT
FROM INV_SAP_SALES_VBAK_VBAP_VBUP
WHERE MATERIAL IN
  (SELECT DISTINCT MATERIAL FROM INV_SAP_SALES_VBAK_VBAP_VBUP
  )
GROUP BY MATERIAL,PLANT
ORDER BY PLANT DESC;

SELECT * FROM
(
SELECT MATERIAL,PLANT, SUM_OPEN_QTY
FROM(
SELECT 
MATERIAL,PLANT,
SUM(OPEN_QTY) AS SUM_OPEN_QTY
FROM(
SELECT MATERIAL,PLANT,
  NVL(OPEN_QTY,0) AS OPEN_QTY
FROM INV_SAP_SALES_VBAK_VBAP_VBUP
)
WHERE MATERIAL IN
  (SELECT DISTINCT MATERIAL FROM INV_SAP_SALES_VBAK_VBAP_VBUP
  )
GROUP BY MATERIAL,PLANT
)ORDER BY SUM_OPEN_QTY DESC
)WHERE ROWNUM <= ROUND(0.01 * (SELECT COUNT(*) FROM INV_SAP_SALES_VBAK_VBAP_VBUP));

SELECT ROUND(0.01 * (SELECT COUNT(*) FROM INV_SAP_SALES_VBAK_VBAP_VBUP)) AS DD FROM INV_SAP_SALES_VBAK_VBAP_VBUP
SELECT * FROM INV_SAP_SALES_VBAK_VBAP_VBUP  WHERE MATERIAL = 'PN-344973'