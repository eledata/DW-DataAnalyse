-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-5-28   Moyue           	JQS&GRC
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

SELECT * FROM
(SELECT MATERIAL FROM INV_SAP_JQS_GRC_PARTLIST)JQS_GRC_PL
LEFT JOIN
(
SELECT * FROM INV_SAP_PP_OPTIMIZATION WHERE PLANTID IN ('5041','5020')
)PP
ON PP.MATERIALID = JQS_GRC_PL.MATERIAL;





SELECT JQS_GRC_PL.MATERIAL,
  PP.MIN_INV,
  PP.TARGET_INV,
  PP.MAX_INV
FROM
  (SELECT MATERIAL FROM INV_SAP_JQS_PARTLIST
  )JQS_GRC_PL
LEFT JOIN
  ( SELECT * FROM INV_SAP_PP_OPT_BASIC WHERE PLANT IN ('1090')
  )PP
ON PP.MATERIAL = JQS_GRC_PL.MATERIAL;