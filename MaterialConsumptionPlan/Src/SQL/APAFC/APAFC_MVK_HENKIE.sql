-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-6-9    Moyue           	  Henkie
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
--TIME: MON 2:30PM


CREATE VIEW VIEW_INV_SAP_MVK_HENKIE AS
SELECT MATERIALID,
SALES_ORG,
DIRECT_SHIP_PLANT,
D_CHAIN_BLK,
CURRENT_SERIES,
PREFERRED_PRODUCT
FROM INV_SAP_PP_MVKE
WHERE SALES_ORG in ('5000','5003','5007','5008','5010','5011','5013','5014','5016','5017')
AND DIRECT_SHIP_PLANT in ('5040','5050','5070','5100','5110','5120','5140','5160','5190','5200');



SELECT * FROM VIEW_INV_SAP_MVK_HENKIE;