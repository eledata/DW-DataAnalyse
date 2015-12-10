-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2015-12-9  Moyue           	  Shipment Fee
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

  CREATE TABLE "SYSTEM"."VEHICLE_FEE" 
   (	"ID" VARCHAR2(200 BYTE), 
	"ORIGIN" VARCHAR2(200 BYTE), 
	"PROVINCE" VARCHAR2(200 BYTE), 
	"LEVEL1" VARCHAR2(200 BYTE), 
	"LEVEL2" VARCHAR2(200 BYTE), 
	"LEVEL3" VARCHAR2(200 BYTE), 
	"LEVEL4" VARCHAR2(200 BYTE), 
	"LEVEL5" VARCHAR2(200 BYTE), 
	"LEVEL6" VARCHAR2(200 BYTE), 
	"LEVEL7" VARCHAR2(200 BYTE),
  "TYPE" VARCHAR2(200 BYTE)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;

  CREATE TABLE "SYSTEM"."WAREHOUSE_SHIP_BILL" 
   (	"NO" VARCHAR2(2000 BYTE), 
	"PICKUP_DATE" VARCHAR2(2000 BYTE), 
	"CARRIER_SHIP_NO" VARCHAR2(2000 BYTE), 
	"ORIGIN" VARCHAR2(2000 BYTE), 
	"SHIP_TO_PROVINCE" VARCHAR2(2000 BYTE), 
	"PACKAGES" VARCHAR2(2000 BYTE), 
	"ACTUAL_WEIGHT" VARCHAR2(2000 BYTE), 
	"CHARGED_WEIGHT" VARCHAR2(2000 BYTE), 
	"FEE" VARCHAR2(2000 BYTE), 
	"REMARK" VARCHAR2(2000 BYTE), 
	"TYPE_OF_SHIPPING" VARCHAR2(2000 BYTE), 
	"VECHICLE_MODEL" VARCHAR2(2000 BYTE), 
	"VECHICLE_NUM" VARCHAR2(2000 BYTE), 
	"SITE" VARCHAR2(2000 BYTE)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
 

  CREATE TABLE "SYSTEM"."WAREHOUSE_SHIP_REP" 
   (	"CARRIER" VARCHAR2(2000 BYTE), 
	"CARRIER_SHIP_NO" VARCHAR2(2000 BYTE), 
	"SAP_SHIPMENT_NO" VARCHAR2(2000 BYTE), 
	"DELIVERY_NO" VARCHAR2(2000 BYTE), 
	"TO_NO" VARCHAR2(2000 BYTE), 
	"SO_NO" VARCHAR2(2000 BYTE), 
	"SITE" VARCHAR2(2000 BYTE), 
	"CUSTOMER_PO" VARCHAR2(2000 BYTE), 
	"SHIP_TO_PARTY" VARCHAR2(2000 BYTE), 
	"COUNTRY" VARCHAR2(2000 BYTE), 
	"CITY" VARCHAR2(2000 BYTE), 
	"PICKUP_DATE" VARCHAR2(2000 BYTE), 
	"AWB" VARCHAR2(2000 BYTE), 
	"PACKAGES" VARCHAR2(2000 BYTE), 
	"ACTUAL_WEIGHT" VARCHAR2(2000 BYTE), 
	"CHARGED_WEIGHT" VARCHAR2(2000 BYTE), 
	"CONSOLIDATED_BY_HWB" VARCHAR2(2000 BYTE), 
	"DOM" VARCHAR2(2000 BYTE), 
	"PACKING" VARCHAR2(2000 BYTE), 
	"PACKING_LIST" VARCHAR2(2000 BYTE), 
	"REMARK" VARCHAR2(2000 BYTE), 
	"PREPAID_OR_COLLECT" VARCHAR2(2000 BYTE), 
	"TYPE_OF_SHIPPING" VARCHAR2(2000 BYTE), 
	"LANDING_DATE" VARCHAR2(2000 BYTE)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
  
INSERT INTO WAREHOUSE_SHIP_REP
(ASOFDATE,
CARRIER,
CARRIER_SHIP_NO,
SAP_SHIPMENT_NO,
DELIVERY_NO,
TO_NO,
SO_NO,
SITE,
CUSTOMER_PO,
SHIP_TO_PARTY,
COUNTRY,
CITY,
PICKUP_DATE,
AWB,
PACKAGES,
ACTUAL_WEIGHT,
CHARGED_WEIGHT,
CONSOLIDATED_BY_HWB,
DOM,
PACKING,
REMARK,
PREPAID_OR_COLLECT,
TYPE_OF_SHIPPING,
LANDING_DATE,
PACKING_LIST
) VALUES (SYSDATE,
'CARRIER_1',
'CARRIER_SHIP_NO_1',
'SAP_SHIPMENT_NO_1',
'DELIVERY_NO_1',
'TO_NO_1',
'SO_NO_1',
'SITE_1',
'CUSTOMER_PO_1',
'SHIP_TO_PARTY_1',
'COUNTRY_1',
'CITY_1',
'PICKUP_DATE_1',
'AWB_1',
'PACKAGES_1',
'ACTUAL_WEIGHT_1',
'CHARGED_WEIGHT_1',
'CONSOLIDATED_BY_HWB_1',
'DOM_1',
'PACKING_1',
'REMARK_1',
'PREPAID_OR_COLLECT_1',
'TYPE_OF_SHIPPING_1',
'LANDING_DATE_1',
'PACKING_LIST_1'
)


INSERT INTO WAREHOUSE_SHIP_BILL
(ASOFDATE,
NO,
PICKUP_DATE,
CARRIER_SHIP_NO,
ORIGIN,
SHIP_TO_PROVINCE,
PACKAGES,
ACTUAL_WEIGHT,
CHARGED_WEIGHT,
FEE,
REMARK,
TYPE_OF_SHIPPING,
VECHICLE_MODEL,
VECHICLE_NUM,
SITE
) VALUES (SYSDATE,
'NO_1',
'PICKUP_DATE_1',
'CARRIER_SHIP_NO_1',
'ORIGIN_1',
'SHIP_TO_PROVINCE_1',
'PACKAGES_1',
'ACTUAL_WEIGHT_1',
'CHARGED_WEIGHT_1',
'FEE_1',
'REMARK_1',
'TYPE_OF_SHIPPING_1',
'VECHICLE_MODEL_1',
'VECHICLE_NUM_1',
'SITE_1'
)


---- Running Result

SELECT


FROM
(
SELECT
WSR.SHIP_SIGNAL,
WSR.CARRIER,
WSR.CARRIER_SHIP_NO,
WSR.SAP_SHIPMENT_NO,
WSR.DELIVERY_NO,
WSR.TO_NO,
WSR.SO_NO,
WSR.SITE,
WSR.CUSTOMER_PO,
WSR.SHIP_TO_PARTY,
WSR.COUNTRY,
WSR.CITY,
WSR.PICKUP_DATE,
WSR.AWB,
WSR.PACKAGES,
WSR.ACTUAL_WEIGHT,
WSR.CHARGED_WEIGHT,
WSR.CONSOLIDATED_BY_HWB,
WSR.DOM,
WSR.PACKING,
WSR.REMARK,
WSR.PREPAID_OR_COLLECT,
WSR.TYPE_OF_SHIPPING,
WSR.LANDING_DATE,
WSR.PACKING_LIST,
FEE.FEE_SIGNAL,
FEE.ID,
FEE.ORIGIN,
FEE.PROVINCE,
FEE.LEVEL1,
FEE.LEVEL2,
FEE.LEVEL3,
FEE.LEVEL4,
FEE.LEVEL5,
FEE.LEVEL6,
FEE.LEVEL7,
FEE.TRN_TYPE
FROM
(
SELECT
'Shanghai'||'_'||CITY||'_'||TYPE_OF_SHIPPING AS SHIP_SIGNAL,
CARRIER,
CARRIER_SHIP_NO,
SAP_SHIPMENT_NO,
DELIVERY_NO,
TO_NO,
SO_NO,
SITE,
CUSTOMER_PO,
SHIP_TO_PARTY,
COUNTRY,
CITY,
PICKUP_DATE,
AWB,
PACKAGES,
ACTUAL_WEIGHT,
CHARGED_WEIGHT,
CONSOLIDATED_BY_HWB,
DOM,
PACKING,
REMARK,
PREPAID_OR_COLLECT,
TYPE_OF_SHIPPING,
LANDING_DATE,
PACKING_LIST
FROM WAREHOUSE_SHIP_REP
)WSR
LEFT JOIN
(
SELECT
ID||'_'||TRN_TYPE AS FEE_SIGNAL,
ID,
ORIGIN,
PROVINCE,
LEVEL1,
LEVEL2,
LEVEL3,
LEVEL4,
LEVEL5,
LEVEL6,
LEVEL7,
TRN_TYPE
FROM VEHICLE_FEE
)FEE
ON FEE.FEE_SIGNAL = WSR.SHIP_SIGNAL
)WSRF
LEFT JOIN
(
SELECT
'Shanghai'||'_'||SHIP_TO_PROVINCE||'_'||TYPE_OF_SHIPPING AS SHIP_SIGNAL,
PICKUP_DATE,
CARRIER_SHIP_NO,
ORIGIN,
SHIP_TO_PROVINCE,
PACKAGES,
ACTUAL_WEIGHT,
CHARGED_WEIGHT,
FEE,
REMARK,
TYPE_OF_SHIPPING,
VECHICLE_MODEL,
VECHICLE_NUM,
SITE
FROM WAREHOUSE_SHIP_BILL
)WSB
ON WSB.ID = WSRF.ID;