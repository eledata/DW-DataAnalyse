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

  CREATE TABLE "SYSTEM"."VEHICLE_RATE" 
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
COUNTRY,
SHIP_TO_PROVINCE,
PICKUP_DATE,
ORIGIN,
PACKAGES,
ACTUAL_WEIGHT,
CHARGED_WEIGHT,
REMARK,
PREPAID_OR_COLLECT,
TYPE_OF_SHIPPING,
LANDING_DATE,
NUM_VMODE,
VEC_MODE
) VALUES (SYSDATE,
'CARRIER_1',
'CARRIER_SHIP_NO_1',
'SAP_SHIPMENT_NO_1',
'DELIVERY_NO_1',
'TO_NO_1',
'SO_NO_1',
'SITE_1',
'CUSTOMER_PO_1',
'COUNTRY_1',
'SHIP_TO_PROVINCE_1',
'PICKUP_DATE_1',
'ORIGIN_1',
'PACKAGES_1',
'ACTUAL_WEIGHT_1',
'CHARGED_WEIGHT_1',
'REMARK_1',
'PREPAID_OR_COLLECT_1',
'TYPE_OF_SHIPPING_1',
'LANDING_DATE_1',
'NUM_VMODE_1',
'VEC_MODE_1'
);


INSERT INTO WAREHOUSE_SHIP_BILL
(ASOFDATE,
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
SITE,
CARRIER
) VALUES (SYSDATE,
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
'SITE_1',
'CARRIER_1'
);

INSERT
INTO VEHICLE_RATE
  (
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
    TRN_TYPE,
    CARRIER
  )
  VALUES
  (
    'Shanghai_Shanghai_Parcel',
    'Shanghai_Shanghai',
    'Shanghai',
    'Shanghai',
    '25',
    '0.8',
    '0.8',
    '0.8',
    '0.8',
    '0.8',
    '0.8',
    'Parcel'
  )

---- Running Result


TRUNCATE TABLE WAREHOUSE_SHIP_REP;
TRUNCATE TABLE WAREHOUSE_SHIP_BILL;

SELECT COUNT(*) FROM WAREHOUSE_SHIP_REP;
SELECT COUNT(*) FROM WAREHOUSE_SHIP_BILL;

--REP
SELECT
WSB.CARRIER,
WSB.PICKUP_DATE,
WSB.CARRIER_SHIP_NO,
WSB.ORIGIN,
WSB.SHIP_TO_PROVINCE,
WSB.PACKAGES,
WSB.ACTUAL_WEIGHT,
WSB.CHARGED_WEIGHT,
WSB.FEE,
WSB.TYPE_OF_SHIPPING,
WSB.VECHICLE_MODEL,
WSB.VECHICLE_NUM,
WSB.SITE,
WSB.REMARK,
WSRF.CARRIER,
WSRF.CARRIER_SHIP_NO,
WSRF.SAP_SHIPMENT_NO,
WSRF.DELIVERY_NO,
WSRF.TO_NO,
WSRF.SO_NO,
WSRF.SITE,
WSRF.CUSTOMER_PO,
WSRF.ORIGIN,
WSRF.SHIP_TO_PROVINCE,
WSRF.PACKAGES,
WSRF.ACTUAL_WEIGHT,
WSRF.CHARGED_WEIGHT,
WSRF.PREPAID_OR_COLLECT,
WSRF.TYPE_OF_SHIPPING,
WSRF.VEC_MODE,
WSRF.NUM_VMODE,
WSRF.PICKUP_DATE,
WSRF.LANDING_DATE,
WSRF.COUNTRY,
WSRF.REMARK,
WSRF.LEVEL1,
WSRF.LEVEL2,
WSRF.LEVEL3,
WSRF.LEVEL4,
WSRF.LEVEL5,
WSRF.LEVEL6,
WSRF.LEVEL7
FROM
(
SELECT
CARRIER||'_'||CARRIER_SHIP_NO AS SHIP_SIG,
CARRIER,
PICKUP_DATE,
TO_NUMBER(CARRIER_SHIP_NO) AS CARRIER_SHIP_NO,
ORIGIN,
SHIP_TO_PROVINCE,
TO_NUMBER(PACKAGES) AS PACKAGES,
TO_NUMBER(ACTUAL_WEIGHT) AS ACTUAL_WEIGHT,
TO_NUMBER(CHARGED_WEIGHT) AS CHARGED_WEIGHT,
TO_NUMBER(FEE) AS FEE,
REMARK,
TYPE_OF_SHIPPING,
VECHICLE_MODEL,
TO_NUMBER(VECHICLE_NUM) AS VECHICLE_NUM,
SITE
FROM WAREHOUSE_SHIP_BILL
)WSB
LEFT JOIN
(
SELECT
WSR.CARRIER||'_'||WSR.CARRIER_SHIP_NO AS SHIP_SIG,
WSR.SHIP_SIGNAL,
WSR.CARRIER,
WSR.CARRIER_SHIP_NO,
WSR.SAP_SHIPMENT_NO,
WSR.DELIVERY_NO,
WSR.TO_NO,
WSR.SO_NO,
WSR.SITE,
WSR.CUSTOMER_PO,
WSR.ORIGIN,
WSR.SHIP_TO_PROVINCE,
WSR.PACKAGES,
WSR.ACTUAL_WEIGHT,
WSR.CHARGED_WEIGHT,
WSR.PREPAID_OR_COLLECT,
WSR.TYPE_OF_SHIPPING,
WSR.VEC_MODE,
WSR.NUM_VMODE,
WSR.PICKUP_DATE,
WSR.LANDING_DATE,
WSR.COUNTRY,
WSR.REMARK,
FEE.LEVEL1,
FEE.LEVEL2,
FEE.LEVEL3,
FEE.LEVEL4,
FEE.LEVEL5,
FEE.LEVEL6,
FEE.LEVEL7
FROM
(
SELECT
CARRIER||'_'||ORIGIN||'_'||SHIP_TO_PROVINCE||'_'||TYPE_OF_SHIPPING AS SHIP_SIGNAL,
CARRIER,
TO_NUMBER(CARRIER_SHIP_NO) AS CARRIER_SHIP_NO,
SAP_SHIPMENT_NO,
DELIVERY_NO,
TO_NO,
SO_NO,
SITE,
CUSTOMER_PO,
COUNTRY,
SHIP_TO_PROVINCE,
PICKUP_DATE,
ORIGIN,
TO_NUMBER(PACKAGES) AS PACKAGES,
TO_NUMBER(ACTUAL_WEIGHT) AS ACTUAL_WEIGHT,
TO_NUMBER(CHARGED_WEIGHT) AS CHARGED_WEIGHT,
REMARK,
PREPAID_OR_COLLECT,
TYPE_OF_SHIPPING,
LANDING_DATE,
TO_NUMBER(NUM_VMODE) AS NUM_VMODE,
VEC_MODE
FROM WAREHOUSE_SHIP_REP
)WSR
LEFT JOIN
(
SELECT
CARRIER||'_'||ID||'_'||TRN_TYPE AS FEE_SIGNAL,
TO_NUMBER(LEVEL1) AS LEVEL1,
TO_NUMBER(LEVEL2) AS LEVEL2,
TO_NUMBER(LEVEL3) AS LEVEL3,
TO_NUMBER(LEVEL4) AS LEVEL4,
TO_NUMBER(LEVEL5) AS LEVEL5,
TO_NUMBER(LEVEL6) AS LEVEL6,
TO_NUMBER(LEVEL7) AS LEVEL7
FROM VEHICLE_RATE
)FEE
ON FEE.FEE_SIGNAL = WSR.SHIP_SIGNAL
)WSRF
ON WSRF.SHIP_SIG = WSB.SHIP_SIG
