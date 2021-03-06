---To Create a useful tools for analyse pre-demand and supplier capacity.
---All plants
--'5041', '5051', '5101', '5111', '5121', '5161', '5191', '5201','5071','5141'
--'||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||'
--'5041', '5051', '5101', '5111', '5121', '5161', '5191', '5201','5071','5141'
--'5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140'

---INI SETUP
DROP TABLE INV_SAP_MATERIAL_CATALOG;
CREATE TABLE INV_SAP_MATERIAL_CATALOG AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATERIAL_CATALOG@ROCKWELL_DW_DBLINK;

DROP TABLE INV_SAP_SHIP_SOLD_TO_PARTYID;
CREATE TABLE INV_SAP_SHIP_SOLD_TO_PARTYID AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_DW_DBLINK;

DROP TABLE INV_SAP_PP_MVKE;
CREATE TABLE INV_SAP_PP_MVKE AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DW_DBLINK;

Drop table INV_SAP_PP_OPTIMIZATION;
CREATE TABLE INV_SAP_PP_OPTIMIZATION AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK;

Drop table INV_SAP_SALES_VBAK_VBAP_VBUP;
CREATE TABLE INV_SAP_SALES_VBAK_VBAP_VBUP AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DBLINK WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DROP TABLE INV_SAP_PP_FRCST_PBIM_PBED;
CREATE TABLE INV_SAP_PP_FRCST_PBIM_PBED AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DROP TABLE INV_SAP_SALES_HST;
CREATE TABLE INV_SAP_SALES_HST AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DROP TABLE INV_SAP_PP_PO_HISTORY;
CREATE TABLE INV_SAP_PP_PO_HISTORY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DROP TABLE INV_SAP_LIKP_LIPS_DAILY;
CREATE TABLE INV_SAP_LIKP_LIPS_DAILY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DROP TABLE INV_SAP_INVENTORY_BY_PLANT;
CREATE TABLE INV_SAP_INVENTORY_BY_PLANT AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DROP TABLE INV_SAP_PP_PARAM;
CREATE TABLE INV_SAP_PP_PARAM AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PARAM@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');


DROP TABLE INV_SAP_INVENTORY_WKS;
CREATE TABLE INV_SAP_INVENTORY_WKS AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_WKS@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');


--DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_IO_INPUTS_DAILY;
CREATE TABLE INV_SAP_IO_INPUTS_DAILY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_IO_INPUTS_DAILY@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');


--Weekly
--Catalog#
DECLARE
  CREATE_TABLE_INV_SAP_MATERIAL_CATALOG        VARCHAR2(1000);
  DROP_TABLE_INV_SAP_MATERIAL_CATALOG              VARCHAR2(1000);
BEGIN
  DROP_TABLE_INV_SAP_MATERIAL_CATALOG:= 'DROP TABLE INV_SAP_MATERIAL_CATALOG';
  CREATE_TABLE_INV_SAP_MATERIAL_CATALOG       := 'CREATE TABLE INV_SAP_MATERIAL_CATALOG AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATERIAL_CATALOG@ROCKWELL_DW_DBLINK';
  EXECUTE IMMEDIATE DROP_TABLE;
  EXECUTE IMMEDIATE STR_CREATE_TABLE;
END;

--DWQ$LIBRARIAN.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_DW_DBLINK
DECLARE
  CREATE_TABLE_SOLD_TO_PARTYID VARCHAR2(1000);
  DROP_TABLE_SOLD_TO_PARTYID   VARCHAR2(1000);
BEGIN
  DROP_TABLE_SOLD_TO_PARTYID   := 'DROP TABLE INV_SAP_SHIP_SOLD_TO_PARTYID';
  CREATE_TABLE_SOLD_TO_PARTYID := 'CREATE TABLE INV_SAP_SHIP_SOLD_TO_PARTYID AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_DW_DBLINK';
  EXECUTE IMMEDIATE DROP_TABLE_SOLD_TO_PARTYID;
  EXECUTE IMMEDIATE CREATE_TABLE_SOLD_TO_PARTYID;
END;

--DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_PP_MVKE;
CREATE TABLE INV_SAP_PP_MVKE AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DW_DBLINK;

DECLARE
  DROP_TABLE_INV_SAP_PP_MVKE   VARCHAR2(1000);
  CREATE_TABLE_INV_SAP_PP_MVKE VARCHAR2(1000);
BEGIN
  DROP_TABLE_INV_SAP_PP_MVKE   := 'DROP TABLE INV_SAP_PP_MVKE';
  CREATE_TABLE_INV_SAP_PP_MVKE := 'CREATE TABLE INV_SAP_PP_MVKE AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DW_DBLINK';
  EXECUTE IMMEDIATE DROP_TABLE_INV_SAP_PP_MVKE;
  EXECUTE IMMEDIATE CREATE_TABLE_INV_SAP_PP_MVKE;
END;

--Basic Tables Update Every day
 
--DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK
Drop table INV_SAP_PP_OPTIMIZATION;
CREATE TABLE INV_SAP_PP_OPTIMIZATION AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK; 

DECLARE
  DROP_TABLE_PP_OPTIMIZATION   VARCHAR2(1000);
  CREATE_TABLE_PP_OPTIMIZATION VARCHAR2(1000);
BEGIN
  DROP_TABLE_PP_OPTIMIZATION   := 'DROP TABLE INV_SAP_PP_OPTIMIZATION';
  CREATE_TABLE_PP_OPTIMIZATION := 'CREATE TABLE INV_SAP_PP_OPTIMIZATION AS
  SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK';
  EXECUTE IMMEDIATE DROP_TABLE_PP_OPTIMIZATION;
  EXECUTE IMMEDIATE CREATE_TABLE_PP_OPTIMIZATION;
END;

--ALL
SELECT COUNT(*) FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK;
DECLARE
  DROP_TABLE_PP_OPTIMIZATION   VARCHAR2(1000);
  CREATE_TABLE_PP_OPTIMIZATION VARCHAR2(1000);
BEGIN
  DROP_TABLE_PP_OPTIMIZATION   := 'DROP TABLE INV_SAP_PP_OPTIMIZATION';
  CREATE_TABLE_PP_OPTIMIZATION := 'CREATE TABLE INV_SAP_PP_OPTIMIZATION AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK';
  EXECUTE IMMEDIATE DROP_TABLE_PP_OPTIMIZATION;
  EXECUTE IMMEDIATE CREATE_TABLE_PP_OPTIMIZATION;
END;

--DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DW_DBLINK
--Select using salesorg.
Drop table INV_SAP_SALES_VBAK_VBAP_VBUP;
CREATE TABLE INV_SAP_SALES_VBAK_VBAP_VBUP AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DW_DBLINK WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DECLARE
  DROP_TABLE_OPEN_SALES   VARCHAR2(1000);
  CREATE_TABLE_OPEN_SALES  VARCHAR2(1000);
BEGIN
  DROP_TABLE_OPEN_SALES   := 'Drop table INV_SAP_SALES_VBAK_VBAP_VBUP';
  CREATE_TABLE_OPEN_SALES := 'CREATE TABLE INV_SAP_SALES_VBAK_VBAP_VBUP AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DW_DBLINK WHERE PLANT IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
  EXECUTE IMMEDIATE DROP_TABLE_OPEN_SALES;
  EXECUTE IMMEDIATE CREATE_TABLE_OPEN_SALES;
END;

--DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_PP_FRCST_PBIM_PBED;
CREATE TABLE INV_SAP_PP_FRCST_PBIM_PBED AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DECLARE
  DROP_TABLE_OPEN_FC   VARCHAR2(1000);
  CREATE_TABLE_OPEN_FC  VARCHAR2(1000);
BEGIN
  DROP_TABLE_OPEN_FC   := 'Drop table INV_SAP_PP_FRCST_PBIM_PBED';
  CREATE_TABLE_OPEN_FC := 'CREATE TABLE INV_SAP_PP_FRCST_PBIM_PBED AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
  EXECUTE IMMEDIATE DROP_TABLE_OPEN_FC;
  EXECUTE IMMEDIATE CREATE_TABLE_OPEN_FC;
END;


--DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_SALES_HST;
CREATE TABLE INV_SAP_SALES_HST AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DECLARE
  DROP_TABLE_SALES_HST  VARCHAR2(1000);
  CREATE_TABLE_SALES_HST  VARCHAR2(1000);
BEGIN
  DROP_TABLE_SALES_HST   := 'Drop table INV_SAP_SALES_HST';
  CREATE_TABLE_SALES_HST := 'CREATE TABLE INV_SAP_SALES_HST AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
  EXECUTE IMMEDIATE DROP_TABLE_SALES_HST;
  EXECUTE IMMEDIATE CREATE_TABLE_SALES_HST;
END;


--DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_PP_PO_HISTORY;
CREATE TABLE INV_SAP_PP_PO_HISTORY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DECLARE
  DROP_TABLE_PP_PO_HISTORY  VARCHAR2(1000);
  CREATE_TABLE_PP_PO_HISTORY  VARCHAR2(1000);
BEGIN
  DROP_TABLE_PP_PO_HISTORY   := 'Drop table INV_SAP_PP_PO_HISTORY';
  CREATE_TABLE_PP_PO_HISTORY := 'CREATE TABLE INV_SAP_PP_PO_HISTORY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
  EXECUTE IMMEDIATE DROP_TABLE_PP_PO_HISTORY;
  EXECUTE IMMEDIATE CREATE_TABLE_PP_PO_HISTORY;
END;

--DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_LIKP_LIPS_DAILY; 
CREATE TABLE INV_SAP_LIKP_LIPS_DAILY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DW_DBLINK WHERE CREATED_ON_DATE > SYSDATE - 91;

DECLARE
  DROP_TABLE_DELIVERY_REC  VARCHAR2(1000);
  CREATE_TABLE_DELIVERY_REC  VARCHAR2(1000);
BEGIN
  DROP_TABLE_DELIVERY_REC   := 'Drop table INV_SAP_LIKP_LIPS_DAILY';
  CREATE_TABLE_DELIVERY_REC := 'CREATE TABLE INV_SAP_LIKP_LIPS_DAILY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DW_DBLINK WHERE CREATED_ON_DATE > SYSDATE - 91';
  EXECUTE IMMEDIATE DROP_TABLE_DELIVERY_REC;
  EXECUTE IMMEDIATE CREATE_TABLE_DELIVERY_REC;
END;

--DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_INVENTORY_BY_PLANT;
CREATE TABLE INV_SAP_INVENTORY_BY_PLANT AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

DECLARE
  DROP_TABLE_INVENTORY_REC  VARCHAR2(1000);
  CREATE_TABLE_INVENTORY_REC  VARCHAR2(1000);
BEGIN
  DROP_TABLE_INVENTORY_REC   := 'Drop table INV_SAP_INVENTORY_BY_PLANT';
  CREATE_TABLE_INVENTORY_REC := 'CREATE TABLE INV_SAP_INVENTORY_BY_PLANT AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
  EXECUTE IMMEDIATE DROP_TABLE_INVENTORY_REC;
  EXECUTE IMMEDIATE CREATE_TABLE_INVENTORY_REC;
END;


--DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK
DROP TABLE INV_SAP_IO_INPUTS_DAILY;
CREATE TABLE INV_SAP_IO_INPUTS_DAILY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_IO_INPUTS_DAILY@ROCKWELL_DW_DBLINK;

DECLARE
  DROP_TABLE_IO_REC  VARCHAR2(1000);
  CREATE_TABLE_IO_REC  VARCHAR2(1000);
BEGIN
  DROP_TABLE_IO_REC   := 'Drop table INV_SAP_IO_INPUTS_DAILY';
  CREATE_TABLE_IO_REC := 'CREATE TABLE INV_SAP_IO_INPUTS_DAILY AS
SELECT * FROM DWQ$LIBRARIAN.INV_SAP_IO_INPUTS_DAILY@ROCKWELL_DW_DBLINK';
  EXECUTE IMMEDIATE DROP_TABLE_IO_REC;
  EXECUTE IMMEDIATE CREATE_TABLE_IO_REC;
END;

--FUNCTION COLLECTIONS
-----Catalog Table Remove Dash
SELECT * FROM INV_SAP_NODASH_MAT_CATA;

DECLARE
  DROP_TABLE_CATANODSH   VARCHAR2(1000);
  CREATE_TABLE_CATANODSH VARCHAR2(1000);
BEGIN
  DROP_TABLE_CATANODSH   := 'DROP TABLE INV_SAP_NODASH_MAT_CATA';
  CREATE_TABLE_CATANODSH := 'CREATE TABLE INV_SAP_NODASH_MAT_CATA AS SELECT * FROM INV_SAP_MATERIAL_CATALOG';
  EXECUTE IMMEDIATE DROP_TABLE_INVENTORY_REC;
  EXECUTE IMMEDIATE CREATE_TABLE_CATANODSH;
END;
  
DECLARE
  CURSOR cur
  IS
    SELECT a.CATALOG_STRING1,
      b.ROWID ROW_ID
    FROM INV_SAP_MATERIAL_CATALOG a,
      INV_SAP_NODASH_MAT_CATA b
    WHERE a.MATERIALID = b.MATERIALID
    ORDER BY b.ROWID; ---order by rowid
  V_COUNTER NUMBER;
BEGIN
  V_COUNTER := 0;
  FOR row IN cur
  LOOP
    UPDATE INV_SAP_NODASH_MAT_CATA SET CATALOG_STRING2 = REPLACE(row.CATALOG_STRING1, '-') WHERE ROWID = row.ROW_ID;
    V_COUNTER     := V_COUNTER + 1;
    IF (V_COUNTER >= 1000) THEN
      COMMIT;
      V_COUNTER := 0;
    END IF;
  END LOOP;
  COMMIT;
END;


----------------------------------------------------VIEWS---------------------------------------------------------------
--VIEW COLLECTIONS
----X CURRENT ITEM VIEW
DROP VIEW VIEW_INV_SAP_ITEM_X;
SELECT * FROM VIEW_INV_SAP_ITEM_X;
CREATE VIEW VIEW_INV_SAP_ITEM_X AS 
SELECT DISTINCT * FROM
(SELECT ISPM.ID           AS ID,
  ISPM.DIRECT_SHIP_PLANT AS PLANT,
  ISPM.MATERIALID        AS MATERIAL,
  ISMC.CATALOG_STRING1   AS CATALOG_DASH,
  ISMC.CATALOG_STRING2   AS CATALOG_NO_DASH,
  ISPM.DIST_CHL          AS DIST_CHL,
  ISPM.CURRENT_SERIES    AS CURRENT_SERIES
FROM
  (SELECT MATERIALID
    ||'_'
    ||DIRECT_SHIP_PLANT AS ID,
    MATERIALID,
    DIST_CHL,
    DIRECT_SHIP_PLANT,
    CURRENT_SERIES
  FROM INV_SAP_PP_MVKE
  WHERE CURRENT_SERIES = 'X'
  )ISPM
LEFT JOIN
  (SELECT CATALOG_STRING1,
    CATALOG_STRING2,
    MATERIALID
  FROM INV_SAP_NODASH_MAT_CATA
  )ISMC
ON ISPM.MATERIALID = ISMC.MATERIALID); --WHERE ID = '80026-146-56-R_5200';

--ITEM ALL
CREATE VIEW VIEW_INV_SAP_ITEM_ALL AS 
SELECT ISPM.ID           AS ID,
  ISPM.DIRECT_SHIP_PLANT AS PLANT,
  ISPM.MATERIALID        AS MATERIAL,
  ISMC.CATALOG_STRING1   AS CATALOG_DASH,
  ISMC.CATALOG_STRING2   AS CATALOG_NO_DASH,
  ISPM.SALES_ORG         AS SALES_ORG,
  ISPM.DIST_CHL          AS DIST_CHL,
  ISPM.D_CHAIN_BLK       AS D_CHAIN_BLK,
  ISPM.VALID_FROM_DATE   AS VALID_FROM_DATE,
  ISPM.STOCK_STATUS      AS STOCK_STATUS,
  ISPM.CURRENT_SERIES    AS CURRENT_SERIES
FROM
  (SELECT MATERIALID
    ||'_'
    ||DIRECT_SHIP_PLANT AS ID,
    MATERIALID,
    SALES_ORG,
    DIST_CHL,
    D_CHAIN_BLK,
    VALID_FROM_DATE,
    DIRECT_SHIP_PLANT,
    STOCK_STATUS,
    CURRENT_SERIES
  FROM INV_SAP_PP_MVKE
  )ISPM
LEFT JOIN
  (SELECT CATALOG_STRING1,
    CATALOG_STRING2,
    MATERIALID
  FROM INV_SAP_NODASH_MAT_CATA
  )ISMC
ON ISPM.MATERIALID = ISMC.MATERIALID;

----PP TABLE BASIC_INFO
--CURRENT ITEM
DROP VIEW VIEW_INV_SAP_PP_OPT_X;
SELECT * FROM VIEW_INV_SAP_PP_OPT_X WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');
CREATE VIEW VIEW_INV_SAP_PP_OPT_X AS
SELECT *
FROM
  (SELECT PP_BASIC.ID,
    PP_BASIC.LAST_REVIEW,
    PP_BASIC.MATERIAL,
    ITEM_X.CATALOG_DASH,
    ITEM_X.CATALOG_NO_DASH,
    PP_BASIC.PLANT,
    PP_BASIC.MAT_DESC,
    PP_BASIC.SAFETY_STOCK,
    PP_BASIC.OH_QTY,
    PP_BASIC.UNIT,
    PP_BASIC.UNIT_COST,
    PP_BASIC.OH_QTY_INTRANSIT,
    PP_BASIC.PROD_BU,
    PP_BASIC.PROD_FAM,
    PP_BASIC.PROC_TYPE,
    PP_BASIC.STRATEGY_GRP,
    PP_BASIC.MRP_TYPE,
    PP_BASIC.PROD_SCHEDULER,
    PP_BASIC.PLANT_SP_MATL_STA,
    PP_BASIC.VENDOR_KEY,
    PP_BASIC.VENDOR_ITEM,
    PP_BASIC.MIN_INV,
    PP_BASIC.TARGET_INV,
    PP_BASIC.MAX_INV,
    PP_BASIC.LOT_SIZE_QTY,
    PP_BASIC.LOT_ROUNDING_VALUE,
    PP_BASIC.LOT_SIZE_DISLS,
    PP_BASIC.AVG26_USAGE_QTY,
    PP_BASIC.STDEV26_USAGE,
    PP_BASIC.AVG13_USAGE_QTY,
    PP_BASIC.STDEV13_USAGE,
    PP_BASIC.Q1_LINES,
    PP_BASIC.Q2_LINES,
    PP_BASIC.Q3_LINES,
    PP_BASIC.Q4_LINES,
    PP_BASIC.Q1_FREQ_COUNT,
    PP_BASIC.Q2_FREQ_COUNT,
    PP_BASIC.Q3_FREQ_COUNT,
    PP_BASIC.Q4_FREQ_COUNT,
    PP_BASIC.EXCHANGE_RATE,
    PP_BASIC.LEVEL_TYPE,
    PP_BASIC.OUT_QTY_W01,
    PP_BASIC.OUT_QTY_W02,
    PP_BASIC.OUT_QTY_W03,
    PP_BASIC.OUT_QTY_W04,
    PP_BASIC.OUT_QTY_W05,
    PP_BASIC.OUT_QTY_W06,
    PP_BASIC.OUT_QTY_W07,
    PP_BASIC.OUT_QTY_W08,
    PP_BASIC.OUT_QTY_W09,
    PP_BASIC.OUT_QTY_W10,
    PP_BASIC.OUT_QTY_W11,
    PP_BASIC.OUT_QTY_W12,
    PP_BASIC.OUT_QTY_W13,
    PP_BASIC.OUT_QTY_W14,
    PP_BASIC.OUT_QTY_W15,
    PP_BASIC.OUT_QTY_W16,
    PP_BASIC.OUT_QTY_W17,
    PP_BASIC.OUT_QTY_W18,
    PP_BASIC.OUT_QTY_W19,
    PP_BASIC.OUT_QTY_W20,
    PP_BASIC.OUT_QTY_W21,
    PP_BASIC.OUT_QTY_W22,
    PP_BASIC.OUT_QTY_W23,
    PP_BASIC.OUT_QTY_W24,
    PP_BASIC.OUT_QTY_W25,
    PP_BASIC.OUT_QTY_W26,
    PP_BASIC.IN_QTY_W01,
    PP_BASIC.IN_QTY_W02,
    PP_BASIC.IN_QTY_W03,
    PP_BASIC.IN_QTY_W04,
    PP_BASIC.IN_QTY_W05,
    PP_BASIC.IN_QTY_W06,
    PP_BASIC.IN_QTY_W07,
    PP_BASIC.IN_QTY_W08,
    PP_BASIC.IN_QTY_W09,
    PP_BASIC.IN_QTY_W10,
    PP_BASIC.IN_QTY_W11,
    PP_BASIC.IN_QTY_W12,
    PP_BASIC.IN_QTY_W13,
    PP_BASIC.IN_QTY_W14,
    PP_BASIC.IN_QTY_W15,
    PP_BASIC.IN_QTY_W16,
    PP_BASIC.IN_QTY_W17,
    PP_BASIC.IN_QTY_W18,
    PP_BASIC.IN_QTY_W19,
    PP_BASIC.IN_QTY_W20,
    PP_BASIC.IN_QTY_W21,
    PP_BASIC.IN_QTY_W22,
    PP_BASIC.IN_QTY_W23,
    PP_BASIC.IN_QTY_W24,
    PP_BASIC.IN_QTY_W25,
    PP_BASIC.IN_QTY_W26,
    PP_BASIC.MATERIAL_LEVEL_VALUE,
    PP_BASIC.MRP_CONTROLLER,
    PP_BASIC.MRP_CONTROLLER_KEY,
    PP_BASIC.PURCH_GROUP,
    PP_BASIC.PURCH_GROUP_KEY,
    PP_BASIC.PROD_SCHED_KEY,
    PP_BASIC.RECORDER_POINT,
    PP_BASIC.LEAD_TIME,
    PP_BASIC.PDT,
    PP_BASIC.GRT,
    ITEM_X.DIST_CHL,
    PP_BASIC.ISSUE_UOM_NUMERATOR,
    PP_BASIC.PO_UOM_NUMERATOR,
    PP_BASIC.MATL_TYPE,
    ITEM_X.CURRENT_SERIES,
    PP_BASIC.ULTIMATE_SOURCE
  FROM
    (SELECT MATERIALID
      ||'_'
      ||(PLANTID - 1) AS ID,
      LAST_REVIEW,
      MATERIALID    AS MATERIAL,
      (PLANTID - 1) AS PLANT,
      MAT_DESC,
      SAFETY_STK AS SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      OH_QTY_INTRANSIT,
      SUBSTR(PROD_BU,0,3) AS PROD_BU,
      PROD_FAM,
      PROC_TYPE,
      STRATEGY_GRP,
      MRP_TYPE,
      PROD_SCHEDULER,
      SP_MATL_STAT_MMSTA AS PLANT_SP_MATL_STA,
      SPC_PROC_KEY_SOBSL AS VENDOR_KEY,
      MATERIALID
      ||'_'
      ||SUBSTR(VENDOR_NAME,0,4)           AS VENDOR_ITEM,
      (SAFETY_STK)                        AS MIN_INV,
      CEIL(SAFETY_STK + 0.5*LOT_SIZE_QTY) AS TARGET_INV,
      CEIL(SAFETY_STK + 1.2*LOT_SIZE_QTY) AS MAX_INV,
      LOT_SIZE_QTY,
      LOT_ROUNDING_VALUE,
      LOT_SIZE_DISLS,
      AVG26_USAGE_QTY,
      STDEV26_USAGE,
      AVG13_USAGE_QTY,
      STDEV13_USAGE,
      Q1_LINES,
      Q2_LINES,
      Q3_LINES,
      Q4_LINES,
      Q1_FREQ_COUNT,
      Q2_FREQ_COUNT,
      Q3_FREQ_COUNT,
      Q4_FREQ_COUNT,
      EXCHANGE_RATE,
      LEVEL_TYPE,
      OUT_QTY_W01,
      OUT_QTY_W02,
      OUT_QTY_W03,
      OUT_QTY_W04,
      OUT_QTY_W05,
      OUT_QTY_W06,
      OUT_QTY_W07,
      OUT_QTY_W08,
      OUT_QTY_W09,
      OUT_QTY_W10,
      OUT_QTY_W11,
      OUT_QTY_W12,
      OUT_QTY_W13,
      OUT_QTY_W14,
      OUT_QTY_W15,
      OUT_QTY_W16,
      OUT_QTY_W17,
      OUT_QTY_W18,
      OUT_QTY_W19,
      OUT_QTY_W20,
      OUT_QTY_W21,
      OUT_QTY_W22,
      OUT_QTY_W23,
      OUT_QTY_W24,
      OUT_QTY_W25,
      OUT_QTY_W26,
      IN_QTY_W01,
      IN_QTY_W02,
      IN_QTY_W03,
      IN_QTY_W04,
      IN_QTY_W05,
      IN_QTY_W06,
      IN_QTY_W07,
      IN_QTY_W08,
      IN_QTY_W09,
      IN_QTY_W10,
      IN_QTY_W11,
      IN_QTY_W12,
      IN_QTY_W13,
      IN_QTY_W14,
      IN_QTY_W15,
      IN_QTY_W16,
      IN_QTY_W17,
      IN_QTY_W18,
      IN_QTY_W19,
      IN_QTY_W20,
      IN_QTY_W21,
      IN_QTY_W22,
      IN_QTY_W23,
      IN_QTY_W24,
      IN_QTY_W25,
      IN_QTY_W26,
      MATERIAL_LEVEL_VALUE,
      MRP_CONTROLLER,
      MRP_CONTROLLER_DISPO AS MRP_CONTROLLER_KEY,
      PURCH_GROUP,
      PURCH_GROUP_EKGRP AS PURCH_GROUP_KEY,
      PROD_SCHED_FEVOR  AS PROD_SCHED_KEY,
      REORDER_PT        AS RECORDER_POINT,
      CEIL(1.4*GRT + PDT)   AS LEAD_TIME,  --Lead Time Change!!
      PDT AS PDT,
      GRT AS GRT,
      MEINS_ISSUE_UOM   AS UNIT,
      ISSUE_UOM_NUMERATOR,
      PO_UOM_NUMERATOR,
      MATL_TYPE_MTART AS MATL_TYPE,
      ULTIMATE_SOURCE
    FROM INV_SAP_PP_OPTIMIZATION --WHERE MATERIALID = '100-C09KJ400 A' AND PLANTID = '5200'
    )PP_BASIC
  LEFT JOIN
    (SELECT ID,
      MATERIAL,
      CATALOG_DASH,
      CATALOG_NO_DASH,
      DIST_CHL,
      CURRENT_SERIES
    FROM VIEW_INV_SAP_ITEM_X --WHERE MATERIAL = '100-C09KJ400 A' AND PLANTID = '5200'
    )ITEM_X
  ON ITEM_X.ID = PP_BASIC.ID
  )
WHERE CURRENT_SERIES = 'X';

SELECT * FROM 
--VIEW_INV_SAP_PP_OPT_ALL INCLUDE NON-CURRENT ITEM, LOOK LIKE UNDER DEV ETC.
DROP VIEW VIEW_INV_SAP_PP_OPT_ALL;
VIEW_INV_SAP_PP_OPT_ALL 
		DROP VIEW VIEW_INV_SAP_PP_OPT_ALL;
		SELECT * FROM VIEW_INV_SAP_PP_OPT_ALL WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');
		CREATE VIEW VIEW_INV_SAP_PP_OPT_ALL AS
		SELECT *
		FROM
		  (SELECT PP_BASIC.ID,
			PP_BASIC.LAST_REVIEW,
			PP_BASIC.MATERIAL,
			ITEM_X.CATALOG_DASH,
			ITEM_X.CATALOG_NO_DASH,
			PP_BASIC.PLANT,
			PP_BASIC.MAT_DESC,
			PP_BASIC.SAFETY_STOCK,
			PP_BASIC.OH_QTY,
			PP_BASIC.UNIT,
			PP_BASIC.UNIT_COST,
			PP_BASIC.OH_QTY_INTRANSIT,
			PP_BASIC.PROD_BU,
			PP_BASIC.PROD_FAM,
			PP_BASIC.PROC_TYPE,
			PP_BASIC.STRATEGY_GRP,
			PP_BASIC.MRP_TYPE,
			PP_BASIC.PROD_SCHEDULER,
			PP_BASIC.PLANT_SP_MATL_STA,
			PP_BASIC.VENDOR_KEY,
			PP_BASIC.VENDOR_ITEM,
			PP_BASIC.MIN_INV,
			PP_BASIC.TARGET_INV,
			PP_BASIC.MAX_INV,
			PP_BASIC.LOT_SIZE_QTY,
			PP_BASIC.LOT_ROUNDING_VALUE,
			PP_BASIC.LOT_SIZE_DISLS,
			PP_BASIC.AVG26_USAGE_QTY,
			PP_BASIC.STDEV26_USAGE,
			PP_BASIC.AVG13_USAGE_QTY,
			PP_BASIC.STDEV13_USAGE,
			PP_BASIC.Q1_LINES,
			PP_BASIC.Q2_LINES,
			PP_BASIC.Q3_LINES,
			PP_BASIC.Q4_LINES,
			PP_BASIC.Q1_FREQ_COUNT,
			PP_BASIC.Q2_FREQ_COUNT,
			PP_BASIC.Q3_FREQ_COUNT,
			PP_BASIC.Q4_FREQ_COUNT,
			PP_BASIC.EXCHANGE_RATE,
			PP_BASIC.LEVEL_TYPE,
			PP_BASIC.OUT_QTY_W01,
			PP_BASIC.OUT_QTY_W02,
			PP_BASIC.OUT_QTY_W03,
			PP_BASIC.OUT_QTY_W04,
			PP_BASIC.OUT_QTY_W05,
			PP_BASIC.OUT_QTY_W06,
			PP_BASIC.OUT_QTY_W07,
			PP_BASIC.OUT_QTY_W08,
			PP_BASIC.OUT_QTY_W09,
			PP_BASIC.OUT_QTY_W10,
			PP_BASIC.OUT_QTY_W11,
			PP_BASIC.OUT_QTY_W12,
			PP_BASIC.OUT_QTY_W13,
			PP_BASIC.OUT_QTY_W14,
			PP_BASIC.OUT_QTY_W15,
			PP_BASIC.OUT_QTY_W16,
			PP_BASIC.OUT_QTY_W17,
			PP_BASIC.OUT_QTY_W18,
			PP_BASIC.OUT_QTY_W19,
			PP_BASIC.OUT_QTY_W20,
			PP_BASIC.OUT_QTY_W21,
			PP_BASIC.OUT_QTY_W22,
			PP_BASIC.OUT_QTY_W23,
			PP_BASIC.OUT_QTY_W24,
			PP_BASIC.OUT_QTY_W25,
			PP_BASIC.OUT_QTY_W26,
			PP_BASIC.IN_QTY_W01,
			PP_BASIC.IN_QTY_W02,
			PP_BASIC.IN_QTY_W03,
			PP_BASIC.IN_QTY_W04,
			PP_BASIC.IN_QTY_W05,
			PP_BASIC.IN_QTY_W06,
			PP_BASIC.IN_QTY_W07,
			PP_BASIC.IN_QTY_W08,
			PP_BASIC.IN_QTY_W09,
			PP_BASIC.IN_QTY_W10,
			PP_BASIC.IN_QTY_W11,
			PP_BASIC.IN_QTY_W12,
			PP_BASIC.IN_QTY_W13,
			PP_BASIC.IN_QTY_W14,
			PP_BASIC.IN_QTY_W15,
			PP_BASIC.IN_QTY_W16,
			PP_BASIC.IN_QTY_W17,
			PP_BASIC.IN_QTY_W18,
			PP_BASIC.IN_QTY_W19,
			PP_BASIC.IN_QTY_W20,
			PP_BASIC.IN_QTY_W21,
			PP_BASIC.IN_QTY_W22,
			PP_BASIC.IN_QTY_W23,
			PP_BASIC.IN_QTY_W24,
			PP_BASIC.IN_QTY_W25,
			PP_BASIC.IN_QTY_W26,
			PP_BASIC.MATERIAL_LEVEL_VALUE,
			PP_BASIC.MRP_CONTROLLER,
			PP_BASIC.MRP_CONTROLLER_KEY,
			PP_BASIC.PURCH_GROUP,
			PP_BASIC.PURCH_GROUP_KEY,
			PP_BASIC.PROD_SCHED_KEY,
			PP_BASIC.RECORDER_POINT,
			PP_BASIC.LEAD_TIME,
			PP_BASIC.PDT,
			PP_BASIC.GRT,
			ITEM_X.DIST_CHL,
			PP_BASIC.ISSUE_UOM_NUMERATOR,
			PP_BASIC.PO_UOM_NUMERATOR,
			PP_BASIC.MATL_TYPE,
			ITEM_X.CURRENT_SERIES,
			PP_BASIC.ULTIMATE_SOURCE
		  FROM
			(SELECT MATERIALID
			  ||'_'
			  ||(PLANTID - 1) AS ID,
			  LAST_REVIEW,
			  MATERIALID    AS MATERIAL,
			  (PLANTID - 1) AS PLANT,
			  MAT_DESC,
			  SAFETY_STK AS SAFETY_STOCK,
			  OH_QTY,
			  UNIT_COST,
			  OH_QTY_INTRANSIT,
			  SUBSTR(PROD_BU,0,3) AS PROD_BU,
			  PROD_FAM,
			  PROC_TYPE,
			  STRATEGY_GRP,
			  MRP_TYPE,
			  PROD_SCHEDULER,
			  SP_MATL_STAT_MMSTA AS PLANT_SP_MATL_STA,
			  SPC_PROC_KEY_SOBSL AS VENDOR_KEY,
			  MATERIALID
			  ||'_'
			  ||SUBSTR(VENDOR_NAME,0,4)           AS VENDOR_ITEM,
			  (SAFETY_STK)                        AS MIN_INV,
			  CEIL(SAFETY_STK + 0.5*LOT_SIZE_QTY) AS TARGET_INV,
			  CEIL(SAFETY_STK + 1.2*LOT_SIZE_QTY) AS MAX_INV,
			  LOT_SIZE_QTY,
			  LOT_ROUNDING_VALUE,
			  LOT_SIZE_DISLS,
			  AVG26_USAGE_QTY,
			  STDEV26_USAGE,
			  AVG13_USAGE_QTY,
			  STDEV13_USAGE,
			  Q1_LINES,
			  Q2_LINES,
			  Q3_LINES,
			  Q4_LINES,
			  Q1_FREQ_COUNT,
			  Q2_FREQ_COUNT,
			  Q3_FREQ_COUNT,
			  Q4_FREQ_COUNT,
			  EXCHANGE_RATE,
			  LEVEL_TYPE,
			  OUT_QTY_W01,
			  OUT_QTY_W02,
			  OUT_QTY_W03,
			  OUT_QTY_W04,
			  OUT_QTY_W05,
			  OUT_QTY_W06,
			  OUT_QTY_W07,
			  OUT_QTY_W08,
			  OUT_QTY_W09,
			  OUT_QTY_W10,
			  OUT_QTY_W11,
			  OUT_QTY_W12,
			  OUT_QTY_W13,
			  OUT_QTY_W14,
			  OUT_QTY_W15,
			  OUT_QTY_W16,
			  OUT_QTY_W17,
			  OUT_QTY_W18,
			  OUT_QTY_W19,
			  OUT_QTY_W20,
			  OUT_QTY_W21,
			  OUT_QTY_W22,
			  OUT_QTY_W23,
			  OUT_QTY_W24,
			  OUT_QTY_W25,
			  OUT_QTY_W26,
			  IN_QTY_W01,
			  IN_QTY_W02,
			  IN_QTY_W03,
			  IN_QTY_W04,
			  IN_QTY_W05,
			  IN_QTY_W06,
			  IN_QTY_W07,
			  IN_QTY_W08,
			  IN_QTY_W09,
			  IN_QTY_W10,
			  IN_QTY_W11,
			  IN_QTY_W12,
			  IN_QTY_W13,
			  IN_QTY_W14,
			  IN_QTY_W15,
			  IN_QTY_W16,
			  IN_QTY_W17,
			  IN_QTY_W18,
			  IN_QTY_W19,
			  IN_QTY_W20,
			  IN_QTY_W21,
			  IN_QTY_W22,
			  IN_QTY_W23,
			  IN_QTY_W24,
			  IN_QTY_W25,
			  IN_QTY_W26,
			  MATERIAL_LEVEL_VALUE,
			  MRP_CONTROLLER,
			  MRP_CONTROLLER_DISPO AS MRP_CONTROLLER_KEY,
			  PURCH_GROUP,
			  PURCH_GROUP_EKGRP AS PURCH_GROUP_KEY,
			  PROD_SCHED_FEVOR  AS PROD_SCHED_KEY,
			  REORDER_PT        AS RECORDER_POINT,
			  CEIL(1.4*GRT + PDT)   AS LEAD_TIME,  --Lead Time Change!!
			  PDT AS PDT,
			  GRT AS GRT,
			  MEINS_ISSUE_UOM   AS UNIT,
			  ISSUE_UOM_NUMERATOR,
			  PO_UOM_NUMERATOR,
			  MATL_TYPE_MTART AS MATL_TYPE,
			  ULTIMATE_SOURCE
			FROM INV_SAP_PP_OPTIMIZATION --WHERE MATERIALID = '100-C09KJ400 A' AND PLANTID = '5200'
			)PP_BASIC
		  LEFT JOIN
			(SELECT ID,
			  MATERIAL,
			  CATALOG_DASH,
			  CATALOG_NO_DASH,
			  DIST_CHL,
			  CURRENT_SERIES
			FROM VIEW_INV_SAP_ITEM_X --WHERE MATERIAL = '100-C09KJ400 A' AND PLANTID = '5200'
			)ITEM_X
		  ON ITEM_X.ID = PP_BASIC.ID
		  );		
      
--SO BASIC
--VIEW_INV_SAP_OPEN_SO
DROP TABLE INV_SAP_OPEN_SO;
DROP VIEW VIEW_INV_SAP_OPEN_SO;
SELECT * FROM VIEW_INV_SAP_OPEN_SO;
CREATE VIEW VIEW_INV_SAP_OPEN_SO AS 
SELECT SALESDOC
  ||'_'
  || SALESDOCITEM AS SO_ID,
  MATERIAL
  ||'_'
  || PLANT                  AS ID,
  SALESDOC                  AS SALES_DOC,
  SALESDOCITEM              AS DOC_ITEM,
  MATERIAL                  AS MATERIAL,
  PLANT                     AS PLANT,
  SALES_ORG                 AS SALES_ORG,
  LINECREATEDATE            AS LINE_CREATED_DATE,
  SUBSTR(PRODHIER,0,3)      AS BU,
  ORDERQTY                  AS ORDER_QTY,
  OPEN_QTY                  AS OPEN_QTY,
  OPEN_QTY_BASE_UOM         AS OPEN_QTY_BASE_UOM,
  PROFITCENTER              AS PROFIT_CENTER,
  SALESPRICE                AS SALES_PRICE,
  CURRENCY                  AS CURRENCY,
  MAX_COMMIT_DATE           AS COMMITTED_DATE,
  MAX_CONFIRM_DATE          AS CONFIRM_DATE,
  SOLD_TO                   AS SOLD_TO_PARTY,
  DELIVSTATUS               AS DELIVERY_STATUS,
  OVERALLDELIVSTATSU        AS OVER_ALL_DELIVERY_STATUS,
  SALESDOCTYPE              AS SALE_DOC_TYPE,
  SALEDOCITEMCATEGORY_PSTYV AS SALEDOCITEMCATEGORY_PSTYV,
  SHIPFROM_VSTEL            AS SHIPPING_POINT,
  STOCKSTATUS               AS STOCK_STATUS,
  DELIVPRIO                 AS DELIVERY_PRIORITY,
  LST_ACT_GI_DATE           AS LST_ACT_GI_DATE,
  LST_DELV_CREATED_DATE     AS LST_DELIVERY_CREATE_DATE,
  REQTYPE                   AS REQUEST_TYPE,
  EXCHANGE_RATE_TO_USD      AS EXCHANGE_RATE_TO_USD,
  MAX_REQUEST_DATE          AS REQUIRE_DATE,
  ROUTE                     AS ROUTE
FROM INV_SAP_SALES_VBAK_VBAP_VBUP; 

SELECT * FROM
(
select SALES_DOC||'_'||
MATERIAL AS SO_ID1,
SALES_DOC,
LINE_NUM,
MATERIAL,
PLANT,
SALES_ORG,
LINE_CREATED_DATE,
ORDER_QTY,
OPEN_QTY,
COMMITTED_DATE,
CONFIRM_DATE,
REQUIRE_DATE,
SALE_DOC_TYPE,
SHIPPING_POINT,
ROUTE
from VIEW_INV_SAP_OPEN_SO where PLANT = '5140' AND COMMITTED_DATE = TO_CHAR(SYSDATE)
)OPEN_SO
LEFT JOIN
(
SELECT 
REFERENCE_DOC_TRIM||'_'||
MATERIALID AS SO_ID,
DELIVERY
FROM
INV_SAP_LIKP_LIPS_DAILY WHERE PLANTID = '5140'
)DELIV
ON OPEN_SO.SO_ID1 = DELIV.SO_ID;

SELECT * FROM
(
SELECT 
REFERENCE_DOC_TRIM||'_'||
MATERIALID AS SO_ID,
DELIVERY
FROM
INV_SAP_LIKP_LIPS_DAILY WHERE PLANTID = '5140'
)DELIV
LEFT JOIN
(
select SALES_DOC||'_'||
MATERIAL AS SO_ID1,
SALES_DOC,
LINE_NUM,
MATERIAL,
PLANT,
SALES_ORG,
LINE_CREATED_DATE,
ORDER_QTY,
OPEN_QTY,
COMMITTED_DATE,
CONFIRM_DATE,
REQUIRE_DATE,
SALE_DOC_TYPE,
SHIPPING_POINT,
ROUTE
from VIEW_INV_SAP_OPEN_SO where PLANT = '5140' AND COMMITTED_DATE = TO_CHAR(SYSDATE)
)OPEN_SO
ON OPEN_SO.SO_ID1 = DELIV.SO_ID;

--VIEW_INV_SAP_ITEM_STATISTICS
--1.pass-due open qty and line
--2.LT open so qty and line
--3.(13weeks - LT) open so qty and line
--4. 13weeks after (Long Committed Date) open so qty and line
--5.All open so qty and line.
--6.FC_QTY

--LT 5040 14, 5050 8,5100 10,5110 11,5110 13,5160 16, 5190 16, 5200 10,5070 14, 5140 14
-- 5040 14,5110 13,5070 14, 5140 14



(
SELECT PLANTID
      ||'_'
      ||MATERIALID                           AS ID,
      MATERIALID                          AS MATERIALID,
      PLANTID                             AS PLANTID,
      CEIL(SUM(PLNMG_PLANNEDQUANTITY)/26) AS FC_AVG_WEEK
    FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DW_DBLINK
    WHERE (PDATU_DELIV_ORDFINISHDATE BETWEEN TO_CHAR(sysdate) AND TO_CHAR(sysdate + 182))
    AND PLANTID = '5110'
    AND VERSBP_VERSION = '55'
    AND MATERIALID    IN  ()
    GROUP BY MATERIALID,
      MATERIALID
      ||'_'
      ||PLANTID,
      PLANTID,
      MATERIALID
      )FC_QTY_WEEK26








--BACKLOG_OPEN_QTY,PAST_DUE_OPEN_QTY,PO_SO_SA_FLAG,STO_OPEN_QTY
--OPEN STATUS
DROP VIEW VIEW_INV_SAP_SOPO_OPEN_QTY;
SELECT * FROM VIEW_INV_SAP_SOPO_OPEN_QTY;
CREATE VIEW VIEW_INV_SAP_SOPO_OPEN_QTY AS
SELECT SALES_OPEN_PASS_QTY.ID           AS ID,
  SALES_OPEN_PASS_QTY.BACKLOG_OPEN_QTY  AS BACKLOG_OPEN_QTY,
  SALES_OPEN_PASS_QTY.PAST_DUE_OPEN_QTY AS PAST_DUE_OPEN_QTY,
  STO_OPEN_QTY_S.PO_SO_SA_FLAG          AS PO_SO_SA_FLAG,
  STO_OPEN_QTY_S.STO_OPEN_QTY           AS STO_OPEN_QTY
FROM
  (
  --Sales Orders Status
  SELECT BACKLOG_OPEN.ID                   AS ID,
    NVL(BACKLOG_OPEN.OPEN_QTY,0)           AS BACKLOG_OPEN_QTY,
    NVL(PAST_DUE_OPEN.PAST_DUE_OPEN_QTY,0) AS PAST_DUE_OPEN_QTY
  FROM
    (SELECT MATERIAL
      ||'_'
      ||PLANT              AS ID,
      MATERIAL             AS MATERIALID,
      PLANT                AS PLANTID,
      NVL(SUM(OPEN_QTY),0) AS OPEN_QTY
    FROM INV_SAP_SALES_VBAK_VBAP_VBUP
    GROUP BY MATERIAL
      ||'_'
      ||PLANT,
      MATERIAL,
      PLANT
    )BACKLOG_OPEN
  LEFT JOIN
    (SELECT MATERIAL
      ||'_'
      ||PLANT               AS ID,
      MATERIAL              AS MATERIALID,
      PLANT                 AS PLANTID,
      NVL(SUM(OPEN_QTY), 0) AS PAST_DUE_OPEN_QTY
    FROM INV_SAP_SALES_VBAK_VBAP_VBUP
    WHERE MAX_COMMIT_DATE < SYSDATE - 1
    GROUP BY MATERIAL
      ||'_'
      ||PLANT,
      MATERIAL,
      PLANT
    )PAST_DUE_OPEN
  ON PAST_DUE_OPEN.ID = BACKLOG_OPEN.ID
  )SALES_OPEN_PASS_QTY
LEFT JOIN
 ---STO_OPEN_QTY
  (
  SELECT MATERIALID
    ||'_'
    ||PLANTID AS ID,
    MATERIALID,
    PLANTID,
    PO_SO_SA_FLAG,
    NVL(SUM(COMMITTEDQTY),0) AS STO_OPEN_QTY
  FROM INV_SAP_PP_PO_HISTORY
  WHERE PO_SO_SA_FLAG     = 'STO'
  AND DELIVERYCOMPLETE IS NULL
  GROUP BY MATERIALID,
    PLANTID,
    PO_SO_SA_FLAG
  )STO_OPEN_QTY_S
ON SALES_OPEN_PASS_QTY.ID = STO_OPEN_QTY_S.ID;
  
  



--DELIVERY RECORD
---This report for jane. It is about the delivery that all vendors shipment to 5040.
---Report frequency is weekly. Every Monday and time span is last week's suppliers' shipment status.
DROP VIEW VIEW_INV_SAP_DELREC_HK_SH;
CREATE VIEW VIEW_INV_SAP_DELREC_HK_SH AS
SELECT DELIVERY,
  DELIVERY_ITEM,
  CREATED_BY,
  CREATED_ON_DATE,
  MATERIALID,
  PLANTID,
  MVMT_TYPE,
  DELIVERY_QTY_SUOM,
  BASE_UOM,
  NUMERATOR,
  DENOMINATOR,
  MATERIAL_AVAIL_DATE,
  REFERENCE_DOC_TRIM,
  REFERENCE_DOC_ITEM_TRIM,
  DELIVERY_TYPE,
  INCOTERMS1,
  INCOTERMS2,
  DELIVERY_PRIORITY,
  CHANGED_ON_DATE,
  ACT_GI_DATE,
  DWQ_SRC_EXTRACT_DATE,
  DELIVERY_QTY_STO_ITEM
FROM INV_SAP_LIKP_LIPS_DAILY
WHERE REFERENCE_DOC_TRIM IN
  (SELECT EBELNPURCHDOCNO
  FROM INV_SAP_PP_PO_HISTORY
  WHERE PLANTID IN ('5040', '5050')
  )
AND CREATED_ON_DATE BETWEEN TO_CHAR(SYSDATE - 7) AND TO_CHAR(SYSDATE);

---------------------------------------------------REPORTS VIEWS--------------------------------------------------------
SELECT * FROM v$parameter WHERE name = 'nls_date_language';


SELECT * FROM INV_SAP_SALES_HST WHERE LINECREATIONDATE < SYSDATE - 300 AND PLANTID = '5040'

