-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Table Init
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2015-1-27  Moyue           	INV PP OPT.
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Test Done, OK!!
CREATE OR REPLACE PROCEDURE APAFC_INV_PP_OPT
IS
STEP_NAME VARCHAR2(100);
PROCESS_TERMINATED EXCEPTION;
COUNT_INV_OPT_RAW NUMBER;
COUNT_INV_OPT NUMBER;

COUNT_IDX_OPTX_MATL NUMBER;
COUNT_IDX_OPTX_PLT NUMBER;
COUNT_IDX_OPTX_ID NUMBER;

BEGIN
	STEP_NAME := 'INV SAP PP TABLE...';
 
	SELECT COUNT(*) INTO COUNT_INV_OPT_RAW FROM USER_TABLES WHERE TABLE_NAME = 'INV_SAP_PP_OPTIMIZATION';
	SELECT COUNT(*) INTO COUNT_INV_OPT FROM USER_TABLES WHERE TABLE_NAME = 'INV_SAP_PP_OPT_X';
  
  SELECT COUNT(*) INTO COUNT_IDX_OPTX_MATL FROM USER_INDEXES WHERE INDEX_NAME = 'INDEX_OPTX_MATL';
  SELECT COUNT(*) INTO COUNT_IDX_OPTX_PLT FROM USER_INDEXES WHERE INDEX_NAME = 'INDEX_OPTX_PLT';
  SELECT COUNT(*) INTO COUNT_IDX_OPTX_ID FROM USER_INDEXES WHERE INDEX_NAME = 'INDEX_OPTX_ID';
  
	IF COUNT_INV_OPT_RAW > 0 THEN
	  APAFC_TRUNCATE('INV_SAP_PP_OPTIMIZATION');
	  COMMIT;
		APAFC_SCRIPT_EXE_IMMEDIATE('INSERT INTO INV_SAP_PP_OPTIMIZATION SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK');
	  COMMIT;
	ELSE
		APAFC_SCRIPT_EXE_IMMEDIATE('CREATE TABLE INV_SAP_PP_OPTIMIZATION AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK');
	  COMMIT;
	END IF;  
  

  IF COUNT_IDX_OPTX_MATL > 0 THEN
    APAFC_DROP('INDEX_OPTX_MATL','INDEX');
  END IF;
  
  IF COUNT_IDX_OPTX_PLT > 0 THEN
    APAFC_DROP('INDEX_OPTX_PLT','INDEX');
  END IF;
  
  IF COUNT_IDX_OPTX_ID > 0 THEN 
    APAFC_DROP('INDEX_OPTX_ID','INDEX');
  END IF;
  COMMIT;
  
	IF COUNT_INV_OPT > 0 THEN
		APAFC_TRUNCATE('INV_SAP_PP_OPT_X');
	  COMMIT;
		APAFC_INSERT('VIEW_INV_SAP_PP_OPT_X','INV_SAP_PP_OPT_X');
	  COMMIT;
	ELSE
		APAFC_SCRIPT_EXE_IMMEDIATE('CREATE TABLE INV_SAP_PP_OPT_X AS SELECT * FROM VIEW_INV_SAP_PP_OPT_X');
		COMMIT;
	END IF;  

	APAFC_CREATE_INDEX('INDEX_OPTX_MATL','INV_SAP_PP_OPT_X','MATERIAL');
	APAFC_CREATE_INDEX('INDEX_OPTX_PLT','INV_SAP_PP_OPT_X','PLANT');
	APAFC_CREATE_INDEX('INDEX_OPTX_ID','INV_SAP_PP_OPT_X','ID');
	COMMIT;
  
EXCEPTION
WHEN OTHERS THEN
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Execute Error:');
  DBMS_OUTPUT.PUT_LINE(SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,200)||SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200));
  RAISE PROCESS_TERMINATED;
END APAFC_INV_PP_OPT;


begin
APAFC_INV_PP_OPT;
end;

create table INV_SAP_PP_OPT_X_bk as 
select count(*) from INV_SAP_PP_OPT_X WHERE plant IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140','1090');
select * from INV_SAP_PP_OPTIMIZATION;
select * from INV_SAP_PP_OPT_X_B2;
drop table INV_SAP_PP_OPTIMIZATION_BK12;
drop table INV_SAP_PP_OPT_X_BK;
drop table INV_SAP_PP_OPTIM_BK;


INSERT INTO INV_SAP_PP_OPT_X_STORE SELECT * FROM INV_SAP_PP_OPT_X WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140','1090');


TRUNCATE TABLE INV_SAP_PP_OPT_X_STORE;


INSERT INTO INV_SAP_PP_OPT_X_STORE
SELECT 
SYSDATE 
ID,
TMP_ID,
LAST_REVIEW,
MATERIAL,
CATALOG_DASH,
CATALOG_NO_DASH,
PLANT,
SALES_ORG,
PLANTID_DESC,
MAT_DESC,
SAFETY_STOCK,
OH_QTY,
UNIT,
UNIT_COST,
OH_QTY_INTRANSIT,
PROD_BU,
PROD_FAM,
PROD_HIERARCHY,
PROC_TYPE,
STRATEGY_GRP,
MRP_TYPE,
PROD_SCHEDULER,
PLANT_SP_MATL_STA,
VENDOR_KEY,
VENDOR_ITEM,
MIN_INV,
TARGET_INV,
MAX_INV,
LOT_SIZE_QTY,
LOT_ROUNDING_VALUE,
LOT_SIZE_DISLS,
LOT_MIN_BUY,
AVG52_USAGE_QTY,
STDEV52_USAGE,
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
MRP_CONTROLLER_KEY,
PURCH_GROUP,
PURCH_GROUP_KEY,
PROD_SCHED_KEY,
RECORDER_POINT,
LEAD_TIME,
PDT,
GRT,
SALES_ORG_COUNT,
DIRECT_SHIP_PLANT,
DELIVERY_PLANT,
DMI_MANAGED,
ABC,
AVG_MONTHLY_DEM,
STDEV_MONTHLY_DEM,
DIST_CHL,
D_CHAIN_BLK,
DELIVERY_UNIT,
ISSUE_UOM_NUMERATOR,
PO_UOM_NUMERATOR,
MATL_TYPE,
CURRENT_SERIES,
ULTIMATE_SOURCE
FROM INV_SAP_PP_OPT_X WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140','1090');


DECLARE
  INSERT_TABLE_OPT_X VARCHAR2(5000);
BEGIN
  INSERT_TABLE_OPT_X :=
  'INSERT INTO INV_SAP_INV_REC    
    SELECT    
    DISTINCT    
    INV.INV_ID                                                                           AS INV_ID,    
    INV.ID                                                                               AS ID,    
    INV.REC_DATE                                                                         AS REC_DATE,    
    INV.PLANTID                                                                          AS PLANTID,    
    INV.MATERIALID                                                                       AS MATERIAL,    
    PP.CATALOG_DASH                                                                      AS CATALOG_DASH,    
    PP.SAFETY_STOCK                                                                      AS SAFETY_STOCK,    
    PP.UNIT_COST                                                                         AS UNIT_COST,    
    PP.STRATEGY_GRP                                                                      AS STRATEGY_GRP,    
    PP.PROD_BU                                                                           AS PROD_BU,    
    PP.PROD_FAM                                                                          AS PROD_FAM,    
    PP.MATL_TYPE                                                                         AS MATL_TYPE,    
    NVL(INV.OH_QTY,0)                                                                    AS OH_QTY,    
    NVL(PP.MIN_INV,0)                                                                    AS MIN_INV,    
    NVL(PP.TARGET_INV,0)                                                                 AS TARGET_INV,    
    NVL(PP.MAX_INV,0)                                                                    AS MAX_INV,    
    (NVL(INV.OH_QTY,0)    *NVL(PP.UNIT_COST,0))                                          AS OH_VAL,    
    (NVL(PP.MIN_INV,0)    *NVL(PP.UNIT_COST,0))                                          AS MIN_INV_VAL,    
    (NVL(PP.TARGET_INV,0) *NVL(PP.UNIT_COST,0))                                          AS TARGET_INV_VAL,    
    (NVL(PP.MAX_INV,0)    *NVL(PP.UNIT_COST,0))                                          AS MAX_INV_VAL,    
    (NVL(INV.OH_QTY,0)    *NVL(PP.UNIT_COST,0) - NVL(PP.MAX_INV,0) *NVL(PP.UNIT_COST,0)) AS OVER_MAX_VAL  
    FROM  
    (SELECT ID,      
    MATERIAL,      
    CATALOG_DASH,      
    PLANT,      
    SAFETY_STOCK,      
    UNIT,      
    UNIT_COST,      
    STRATEGY_GRP,      
    PROD_BU,      
    PROD_FAM,      
    MATL_TYPE,      
    MIN_INV,      
    TARGET_INV,      
    MAX_INV,      
    LEAD_TIME    
    FROM INV_SAP_PP_OPT_X    
    )PP  
    LEFT JOIN      
    (SELECT       
    SYSDATE||''_''||MATERIALID||''_''||PLANTID AS INV_ID,      
    MATERIALID||''_''||PLANTID AS ID,      
    SYSDATE AS REC_DATE,      
    MATERIALID,      
    PLANTID,      
    OH_QTY    
    FROM VIEW_INV_SAP_INV_BY_PLANT WHERE PLANTID IN ('
      ||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||','||5180||','||5150||','||5130||')    
    )INV      
    ON PP.ID = INV.ID';
  EXECUTE IMMEDIATE INSERT_TABLE_INV_REC;
END;
    