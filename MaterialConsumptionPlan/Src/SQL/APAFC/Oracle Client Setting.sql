-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Project Name : APAFC
-- Version : 1.0
-- Description:
-- Oracle Client Setting
-- Revision History:
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-12-24  Moyue           	Oracle Client Setting.
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

1. Oracle Client Setup
	1.1 Net Service Name: data_analysis
		SERVICE_NAME: APAFC
		SID: APAFC
		HOST: APCNXMNGYKG422
		PORT: 1523
		PW:apafc198617
		Database Control URL :https://localhost:5500/em
	1.2 DB LINK Setting
		DB LINK NAME:ROCKWELL_DW_DBLINK
		Connection:	RA_DB_Oracle		Host_Name:	risop.mke.ra.rockwell.com
		User_Name:	INVANALYST		Port:	1522
		PWD:	Materials1		SID:	RISOP

	1.3 Ini Tables
		DROP TABLE INV_SAP_PP_OPTIMIZATION;
		CREATE TABLE INV_SAP_PP_OPTIMIZATION AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DW_DBLINK;
		
		CREATE TABLE INV_SAP_PLAN_PARAM_USERLOG AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PLAN_PARAM_USERLOG@ROCKWELL_DW_DBLINK;
		SELECT * FROM INV_SAP_PLAN_PARAM_USERLOG;
    
		DROP TABLE INV_SAP_SALES_VBAK_VBAP_VBUP;
		CREATE TABLE INV_SAP_SALES_VBAK_VBAP_VBUP AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DW_DBLINK;

		DROP TABLE INV_SAP_PP_FRCST_PBIM_PBED;
		CREATE TABLE INV_SAP_PP_FRCST_PBIM_PBED AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

		DROP TABLE INV_SAP_LIKP_LIPS_DAILY; 
		CREATE TABLE INV_SAP_LIKP_LIPS_DAILY AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DW_DBLINK WHERE CREATED_ON_DATE > SYSDATE - 91;

		DROP TABLE INV_SAP_INVENTORY_BY_PLANT;
		CREATE TABLE INV_SAP_INVENTORY_BY_PLANT AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DW_DBLINK;

		DROP TABLE INV_SAP_PP_PO_HISTORY;
		CREATE TABLE INV_SAP_PP_PO_HISTORY AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

		DROP TABLE INV_SAP_IO_INPUTS_DAILY;
		CREATE TABLE INV_SAP_IO_INPUTS_DAILY AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_IO_INPUTS_DAILY@ROCKWELL_DW_DBLINK;
			
		DROP TABLE INV_SAP_SALES_HST;
		CREATE TABLE INV_SAP_SALES_HST AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DW_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

		DROP TABLE INV_SAP_PP_MVKE;
		CREATE TABLE INV_SAP_PP_MVKE AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DW_DBLINK;
		
		DROP TABLE INV_SAP_PP_PLANT_SAOG;
		CREATE TABLE INV_SAP_PP_PLANT_SAOG AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_T001W@ROCKWELL_DW_DBLINK;
		
		--MRP TYPE
		DROP TABLE INV_SAP_MRP_TYPE_DIS;
		CREATE TABLE INV_SAP_MRP_TYPE_DIS AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MRP_TYPE_DIS@ROCKWELL_DW_DBLINK;

		--PRODUCTION PARAMETERS
		DROP TABLE INV_SAP_PRODUCTION_PARAMETERS;
		CREATE TABLE INV_SAP_PRODUCTION_PARAMETERS AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PRODUCTION_PARAMETERS@ROCKWELL_DW_DBLINK;
		
		--MATL TYPE
		DROP TABLE INV_SAP_MATL_TYPE_DIS;
		CREATE TABLE INV_SAP_MATL_TYPE_DIS AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATL_TYPE_DIS@ROCKWELL_DW_DBLINK;
 
		--Table that houses Special Pro Key Plant to Plant Combinations. Used to take plant and special pro key and determine the sending plant
 		DROP TABLE INV_SAP_CODE_LOOKUP;
		CREATE TABLE INV_SAP_CODE_LOOKUP AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_CODE_LOOKUP@ROCKWELL_DW_DBLINK; 
		
		--MRP CONTROLLER INFO
		DROP TABLE INV_SAP_MRP_CONTROLLER_DIS;
		CREATE TABLE INV_SAP_MRP_CONTROLLER_DIS AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MRP_CONTROLLER_DIS@ROCKWELL_DW_DBLINK;
		
		--Table that holds last RISO SS and ROP User Review and special flag updates info.
		DROP TABLE INV_SAP_PLAN_PARAM_REVIEW;
		CREATE TABLE INV_SAP_PLAN_PARAM_REVIEW AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PLAN_PARAM_REVIEW@ROCKWELL_DW_DBLINK;
		
		DROP TABLE INV_SAP_SHIP_SOLD_TO_PARTYID;
		CREATE TABLE INV_SAP_SHIP_SOLD_TO_PARTYID AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_DW_DBLINK;
		
		--PLANT -USAGE -DEMAND
		DROP TABLE INV_SAP_PP_OPT_PLANTS;
		CREATE TABLE INV_SAP_PP_OPT_PLANTS AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPT_PLANTS@ROCKWELL_DW_DBLINK;
		
		--MPS ROUTING INFO
		DROP TABLE INV_SAP_PP_ROUTING;
		CREATE TABLE INV_SAP_PP_ROUTING AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_ROUTING@ROCKWELL_DW_DBLINK;
		
		--MFG DATA WAREHOUSE SO DATA
		--5040,5050,5110,5100
		DROP TABLE INV_SAP_R8_SALES_DC_HST;
		CREATE TABLE INV_SAP_R8_SALES_DC_HST AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_R8_SALES_DC_HST@ROCKWELL_DW_DBLINK;		
		
		--MFG DATA WAREHOUSE SO DATA
		--5070,5140
		DROP TABLE INV_SAP_R9_SALES_DC_HST;
		CREATE TABLE INV_SAP_R9_SALES_DC_HST AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_R9_SALES_DC_HST@ROCKWELL_DW_DBLINK;	
		
		--MFG DATA WAREHOUSE SO DATA
		--5120,5190,5200,5160
		DROP TABLE INV_SAP_R7B_SALES_SEA_HST;
		CREATE TABLE INV_SAP_R7B_SALES_SEA_HST AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_R7B_SALES_SEA_HST@ROCKWELL_DW_DBLINK;	