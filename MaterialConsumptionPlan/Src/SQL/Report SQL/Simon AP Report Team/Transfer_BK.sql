




1.Clear
DROP TABLE VIEW_INV_SAP_BACKLOG_PO_TMP;
DROP TABLE STOCK_ITEM_STATUS_BY_BU_WEEK;
DROP TABLE INV_SAP_BACKLOG_DB;
DROP TABLE INV_SAP_DMT_FRD;
DROP TABLE INV_SAP_FC_STATS;
DROP TABLE INV_SAP_INVENTORY_WKS;
DROP TABLE INV_SAP_ITEM_SO_STAT;
DROP TABLE INV_SAP_MATERIAL_CATALOG;
DROP TABLE INV_SAP_NODASH_MAT_CATA;
DROP TABLE INV_SAP_PP_MVKE;
DROP TABLE INV_SAP_PP_OPTIMIZATION;
DROP TABLE INV_SAP_PP_PARAM;
DROP TABLE INV_SAP_RCCP_INDREQ;
DROP TABLE INV_SAP_RCCP_T;

DROP TABLE INV_SAP_SVD_COMMENTS;
DROP TABLE INV_SAP_SVD_COMMENTS_TMP;
DROP TABLE ITEM_CREATE_FSTUSE_DATE;
DROP TABLE DELIVERY;


2.Create table 


--Daily Jobs
	2.1 Job Name: Job_Daily_INV_PP_OPT
		Job Time: 1:00 AM
		Job Repeat Frequency: Daily, 5 days per week
      
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_PP_OPTIMIZATION   VARCHAR2(1000);
		  INSERT_TABLE_PP_OPTIMIZATION VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_PP_OPTIMIZATION   := 'TRUNCATE TABLE INV_SAP_PP_OPTIMIZATION';
		  CREATE_TABLE_PP_OPTIMIZATION := 'INSERT INTO INV_SAP_PP_OPTIMIZATION 
			SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DBLINK';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_PP_OPTIMIZATION;
		  EXECUTE IMMEDIATE INSERT_TABLE_PP_OPTIMIZATION;
		END;
		
		Job INIT:
		DROP TABLE INV_SAP_PP_OPTIMIZATION;
		CREATE TABLE INV_SAP_PP_OPTIMIZATION AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION@ROCKWELL_DBLINK;
		
	2.2 Job Name: Job_Daily_PP_TABLE
		Job Time: 2:00 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DROP TABLE INV_SAP_PP_OPT_X;
		CREATE TABLE INV_SAP_PP_OPT_X AS SELECT * FROM VIEW_INV_SAP_PP_OPT_X;
		
		DECLARE
		  TRUNCATE_TABLE_INV_PP_TABLE  VARCHAR2(1000);
		  INSERT_TABLE_INV_PP_TABLE  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_PP_TABLE   := 'TRUNCATE TABLE INV_SAP_PP_OPT_X';
		  INSERT_TABLE_INV_PP_TABLE := 'INSERT INTO INV_SAP_PP_OPT_X SELECT * FROM VIEW_INV_SAP_PP_OPT_X';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_PP_TABLE;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_PP_TABLE;
		END;


	2.4 Job Name: Job_Weekly_INV_PP_MVKE
		Job Time: 10:00 AM SAT
		Job Repeat Frequency: Weekly
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_INV_SAP_PP_MVKE   VARCHAR2(1000);
		  INSERT_TABLE_INV_SAP_PP_MVKE VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_SAP_PP_MVKE   := 'TRUNCATE TABLE INV_SAP_PP_MVKE';
		  INSERT_TABLE_INV_SAP_PP_MVKE := 'INSERT INTO INV_SAP_PP_MVKE SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DBLINK';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_SAP_PP_MVKE;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_SAP_PP_MVKE;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_PP_MVKE;
		CREATE TABLE INV_SAP_PP_MVKE AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_MVKE@ROCKWELL_DBLINK;
	
	2.5 Job Name: Job_Daily_INV_SO_OPEN
		Job Time: 3:00 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_OPEN_SALES   VARCHAR2(1000);
		  INSERT_TABLE_OPEN_SALES  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_OPEN_SALES   := 'TRUNCATE TABLE INV_SAP_SALES_VBAK_VBAP_VBUP';
		  INSERT_TABLE_OPEN_SALES := 'INSERT INTO INV_SAP_SALES_VBAK_VBAP_VBUP 
			SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DBLINK';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_OPEN_SALES;
		  EXECUTE IMMEDIATE INSERT_TABLE_OPEN_SALES;
		END;
		
		Job Initialization: 
		Drop table INV_SAP_SALES_VBAK_VBAP_VBUP;
		CREATE TABLE INV_SAP_SALES_VBAK_VBAP_VBUP AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_VBAK_VBAP_VBUP@ROCKWELL_DBLINK;

	2.6 Job Name: Job_Daily_INV_PP_FCST
		Job Time: 3:30 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_OPEN_FC   VARCHAR2(1000);
		  INSERT_TABLE_OPEN_FC  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_OPEN_FC   := 'TRUNCATE TABLE INV_SAP_PP_FRCST_PBIM_PBED';
		  INSERT_TABLE_OPEN_FC := 'INSERT INTO INV_SAP_PP_FRCST_PBIM_PBED 
			SELECT count * FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_OPEN_FC;
		  EXECUTE IMMEDIATE INSERT_TABLE_OPEN_FC;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_PP_FRCST_PBIM_PBED;
		CREATE TABLE INV_SAP_PP_FRCST_PBIM_PBED AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_FRCST_PBIM_PBED@ROCKWELL_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

		
	2.7 Job Name: Job_Daily_INV_DELY
		Job Time: 4:00 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_DELIVERY_REC  VARCHAR2(1000);
		  INSERT_TABLE_DELIVERY_REC  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_DELIVERY_REC   := 'TRUNCATE TABLE INV_SAP_LIKP_LIPS_DAILY';
		  INSERT_TABLE_DELIVERY_REC := 'INSERT INTO INV_SAP_LIKP_LIPS_DAILY 
			SELECT * FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DBLINK WHERE CREATED_ON_DATE > SYSDATE - 91';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_DELIVERY_REC;
		  EXECUTE IMMEDIATE INSERT_TABLE_DELIVERY_REC;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_LIKP_LIPS_DAILY; 
		CREATE TABLE INV_SAP_LIKP_LIPS_DAILY AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DBLINK WHERE CREATED_ON_DATE > SYSDATE - 91;

	2.8 Job Name: Job_Daily_INV_PLANT
		Job Time: 4:30 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_INVENTORY_REC  VARCHAR2(1000);
		  INSERT_TABLE_INVENTORY_REC  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INVENTORY_REC   := 'TRUNCATE TABLE INV_SAP_INVENTORY_BY_PLANT';
		  INSERT_TABLE_INVENTORY_REC := 'INSERT INTO INV_SAP_INVENTORY_BY_PLANT 
			SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DBLINK';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INVENTORY_REC;
		  EXECUTE IMMEDIATE INSERT_TABLE_INVENTORY_REC;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_INVENTORY_BY_PLANT;
		CREATE TABLE INV_SAP_INVENTORY_BY_PLANT AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_INVENTORY_BY_PLANT@ROCKWELL_DBLINK;

	2.9 Job Name: Job_Daily_INV_PP_PO
		Job Time: 5:00 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_PP_PO_HISTORY  VARCHAR2(1000);
		  INSERT_TABLE_PP_PO_HISTORY  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_PP_PO_HISTORY   := 'TRUNCATE TABLE INV_SAP_PP_PO_HISTORY';
		  INSERT_TABLE_PP_PO_HISTORY := 'INSERT INTO INV_SAP_PP_PO_HISTORY 
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_PP_PO_HISTORY;
		  EXECUTE IMMEDIATE INSERT_TABLE_PP_PO_HISTORY;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_PP_PO_HISTORY;
		CREATE TABLE INV_SAP_PP_PO_HISTORY AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_PO_HISTORY@ROCKWELL_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

	2.10 Job Name: Job_Daily_INV_IO
		Job Time: 5:30 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_IO_REC  VARCHAR2(1000);
		  INSERT_TABLE_IO_REC  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_IO_REC   := 'TRUNCATE TABLE INV_SAP_IO_INPUTS_DAILY';
		  INSERT_TABLE_IO_REC := 'INSERT INTO INV_SAP_IO_INPUTS_DAILY
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_IO_INPUTS_DAILY@ROCKWELL_DBLINK';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_IO_REC;
		  EXECUTE IMMEDIATE INSERT_TABLE_IO_REC;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_IO_INPUTS_DAILY;
		CREATE TABLE INV_SAP_IO_INPUTS_DAILY AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_IO_INPUTS_DAILY@ROCKWELL_DBLINK;
				
	2.11 Job Name: Job_Daily_FCSTAT55_TABLE
		Job Time: 6:00 AM
		Job Repeat Frequency: Daily
		Job Procedure: 
		DECLARE
		  TRUNCATE_TABLE_INV_FCSTAT  VARCHAR2(1000);
		  INSERT_TABLE_INV_FCSTAT  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_FCSTAT   := 'TRUNCATE TABLE INV_SAP_FC55_STATS';
		  INSERT_TABLE_INV_FCSTAT := 'INSERT INTO INV_SAP_FC55_STATS SELECT * FROM VIEW_INV_SAP_FC55_STATS';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_FCSTAT;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_FCSTAT;
		END;
  	
		Job Initialization: 
		DROP TABLE INV_SAP_FC55_STATS;
		CREATE TABLE INV_SAP_FC55_STATS AS
		SELECT * FROM VIEW_INV_SAP_FC55_STATS;
		
	2.12 Job Name: Job_Daily_FCSTAT00_TABLE
		Job Time: 6:15 AM
		Job Repeat Frequency: Daily
		Job Procedure: 
		DECLARE
		  TRUNCATE_TABLE_INV_FCSTAT  VARCHAR2(1000);
		  INSERT_TABLE_INV_FCSTAT  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_FCSTAT   := 'TRUNCATE TABLE INV_SAP_FC00_STATS';
		  INSERT_TABLE_INV_FCSTAT := 'INSERT INTO INV_SAP_FC00_STATS SELECT * FROM VIEW_INV_SAP_FC00_STATS';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_FCSTAT;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_FCSTAT;
		END;
    
		Job Initialization: 
		DROP TABLE INV_SAP_FC00_STATS;
		CREATE TABLE INV_SAP_FC00_STATS AS
		SELECT * FROM VIEW_INV_SAP_FC00_STATS;
		
	2.13 Job Name: Job_Daily_BLSO_TABLE
		Job Time: 6:30 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_INV_BLSO_TABLE  VARCHAR2(1000);
		  INSERT_TABLE_INV_BLSO_TABLE  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_BLSO_TABLE   := 'TRUNCATE TABLE INV_SAP_BACKLOG_SO';
		  INSERT_TABLE_INV_BLSO_TABLE := 'INSERT INTO INV_SAP_BACKLOG_SO SELECT * FROM VIEW_INV_SAP_BACKLOG_SO';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_BLSO_TABLE;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_BLSO_TABLE;
		END;    
    
		Job Initialization: 
		DROP TABLE INV_SAP_BACKLOG_SO;
		CREATE TABLE INV_SAP_BACKLOG_SO AS
		SELECT * FROM VIEW_INV_SAP_BACKLOG_SO;
		
	2.14 Job Name: Job_Daily_BLINV_TABLE
		Job Time: 6:45 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_INV_BLINV_TABLE  VARCHAR2(1000);
		  INSERT_TABLE_INV_BLINV_TABLE  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_BLINV_TABLE   := 'TRUNCATE TABLE INV_SAP_BACKLOG_INV';
		  INSERT_TABLE_INV_BLINV_TABLE := 'INSERT INTO INV_SAP_BACKLOG_INV SELECT * FROM VIEW_INV_SAP_BACKLOG_INV';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_BLINV_TABLE;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_BLINV_TABLE;
		END; 

		Job Initialization: 
		DROP TABLE INV_SAP_BACKLOG_INV;
		CREATE TABLE INV_SAP_BACKLOG_INV AS
		SELECT * FROM VIEW_INV_SAP_BACKLOG_INV;
			
	2.15 Job Name: Job_Daily_BLPO_TABLE
		Job Time: 7:00 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_INV_BLPO_TABLE  VARCHAR2(1000);
		  INSERT_TABLE_INV_BLPO_TABLE  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_INV_BLPO_TABLE   := 'TRUNCATE TABLE INV_SAP_BACKLOG_PO';
		  INSERT_TABLE_INV_BLPO_TABLE := 'INSERT INTO INV_SAP_BACKLOG_PO SELECT * FROM VIEW_INV_SAP_BACKLOG_PO';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_INV_BLPO_TABLE;
		  EXECUTE IMMEDIATE INSERT_TABLE_INV_BLPO_TABLE;
		END;
    
		Job Initialization: 
		DROP TABLE INV_SAP_BACKLOG_PO;
		CREATE TABLE INV_SAP_BACKLOG_PO AS
		SELECT * FROM VIEW_INV_SAP_BACKLOG_PO;
	
	2.16 Job Name: Job_Daily_SOST_TABLE
		Job Time: 7:15 AM
		Job Repeat Frequency: Daily
		Job Procedure:

		DECLARE
			TRUNCATE_TABLE_SOST  VARCHAR2(1000);
			INSERT_TABLE_SOST_A  VARCHAR2(1000);
			INSERT_TABLE_SOST_B  VARCHAR2(1000);
		BEGIN
			TRUNCATE_TABLE_SOST   := 'TRUNCATE TABLE INV_SAP_ITEM_SO_STAT';
			INSERT_TABLE_SOST_A := 'INSERT INTO INV_SAP_ITEM_SO_STAT SELECT * FROM VIEW_INV_SAP_IMSO_LTIN13TMP';
			INSERT_TABLE_SOST_B := 'INSERT INTO INV_SAP_ITEM_SO_STAT SELECT * FROM VIEW_INV_SAP_IMSO_LTOUT13TMP';

			EXECUTE IMMEDIATE TRUNCATE_TABLE_SOST;
			EXECUTE IMMEDIATE INSERT_TABLE_SOST_A;
			EXECUTE IMMEDIATE INSERT_TABLE_SOST_B;
		END;  
		
		Job Initialization: 
		DROP TABLE INV_SAP_BACKLOG_PO;
		TRUNCATE TABLE INV_SAP_ITEM_SO_STAT;
		INSERT INTO INV_SAP_ITEM_SO_STAT SELECT * FROM VIEW_INV_SAP_IMSO_LTIN13TMP;
		INSERT INTO INV_SAP_ITEM_SO_STAT SELECT * FROM VIEW_INV_SAP_IMSO_LTOUT13TMP;

	2.17 Job Name: Job_Daily_SVD_TO_TABLE
		Job Time: 8:15 AM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
			TRUNCATE_SVD_TABLE  VARCHAR2(1000);
			INSERT_TABLE_SVD  VARCHAR2(1000);
		BEGIN
			TRUNCATE_SVD_TABLE   := 'TRUNCATE TABLE INV_SAP_SVD_REPORT';
			INSERT_TABLE_SVD := 'INSERT INTO INV_SAP_SVD_REPORT SELECT * FROM VIEW_INV_SAP_SVD_REPORT';
			EXECUTE IMMEDIATE TRUNCATE_SVD_TABLE;
			EXECUTE IMMEDIATE INSERT_TABLE_SVD;
		END;   		
		
		Job Initialization: 
		DROP TABLE INV_SAP_SVD_REPORT;
		CREATE TABLE INV_SAP_SVD_REPORT AS SELECT * FROM VIEW_INV_SAP_SVD_REPORT;
		
	2.18 Job Name: Job_Daily_JOB_LOG_TABLE
		Job Time: 8:45 AM
		Job Repeat Frequency: Daily
		Job Procedure:
    SELECT STATUS,REQ_START_DATE FROM user_scheduler_job_run_details WHERE STATUS = 'FAILED';
		CREATE TABLE INV_USER_SCHEDULE_JOB_LOG AS SELECT * FROM user_scheduler_job_log;
		CREATE TABLE INV_USER_SCHEDULE_JOB_DETAILS AS SELECT * FROM user_scheduler_job_run_details WHERE STATUS = 'FAILED';
		TRUNCATE TABLE INV_USER_SCHEDULE_JOB_LOG;
		TRUNCATE TABLE INV_USER_SCHEDULE_JOB_DETAILS;
		INSERT INTO INV_USER_SCHEDULE_JOB_LOG SELECT * FROM user_scheduler_job_log;
		INSERT INTO INV_USER_SCHEDULE_JOB_DETAILS SELECT * FROM user_scheduler_job_run_details;

		DECLARE
			TRUNCATE_JOB_LOG  VARCHAR2(1000);
			TRUNCATE_JOB_DETAILS  VARCHAR2(1000);
			INSERT_TABLE_JOB_LOG  VARCHAR2(1000);
			INSERT_TABLE_JOB_DETAILS  VARCHAR2(1000);
		BEGIN
			TRUNCATE_JOB_LOG   := 'TRUNCATE TABLE INV_USER_SCHEDULE_JOB_LOG';
			TRUNCATE_JOB_DETAILS := 'TRUNCATE TABLE INV_USER_SCHEDULE_JOB_DETAILS';
			INSERT_TABLE_JOB_LOG := 'INSERT INTO INV_USER_SCHEDULE_JOB_LOG SELECT * FROM user_scheduler_job_log';
			INSERT_TABLE_JOB_DETAILS := 'INSERT INTO INV_USER_SCHEDULE_JOB_DETAILS SELECT * FROM user_scheduler_job_run_details';
			EXECUTE IMMEDIATE TRUNCATE_JOB_LOG;
			EXECUTE IMMEDIATE TRUNCATE_JOB_DETAILS;
			EXECUTE IMMEDIATE INSERT_TABLE_JOB_LOG;
			EXECUTE IMMEDIATE INSERT_TABLE_JOB_DETAILS;
		END;

	2.19 Job Name: Job_Daily_SVD_CMS_STATS
		Job Time: 22:00 PM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
			INSERT_TABLE_SVD_CMS  VARCHAR2(1000);
		BEGIN
			INSERT_TABLE_SVD_CMS := 'INSERT
        INTO INV_SAP_SVD_COMMENTS_STATS
          (
            ID,
            COMMENTS,
            PLANNER,
            LAST_UPDATE_DATE,
            SNAPSHOT_DATE
          )
        SELECT ID,
          COMMENTS,
          PLANNER,
          LAST_UPDATE_DATE,
          SYSDATE
        FROM INV_SAP_SVD_COMMENTS';
			EXECUTE IMMEDIATE INSERT_TABLE_SVD_CMS;
		END;   		
		
		Job Initialization: 
		DROP TABLE INV_SAP_SVD_COMMENTS_STATS;
    CREATE TABLE INV_SAP_SVD_COMMENTS_STATS AS
    SELECT * FROM INV_SAP_SVD_COMMENTS;
    
    INSERT
    INTO INV_SAP_SVD_COMMENTS_STATS
      (
        ID,
        COMMENTS,
        PLANNER,
        LAST_UPDATE_DATE,
        SNAPSHOT_DATE
      )
    SELECT ID,
      COMMENTS,
      PLANNER,
      LAST_UPDATE_DATE,
      SYSDATE
    FROM INV_SAP_SVD_COMMENTS;

	2.20 Job Name: Job_Daily_AP_DELIERC_Y
		Job Time: 21:00 PM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
			INSERT_TABLE_DELI  VARCHAR2(1000);
      TRUNCATE_TABLE_DELI VARCHAR2(1000);
		BEGIN
      TRUNCATE_TABLE_DELI := 'TRUNCATE TABLE INV_SAP_LIKP_LIPS_DAILY_YEAR';
			INSERT_TABLE_DELI := 'INSERT INTO INV_SAP_LIKP_LIPS_DAILY_YEAR
        SELECT *
        FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DBLINK
        WHERE CREATED_ON_DATE > SYSDATE - 365
        AND PLANTID                              IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
      EXECUTE IMMEDIATE TRUNCATE_TABLE_DELI;
    	EXECUTE IMMEDIATE INSERT_TABLE_DELI;
		END;   		
		
		Job Initialization: 
    CREATE TABLE INV_SAP_LIKP_LIPS_DAILY_YEAR AS
    SELECT * FROM INV_SAP_LIKP_LIPS_DAILY;
    TRUNCATE TABLE INV_SAP_LIKP_LIPS_DAILY_YEAR;
    INSERT INTO INV_SAP_LIKP_LIPS_DAILY_YEAR
    SELECT COUNT(*)
    FROM DWQ$LIBRARIAN.INV_SAP_LIKP_LIPS_DAILY@ROCKWELL_DBLINK
    WHERE CREATED_ON_DATE > SYSDATE - 365
    AND PLANTID                              IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');

    
    
		2.21 Job Name: Job_Daily_INV_SUM_STATS
		Job Time: 19:00 PM
		Job Repeat Frequency: Daily
		Job Procedure:
		DECLARE
			INSERT_TABLE_INVS  VARCHAR2(4000);
		BEGIN
			INSERT_TABLE_INVS := 'INSERT
      INTO INV_SAP_INV_SUM_STATS
        (
          PROC_TYPE,
          PLANT,
          MATERIAL,
          CATALOG_DASH,
          BU,
          UNIT_COST,
          STRATEGY_GRP,
          SAFETY_STOCK,
          LEAD_TIME,
          MATL_TYPE,
          MRP_TYPE,
          AVG13_USAGE_QTY,
          FC_AVG13_WEEK_QTY,
          AVG26_USAGE_QTY,
          FC_AVG26_WEEK_QTY,
          TOT_OPEN_QTY,
          SO_OPEN_VAL,
          PO_OPEN_QTY_ALL,
          STOCK_IN_TRANSIT_QTY,
          PO_OPEN_VAL,
          SIT_VAL,
          INV_TOTAL_QTY,
          INV_TOTAL_VAL,
          SNAPSHOT_DATE
        )
      SELECT PROC_TYPE,
        PLANT,
        MATERIAL,
        CATALOG_DASH,
        BU,
        CEIL(NVL(UNIT_COST,0)) AS UNIT_COST,
        STRATEGY_GRP,
        SAFETY_STOCK,
        LEAD_TIME,
        MATL_TYPE,
        MRP_TYPE,
        NVL(AVG13_USAGE_QTY,0) AS AVG13_USAGE_QTY,
        NVL(FC_AVG13_WEEK_QTY,0) AS FC_AVG13_WEEK_QTY,
        NVL(AVG26_USAGE_QTY,0) AS AVG26_USAGE_QTY,
        NVL(FC_AVG26_WEEK_QTY,0) AS FC_AVG26_WEEK_QTY,
        NVL(TOT_OPEN_QTY,0) AS TOT_OPEN_QTY,
        CEIL(NVL(SO_OPEN_VAL,0)) AS SO_OPEN_VAL,
        CEIL(NVL(PO_OPEN_QTY_ALL,0)) AS PO_OPEN_QTY_ALL,
        NVL(STOCK_IN_TRANSIT_QTY,0) AS STOCK_IN_TRANSIT_QTY,
        CEIL(NVL(PO_OPEN_VAL,0)) AS PO_OPEN_VAL,
        CEIL(NVL(SIT_VAL,0)) AS SIT_VAL,
        NVL(INV_TOTAL_QTY,0) AS INV_TOTAL_QTY,
        CEIL(NVL(INV_TOTAL_VAL,0)) AS INV_TOTAL_VAL,
        SYSDATE
      FROM VIEW_INV_SAP_INV_TREND
      WHERE KEY <> 0 AND PLANT  IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
        EXECUTE IMMEDIATE INSERT_TABLE_INVS;
		END;   		
		
		Job Initialization: 
      TRUNCATE TABLE INV_SAP_INV_SUM_STATS;
      --CREATE THE TABLE
      CREATE TABLE INV_SAP_INV_SUM_STATS AS
      SELECT PROC_TYPE,
        PLANT,
        MATERIAL,
        CATALOG_DASH,
        BU,
        CEIL(NVL(UNIT_COST,0)) AS UNIT_COST,
        STRATEGY_GRP,
        SAFETY_STOCK,
        LEAD_TIME,
        MATL_TYPE,
        MRP_TYPE,
        NVL(AVG13_USAGE_QTY,0) AS AVG13_USAGE_QTY,
        NVL(FC_AVG13_WEEK_QTY,0) AS FC_AVG13_WEEK_QTY,
        NVL(AVG26_USAGE_QTY,0) AS AVG26_USAGE_QTY,
        NVL(FC_AVG26_WEEK_QTY,0) AS FC_AVG26_WEEK_QTY,
        NVL(TOT_OPEN_QTY,0) AS TOT_OPEN_QTY,
        CEIL(NVL(SO_OPEN_VAL,0)) AS SO_OPEN_VAL,
        CEIL(NVL(PO_OPEN_QTY_ALL,0)) AS PO_OPEN_QTY_ALL,
        NVL(STOCK_IN_TRANSIT_QTY,0) AS STOCK_IN_TRANSIT_QTY,
        CEIL(NVL(PO_OPEN_VAL,0)) AS PO_OPEN_VAL,
        CEIL(NVL(SIT_VAL,0)) AS SIT_VAL,
        NVL(INV_TOTAL_QTY,0) AS INV_TOTAL_QTY,
        CEIL(NVL(INV_TOTAL_VAL,0)) AS INV_TOTAL_VAL
      FROM VIEW_INV_SAP_INV_TREND
      WHERE KEY <> 0
      AND PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140','5130','5150','5180');
      
      --INSERT THE RECORDS
      INSERT
      INTO INV_SAP_INV_SUM_STATS
        (
          PROC_TYPE,
          PLANT,
          MATERIAL,
          CATALOG_DASH,
          BU,
          UNIT_COST,
          STRATEGY_GRP,
          SAFETY_STOCK,
          LEAD_TIME,
          MATL_TYPE,
          MRP_TYPE,
          AVG13_USAGE_QTY,
          FC_AVG13_WEEK_QTY,
          AVG26_USAGE_QTY,
          FC_AVG26_WEEK_QTY,
          TOT_OPEN_QTY,
          SO_OPEN_VAL,
          PO_OPEN_QTY_ALL,
          STOCK_IN_TRANSIT_QTY,
          PO_OPEN_VAL,
          SIT_VAL,
          INV_TOTAL_QTY,
          INV_TOTAL_VAL,
          SNAPSHOT_DATE
        )
      SELECT PROC_TYPE,
        PLANT,
        MATERIAL,
        CATALOG_DASH,
        BU,
        CEIL(NVL(UNIT_COST,0)) AS UNIT_COST,
        STRATEGY_GRP,
        SAFETY_STOCK,
        LEAD_TIME,
        MATL_TYPE,
        MRP_TYPE,
        NVL(AVG13_USAGE_QTY,0) AS AVG13_USAGE_QTY,
        NVL(FC_AVG13_WEEK_QTY,0) AS FC_AVG13_WEEK_QTY,
        NVL(AVG26_USAGE_QTY,0) AS AVG26_USAGE_QTY,
        NVL(FC_AVG26_WEEK_QTY,0) AS FC_AVG26_WEEK_QTY,
        NVL(TOT_OPEN_QTY,0) AS TOT_OPEN_QTY,
        CEIL(NVL(SO_OPEN_VAL,0)) AS SO_OPEN_VAL,
        CEIL(NVL(PO_OPEN_QTY_ALL,0)) AS PO_OPEN_QTY_ALL,
        NVL(STOCK_IN_TRANSIT_QTY,0) AS STOCK_IN_TRANSIT_QTY,
        CEIL(NVL(PO_OPEN_VAL,0)) AS PO_OPEN_VAL,
        CEIL(NVL(SIT_VAL,0)) AS SIT_VAL,
        NVL(INV_TOTAL_QTY,0) AS INV_TOTAL_QTY,
        CEIL(NVL(INV_TOTAL_VAL,0)) AS INV_TOTAL_VAL,
        SYSDATE
      FROM VIEW_INV_SAP_INV_TREND
      WHERE KEY <> 0 AND PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140','5130','5150','5180');
      
      
      

     
                
          
          
    
    
    
    
    
    
    
    
    
    
  2.2 PLANTID-SALESORG MAPPING TABLE
  		CREATE TABLE INV_SAP_PP_PLANT_SAOG AS
      SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_T001W@ROCKWELL_DBLINK;
  
  2.2 INV_SAP_MRP_TYPE_DIS
      CREATE TABLE INV_SAP_MRP_TYPE_DIS AS
      SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MRP_TYPE_DIS@ROCKWELL_DBLINK;
  
  2.2 INV_SAP_MATL_TYPE_DIS
      CREATE TABLE INV_SAP_MATL_TYPE_DIS AS
      SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATL_TYPE_DIS@ROCKWELL_DBLINK;
  
  2.2 INV_SAP_MRP_CONTROLLER_DIS
    CREATE TABLE INV_SAP_MRP_CONTROLLER_DIS AS
    SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MRP_CONTROLLER_DIS@ROCKWELL_DBLINK;


  2.2 INV_SAP_PLAN_PARAM_REVIEW
    CREATE TABLE INV_SAP_PLAN_PARAM_REVIEW AS
    SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PLAN_PARAM_REVIEW@ROCKWELL_DBLINK;


--Weekly Jobs
	3.1 Job Name: Job_Weekly_INV_SO_HST
		Job Time: 20:00 AM
		Job Repeat Frequency: Weekly.Every Monday.
		Job Procedure:
		DECLARE
		  TRUNCATE_TABLE_SALES_HST  VARCHAR2(1000);
		  INSERT_TABLE_SALES_HST  VARCHAR2(1000);
		BEGIN
		  TRUNCATE_TABLE_SALES_HST   := 'TRUNCATE TABLE INV_SAP_SALES_HST';
		  INSERT_TABLE_SALES_HST := 'INSERT INTO INV_SAP_SALES_HST
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DBLINK WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||')';
		  EXECUTE IMMEDIATE TRUNCATE_TABLE_SALES_HST;
		  EXECUTE IMMEDIATE INSERT_TABLE_SALES_HST;
		END;
    		
		Job Initialization: 
		DROP TABLE INV_SAP_SALES_HST;
		CREATE TABLE INV_SAP_SALES_HST AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SALES_HST@ROCKWELL_DBLINK WHERE PLANTID IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140');


  3.2 Job Name: Job_Weekly_BK_STI -- Just back the Stock item metric weekly bu and item data
		Job Time: 8:00 AM
		Job Repeat Frequency: Weekly SAT
		Job Procedure:
		DECLARE
			TRUNCATE_BK_STI_WK  VARCHAR2(1000);
			TRUNCATE_BK_STI_ITEM  VARCHAR2(1000);
			INSERT_TABLE_BK_STI_WK  VARCHAR2(1000);
			INSERT_TABLE_BK_STI_ITEM  VARCHAR2(1000);
		BEGIN
			TRUNCATE_BK_STI_WK   := 'TRUNCATE TABLE STI_BY_BU_WEEK_TMP';
			TRUNCATE_BK_STI_ITEM := 'TRUNCATE TABLE STI_BY_ITEM_TMP';
			INSERT_TABLE_BK_STI_WK := 'INSERT INTO STI_BY_BU_WEEK_TMP SELECT * FROM STOCK_ITEM_STATUS_BY_BU_WEEK';
			INSERT_TABLE_BK_STI_ITEM := 'INSERT INTO STI_BY_ITEM_TMP SELECT * FROM STOCK_ITEM_STATUS_BY_ITEM';
			EXECUTE IMMEDIATE TRUNCATE_BK_STI_WK;
			EXECUTE IMMEDIATE TRUNCATE_BK_STI_ITEM;
			EXECUTE IMMEDIATE INSERT_TABLE_BK_STI_WK;
			EXECUTE IMMEDIATE INSERT_TABLE_BK_STI_ITEM;
		END;				
    
    Job Initialization: 
    TRUNCATE TABLE STI_BY_BU_WEEK_TMP;
    TRUNCATE TABLE STI_BY_ITEM_TMP;

    INSERT INTO STI_BY_BU_WEEK_TMP SELECT * FROM STOCK_ITEM_STATUS_BY_BU_WEEK;
    INSERT INTO STI_BY_ITEM_TMP AS SELECT * FROM STOCK_ITEM_STATUS_BY_ITEM;
				
        
  3.3 Job Name: Job_Weekly_INV_REC
		Job Time: 9:30 AM
		Job Repeat Frequency: Weekly.Every 5 times per week
		Job Procedure:
		DECLARE
    INSERT_TABLE_INV_REC  VARCHAR2(5000);
    BEGIN
      INSERT_TABLE_INV_REC := 'INSERT INTO INV_SAP_INV_REC
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
      (SELECT 
        SYSDATE||''_''||MATERIALID||''_''||PLANTID AS INV_ID,
        MATERIALID||''_''||PLANTID AS ID,
        SYSDATE AS REC_DATE,
        MATERIALID,
        PLANTID,
        LOCATIONID,
        OH_QTY
      FROM INV_SAP_INVENTORY_BY_PLANT WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||','||5180||','||5150||','||5130||')
      )INV
    LEFT JOIN
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
    ON PP.ID = INV.ID';
    EXECUTE IMMEDIATE INSERT_TABLE_INV_REC;
    END;
    		
		Job Initialization:    
    DROP TABLE INV_SAP_INV_REC;
    CREATE TABLE INV_SAP_INV_REC AS
    SELECT DISTINCT INV.INV_ID                                                             AS INV_ID,
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
      (SELECT SYSDATE
        ||'_'
        ||MATERIALID
        ||'_'
        ||PLANTID AS INV_ID,
        MATERIALID
        ||'_'
        ||PLANTID AS ID,
        SYSDATE   AS REC_DATE,
        MATERIALID,
        PLANTID,
        OH_QTY
      FROM INV_SAP_INVENTORY_BY_PLANT
      WHERE PLANTID IN ('||5040||', '||5050||', '||5100||', '||5110||', '||5120||', '||5160||', '||5190||', '||5200||','||5070||','||5140||','||5180||','||5150||','||5130||')
      )INV
    LEFT JOIN
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
      FROM INV_SAP_PP_OPT_X WHERE SAFETY_STOCK = 0
      )PP
    ON PP.ID = INV.ID;

--Monthly Jobs
	4.1 Job Name: Job_Monthly_INV_SHIPSOLD
		Job Time: Every month 9th 23:00 
		Job Repeat Frequency: Monthly
		Job Procedure:		
		DECLARE
		  DROP_TABLE_INV_SS  VARCHAR2(1000);
      CREATE_TABLE_INV_SS_DW  VARCHAR2(1000);
		  CREATE_TABLE_INV_SS_LO  VARCHAR2(1000);
      TRUN_INV_SHIP_SOLD  VARCHAR2(1000);
      INSERT_INV_SS_A  VARCHAR2(1000);
      INSERT_INV_SS_B  VARCHAR2(1000);
		BEGIN
		  DROP_TABLE_INV_SS   := 'TRUNCATE TABLE INV_SAP_SHIP_SOLD_TO_PARTYID';
		  CREATE_TABLE_INV_SS_DW := 'INSERT INTO INV_SAP_SHIP_SOLD_TO_PARTYID 
      SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_DBLINK';
      CREATE_TABLE_INV_SS_LO := 'INSERT INTO INV_SAP_SHIP_SOLD_TO_PARTYID
      SELECT * FROM SYSTEM.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_LOCAL_BK';
      TRUN_INV_SHIP_SOLD := 'TRUNCATE TABLE INV_SAP_SHIP_SOLD_TO';
      INSERT_INV_SS_A := 'INSERT INTO INV_SAP_SHIP_SOLD_TO 
          SELECT LPAD(SHIP_SOLD_TOPARTY,10,''0''),
          SHIP_TO_PARTY_NAME,
          SHIP_TO_CITY,
          SHIP_TO_POSTAL_CODE,
          SHIP_TO_COUNTRY
        FROM INV_SAP_SHIP_SOLD_TO_PARTYID WHERE SHIP_TO_PARTY_NAME IS NOT NULL';
      INSERT_INV_SS_B := 'INSERT INTO INV_SAP_SHIP_SOLD_TO 
        SELECT 
        LPAD(SHIP_SOLD_TOPARTY,10,''0''),
        SOLD_TO_PARTY_NAME,
        SOLD_TO_CITY,
        SOLD_TO_POSTAL_CODE,
        SOLD_TO_COUNTRY
        FROM INV_SAP_SHIP_SOLD_TO_PARTYID WHERE SHIP_TO_PARTY_NAME IS NULL';
		  EXECUTE IMMEDIATE DROP_TABLE_INV_SS;
		  EXECUTE IMMEDIATE CREATE_TABLE_INV_SS_DW;
      --EXECUTE IMMEDIATE CREATE_TABLE_INV_SS_LO;
      EXECUTE IMMEDIATE TRUN_INV_SHIP_SOLD;
      EXECUTE IMMEDIATE INSERT_INV_SS_A;
      EXECUTE IMMEDIATE INSERT_INV_SS_B;
		END;
   
		DROP TABLE INV_SAP_SHIP_SOLD_TO_PARTYID;
		CREATE TABLE INV_SAP_SHIP_SOLD_TO_PARTYID AS
		SELECT * FROM DWQ$LIBRARIAN.INV_SAP_SHIP_SOLD_TO_PARTYID@ROCKWELL_DBLINK;
    CREATE TABLE INV_SAP_SHIP_SOLD_TO_TMP AS SELECT * FROM INV_SAP_SHIP_SOLD_TO; -- Schedule BK in DW.
    
    
4.2 Job Name: Job_Monthly_INV_PP_CATA
		Job Time: 12:00 AM SAT
		Job Repeat Frequency: Monthly
		Job Procedure:
		DECLARE
		  CREATE_INV_SAP_MATL_CATA        VARCHAR2(1000);
		  DROP_INV_SAP_MATL_CATA            VARCHAR2(1000);
      DROP_INV_SAP_NODASH VARCHAR2(1000);
      CREATE_INV_SAP_NODASH VARCHAR2(1000);
      
		BEGIN
		  DROP_INV_SAP_MATL_CATA:= 'DROP TABLE INV_SAP_MATERIAL_CATALOG';
		  CREATE_INV_SAP_MATL_CATA:= 'CREATE TABLE INV_SAP_MATERIAL_CATALOG AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATERIAL_CATALOG@ROCKWELL_DBLINK';
		  EXECUTE IMMEDIATE DROP_INV_SAP_MATL_CATA;
		  EXECUTE IMMEDIATE CREATE_INV_SAP_MATL_CATA;
		END;
		
		Job Initialization: 
		DROP TABLE INV_SAP_MATERIAL_CATALOG;
		CREATE TABLE INV_SAP_MATERIAL_CATALOG AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATERIAL_CATALOG@ROCKWELL_DBLINK;

		DROP TABLE INV_SAP_NODASH_MAT_CATA;
		CREATE TABLE INV_SAP_NODASH_MAT_CATA AS SELECT * FROM INV_SAP_MATERIAL_CATALOG;
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
    
 
    
    
    
    DECLARE
      CREATE_INV_SAP_MATL_CATA        VARCHAR2(1000);
      DROP_INV_SAP_MATL_CATA            VARCHAR2(1000);
    BEGIN
      DROP_INV_SAP_MATL_CATA:= 'DROP TABLE INV_SAP_MATERIAL_CATALOG';
      CREATE_INV_SAP_MATL_CATA:= 'CREATE TABLE INV_SAP_MATERIAL_CATALOG AS SELECT * FROM DWQ$LIBRARIAN.INV_SAP_MATERIAL_CATALOG@ROCKWELL_DBLINK';
      EXECUTE IMMEDIATE DROP_TABLE;
      EXECUTE IMMEDIATE STR_CREATE_TABLE;
    END;