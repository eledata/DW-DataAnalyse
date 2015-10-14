CREATE OR REPLACE PROCEDURE INV_SAP_ANALYTICS
AS
  -------------------------------------------------------------------------------------------
  -------------------------------------------------------------------------------------------
  -- Procedure  Description:
  --
  --        This package controls the procesing of the contents of QWD_PROCEDURE_SQL based on
  --      the pacakage name passed to it.
  --
  -- Revision History:
  --
  --    Date        Developer         Description
  --    ---------   ---------------   -------------------------------------------
  --    2006-11-01  JMEDINA           Initial procedure creation .
  ---   2007-07-13  JMEDINA            CHANGE THE hIERARCHY SEPARATOR FOR ":"
  -------------------------------------------------------------------------------------------
  -------------------------------------------------------------------------------------------
  --  This procedure will extract data from DMI DB2 table through the DBLINK DMI daily inventory
  -------------------------------------------------------------------------------------------
  -------------------------------------------------------------------------------------------
  timeout_exception EXCEPTION;
  PRAGMA EXCEPTION_INIT (timeout_exception, -12535); -- ORA-12535 TNS Time OUT
  table_Missing EXCEPTION;
  PRAGMA EXCEPTION_INIT (table_Missing, -942); -- ORA-00942
  PROCESS_MONTH          NUMBER;
  EXTRACT_SHIP_DATE      DATE;
  EXTRACT_SHIP_DATE_CHAR VARCHAR(20);
  GBBB_EXTRACT_COUNT     NUMBER :=0;
  PARTITION_1            VARCHAR2(10);
  PARTITION_2            VARCHAR2(10);
  RECORD_COUNT           NUMBER;
  PROBLEM                VARCHAR2(2000);
  STMT_QUERY             VARCHAR2(2000);
  BEGIN_DATE             DATE;
  STEP_NAME              VARCHAR2(80);
  STEP_BEGIN_DATE        DATE;
  STEP_END_DATE          DATE;
  rawdate                DATE;
  PROCESS_TERMINATED     EXCEPTION;
  ROW_ERROR_COUNT        NUMBER;
  INSERT_COUNT           NUMBER;
  UPDATE_COUNT           NUMBER;
  REC_ID                 NUMBER;
  SCRT_GRP               VARCHAR2(40)  := 'INV_SAP_ANALYTICS';
  SCRT_IND               VARCHAR2(200) := SCRT_GRP;
  ORACLE_ERROR           VARCHAR2(2000);
  ORACLE_ERROR_GLOBAL    VARCHAR2(2000);
  STATUS                 VARCHAR2(3);
  DT_FORMAT              VARCHAR2(22) := 'YYYY-MM-DD HH24:MI:SS';
  Inventroy              NUMBER       :=0;
  PLANTCHECK             NUMBER       :=0;
  Dtldate                DATE;
  impoDate               DATE;
BEGIN
  DBMS_OUTPUT.ENABLE (100000);
  -- check if data has been selected in the last
  RECORD_COUNT := 0;
  Aa_Qdw_Script_Get_Seq (REC_ID);
  ROW_ERROR_COUNT := 0;
  INSERT_COUNT    := 0;
  UPDATE_COUNT    := 0;
  BEGIN_DATE      := SYSDATE;
  STATUS          := 'IP';
  Aa_Qdw_Script_Log (Rec_Id,Scrt_Grp,Scrt_Ind,Scrt_Ind || ' BEGIN' ,Sysdate,Status,0,NULL,True,'EXTRACT DATE',Sysdate, NULL,NULL);
  Dbms_Output.Put_Line('EXEC KEY '|| Rec_Id || ' BEGIN OF ' ||Scrt_Grp || ' STRORED PROC ' || TO_CHAR(Sysdate,'YYYY-MM-DD HH24:MI:SS') );
  BEGIN
    -- Build Standard Stock-Nonstock Table
    STEP_NAME      := 'Build Std Stock NonStock';
    STEP_BEGIN_DATE:=SYSDATE;
    Update_Count   :=0;
    RECORD_COUNT   := 0;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.Inv_Mpt_Stk_Nonstk_Test_Vbc ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.Inv_Mpt_Stk_Nonstk_Test_Vbc Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_CEDC ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_CEDC Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_CEDC_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_SA ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_SA Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_SA_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_SEA ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_SEA Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_SEA_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_SHG ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_SHG Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_SHG_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_KOR ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_KOR Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_KOR_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_IND ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_IND Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_IND_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_AUS ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_AUS Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_AUS_V');
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.MPT_STK_NONSTK_TEST_STD_TWN ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.MPT_STK_NONSTK_TEST_STD_TWN Select * From dwq$librarian.MPT_STK_NONSTK_TEST_STD_TWN_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
  EXCEPTION
  WHEN timeout_exception OR table_missing THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
  WHEN OTHERS THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
    COMMIT;
  END;
  NULL;
  BEGIN
    -- Build Standard Stock-Nonstock Table
    STEP_NAME      := 'Build CDC_MEM_DOH';
    STEP_BEGIN_DATE:=SYSDATE;
    Update_Count   :=0;
    RECORD_COUNT   := 0;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_CDC_MEM_DOH ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_CDC_MEM_DOH Select * From dwq$librarian.INV_SAP_CDC_MEM_DOH_V ');
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_DC_DOH ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_DC_DOH Select * From dwq$librarian.INV_SAP_DC_DOH_V ');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
  EXCEPTION
  WHEN timeout_exception OR table_missing THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
  WHEN OTHERS THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
    COMMIT;
  END;
  BEGIN
    -- Build Standard Stock-Nonstock Table
    STEP_NAME      := 'Build RISO Drop Down';
    STEP_BEGIN_DATE:=SYSDATE;
    Update_Count   :=0;
    RECORD_COUNT   := 0;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_VENDOR_NAME_DIS ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_VENDOR_NAME_DIS Select * From dwq$librarian.INV_SAP_VENDOR_NAME_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_STRAT_GRP_DIS ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_STRAT_GRP_DIS Select * From dwq$librarian.INV_SAP_STRAT_GRP_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_SPC_PROC_DIS ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_SPC_PROC_DIS Select * From dwq$librarian.INV_SAP_SPC_PROC_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_PURCH_GROUP_DIS ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_PURCH_GROUP_DIS Select * From dwq$librarian.INV_SAP_PURCH_GROUP_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_PROD_SCHEDULER_DIS ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_PROD_SCHEDULER_DIS Select * From dwq$librarian.INV_SAP_PROD_SCHEDULER_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_PROD_BU_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_PROD_BU_DIS Select * From dwq$librarian.INV_SAP_PROD_BU_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_PROC_TYPE_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_PROC_TYPE_DIS Select * From dwq$librarian.INV_SAP_PROC_TYPE_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_PART_FLAG_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_PART_FLAG_DIS Select * From dwq$librarian.INV_SAP_PART_FLAG_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_MRP_TYPE_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_MRP_TYPE_DIS Select * From dwq$librarian.INV_SAP_MRP_TYPE_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_MRP_CONTROLLER_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_MRP_CONTROLLER_DIS Select * From dwq$librarian.INV_SAP_MRP_CONTROLLER_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_MATL_TYPE_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_MATL_TYPE_DIS Select * From dwq$librarian.INV_SAP_MATL_TYPE_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_INV_CLASS_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_INV_CLASS_DIS Select * From dwq$librarian.INV_SAP_INV_CLASS_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_ANALY_SPEED_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_ANALY_SPEED_DIS Select * From dwq$librarian.INV_SAP_ANALY_SPEED_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_ABC_STD_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_ABC_STD_DIS Select * From dwq$librarian.INV_SAP_ABC_STD_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_REVIEW_FLAG_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_REVIEW_FLAG_DIS Select * From DWQ$LIBRARIAN.INV_SAP_REVIEW_FLAG_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_ULT_Source_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_ULT_Source_DIS Select * From DWQ$LIBRARIAN.INV_SAP_ULT_Source_DIS_V');
    COMMIT;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_ANALY_COST_DIS');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_ANALY_COST_DIS Select * From DWQ$LIBRARIAN.INV_SAP_ANALY_COST_DIS_V');
    COMMIT;
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
  EXCEPTION
  WHEN timeout_exception OR table_missing THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
  WHEN OTHERS THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
    COMMIT;
  END;
  SELECT COUNT(9),
    NVL(MAX(DW_DATE),sysdate-5)
  INTO RECORD_COUNT,
    rawdate
  FROM dwq$librarian.inv_sap_Vendor_Mat;
  IF rawdate <=sysdate -6 OR RECORD_COUNT = 0 THEN
    BEGIN
      -- Build Standard Stock-Nonstock Table
      STEP_NAME      := 'Build Vendor Part Number';
      STEP_BEGIN_DATE:=SYSDATE;
      Update_Count   :=0;
      RECORD_COUNT   := 0;
      Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.inv_sap_Vendor_Mat ');
      Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.inv_sap_Vendor_Mat Select * From dwq$librarian.inv_sap_Vendor_Mat_V');
      COMMIT;
      Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
      COMMIT;
    EXCEPTION
    WHEN timeout_exception OR table_missing THEN
      STATUS       := 'C';
      Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
      ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
      DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
      Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      COMMIT;
      DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
    WHEN OTHERS THEN
      STATUS       := 'C';
      Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
      Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
      COMMIT;
    END;
  END IF;
  --Obtain Address Information
  SELECT COUNT(9),
    NVL(MAX(DW_DATE),sysdate-5)
  INTO RECORD_COUNT,
    rawdate
  FROM dwq$librarian.INV_SAP_SHIP_SOLD_TO_PARTYID;
  IF rawdate    <=sysdate -6 OR RECORD_COUNT = 0 THEN
    RECORD_COUNT:=0;
    FOR UPDATE_COUNT IN 1..3
    LOOP
      IF RECORD_COUNT<=0 THEN -- once one goes through then check the MARC table them ignore the other attempts
        BEGIN
          STEP_NAME := 'DQD check '||UPDATE_COUNT ;
          DBMS_OUTPUT.PUT_LINE(SUBSTR( STEP_NAME,1,254) );
          --- check with Dual if the DB connetion is live...
          Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID) || ' , count(9), sysdate   from SAPECC_LIBRARIAN.ADRC@DQD.mke.ra.rockwell.com ');
          SELECT MAX(VALUE_NUMBER)
          INTO RECORD_COUNT
          FROM AAA_PROCEDURE_VALUE_EXCHANGE
          WHERE PROCEDURE_RECORD_ID = REC_ID ;
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
          COMMIT;
          IF RECORD_COUNT > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Exit dqd FOR check' );
            DBMS_OUTPUT.PUT_LINE('seleted  '||REC_ID || '   returned DQ ADRC =' || RECORD_COUNT );
            EXIT ;
          END IF ;
        EXCEPTION
        WHEN timeout_exception THEN
          RECORD_COUNT :=0;
          ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P', ORACLE_ERROR , UPDATE_COUNT);
          COMMIT;
        WHEN table_Missing THEN
          RECORD_COUNT :=0;
          ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P', ORACLE_ERROR , UPDATE_COUNT);
          COMMIT;
        WHEN OTHERS THEN
          RECORD_COUNT :=0;
          ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('other Tbl Missing '||ORACLE_ERROR,1,254));
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P', ORACLE_ERROR , UPDATE_COUNT);
          COMMIT;
        END ;
      END IF;
      COMMIT;
    END LOOP;
    IF RECORD_COUNT    > 0 THEN
      ROW_ERROR_COUNT := 0;
      UPDATE_COUNT    := 0;
      INSERT_COUNT    := 0;
      BEGIN_DATE      := SYSDATE;
      STATUS          := 'IP';
      BEGIN
        -- Build Standard Stock-Nonstock Table
        STEP_NAME      := 'Build VBPA Table';
        STEP_BEGIN_DATE:=SYSDATE;
        Update_Count   :=0;
        RECORD_COUNT   := 0;
        Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_VBPA_TEST_ID ');
        Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_VBPA_TEST_ID Select * From dwq$librarian.INV_SAP_VBPA_TEST_ID_V');
        Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
        COMMIT;
      EXCEPTION
      WHEN timeout_exception OR table_missing THEN
        STATUS       := 'C';
        Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
      WHEN OTHERS THEN
        STATUS       := 'C';
        Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
        COMMIT;
      END;
      BEGIN
        -- Build Standard Stock-Nonstock Table
        STEP_NAME      := 'Build ADRC Table';
        STEP_BEGIN_DATE:=SYSDATE;
        Update_Count   :=0;
        RECORD_COUNT   := 0;
        Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_ADRC_TEST ');
        Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_ADRC_TEST Select * From dwq$librarian.INV_SAP_ADRC_TEST_V');
        Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
        COMMIT;
      EXCEPTION
      WHEN timeout_exception OR table_missing THEN
        STATUS       := 'C';
        Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
      WHEN OTHERS THEN
        STATUS       := 'C';
        Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
        COMMIT;
      END;
      BEGIN
        STEP_NAME      := 'Build ADDRESS Table';
        STEP_BEGIN_DATE:=SYSDATE;
        Update_Count   :=0;
        RECORD_COUNT   := 0;
        Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_SHIP_SOLD_TO_PARTYID ');
        Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_SHIP_SOLD_TO_PARTYID Select * From dwq$librarian.INV_SAP_SHIP_SOLD_TO_PARTYID_V');
        Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
        COMMIT;
      EXCEPTION
      WHEN timeout_exception OR table_missing THEN
        STATUS       := 'C';
        Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
      WHEN OTHERS THEN
        STATUS       := 'C';
        Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
        COMMIT;
      END;
    ELSE
      STATUS              := 'C';
      ORACLE_ERROR_GLOBAL := 'Several attemps in opening the Dblink Failed' || ORACLE_ERROR_GLOBAL;
    END IF; ---- check the DBLink is open for business :)
  ELSE
    STATUS := 'P';
  END IF;
  BEGIN
    STEP_NAME      := 'Build QMEL Table';
    STEP_BEGIN_DATE:=SYSDATE;
    Update_Count   :=0;
    RECORD_COUNT   := 0;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_QA_QMEL ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_QA_QMEL Select * From dwq$librarian.INV_SAP_QA_QMEL_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
  EXCEPTION
  WHEN timeout_exception OR table_missing THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
  WHEN OTHERS THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
    COMMIT;
  END;
  BEGIN
    STEP_NAME      := 'Build Warehouse Table';
    STEP_BEGIN_DATE:=SYSDATE;
    Update_Count   :=0;
    RECORD_COUNT   := 0;
    Aaa_Qdw_Script_Exec_Immediate ('truncate table dwq$librarian.INV_SAP_WAREHOUSE_MASTER ');
    Aaa_Qdw_Script_Exec_Immediate ('Insert Into dwq$librarian.INV_SAP_WAREHOUSE_MASTER Select * From dwq$librarian.INV_SAP_WAREHOUSE_MASTER_V');
    Aa_Qdw_Script_Step_Log (Rec_Id,Step_Name ,Step_Begin_Date,'P',NULL, Update_Count);
    COMMIT;
  EXCEPTION
  WHEN timeout_exception OR table_missing THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
  WHEN OTHERS THEN
    STATUS       := 'C';
    Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack,1,250) || SUBSTR(Dbms_Utility.Format_Call_Stack,1,254);
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
    DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,250) ) ;
    COMMIT;
  END;
  STATUS := 'P';
  Aa_Qdw_Script_Log (Rec_Id,Scrt_Grp,Scrt_Ind,Scrt_Ind || ' BEGIN' ,Sysdate,Status,0,NULL,True,'EXTRACT DATE',Sysdate, NULL,NULL);
EXCEPTION
WHEN timeout_exception OR table_Missing THEN
  STATUS       := 'C';
  Oracle_Error := SUBSTR(Dbms_Utility.Format_Error_Stack || Dbms_Utility.Format_Call_Stack,1,254);
  Dbms_Output.Put_Line(Oracle_Error) ;
  Aa_Qdw_Script_Log (Rec_Id,Scrt_Grp,Scrt_Ind,Scrt_Ind || ' BEGIN' ,Sysdate,Status,0,NULL,True,'EXTRACT DATE',Sysdate, NULL,NULL);
END INV_SAP_ANALYTICS;