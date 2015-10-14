CREATE OR REPLACE PROCEDURE INV_SAP_DQD_PARAM_OPTIMIZATION
IS
  -------------------------------------------------------------------------------------------
  -------------------------------------------------------------------------------------------
  -- Procedure  Description:
  --
  --  This package controls the procesing of the contents of QWD_PROCEDURE_SQL based on
  --   the pacakage name passed to it.
  --
  -- Revision History:
  --
  --    Date        Developer         Description
  --    ---------   ---------------   -------------------------------------------
  --    2006-11-01  JMEDINA           Initial procedure creation .
  ---   2007-07-13  JMEDINA   CHANGE THE hIERARCHY SEPARATOR FOR ":"
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
  PROCESS_TERMINATED     EXCEPTION;
  ROW_ERROR_COUNT        NUMBER;
  INSERT_COUNT           NUMBER;
  UPDATE_COUNT           NUMBER;
  REC_ID                 NUMBER;
  SCRT_GRP               VARCHAR2(40)  := 'INV_SAP_DQD_PARAM_OPTIMIZATION';
  SCRT_IND               VARCHAR2(200) := SCRT_GRP;
  ORACLE_ERROR           VARCHAR2(2000);
  ORACLE_ERROR_GLOBAL    VARCHAR2(2000);
  STATUS                 VARCHAR2(3);
  DT_FORMAT              VARCHAR2(22) := 'YYYY-MM-DD HH24:MI:SS';
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
  Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,SCRT_IND || ' BEGIN' ,SYSDATE,STATUS,0,NULL,TRUE,'EXTRACT DATE',SYSDATE, NULL,NULL);
  DBMS_OUTPUT.PUT_LINE('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') );
  -------------------------------------------------------------------------------------------------------------------------------
  -----------------------  BUILD Material growth table             -------------------------------------------------------------------------
  -----------------------------------------------------------------------------------------------------------------------------------
  BEGIN
    STEP_NAME      := 'BuildOptimization table ';
    STEP_BEGIN_DATE:=SYSDATE;
    UPDATE_COUNT   :=0;
    SELECT COUNT(9)
    INTO RECORD_COUNT ---expect 0 if the table has not been update in the last 16 hours
    FROM dwq$librarian.INV_SAP_PP_OPTIMIZATION2 ;
    IF RECORD_COUNT > 1 THEN
      RECORD_COUNT := 0;
      SELECT COUNT(9)
      INTO RECORD_COUNT ---expect 0 if the table has not been update in the last 16 hours
      FROM dwq$librarian.INV_SAP_PP_MATERIAL_GROWTH
      WHERE dw_date > sysdate - .1;
      SELECT COUNT(9)
      INTO UPDATE_COUNT
      FROM dwq$librarian.INV_SAP_PP_SERVICE_LEVELS
      WHERE CHANGED_DATE > sysdate - .1;-- in in the last 2 hour
    ELSE
      UPDATE_COUNT :=1;
      RECORD_COUNT :=1;
    END IF;
    --- rebuild the table growth only when there has been 16 hours without rebuilding it
    -- or if the control and setting table has been modified in the last 3 hours
    IF UPDATE_COUNT > 0 OR RECORD_COUNT > 0 THEN
      Aaa_Qdw_Script_Exec_Immediate ('truncate table  DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION2');
      COMMIT;
      INSERT INTO DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION2
      SELECT * FROM DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION_V;
      UPDATE_COUNT := UPDATE_COUNT + SQL%ROWCOUNT;
      COMMIT;
      Aaa_Qdw_Script_Exec_Immediate ('analyze table DWQ$LIBRARIAN.INV_SAP_PP_OPTIMIZATION2 estimate  statistics sample 40 percent ');
      Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_PP_OPTIMIZATION  to INV_SAP_PP_OPTIMIZATION3 ');
      Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_PP_OPTIMIZATION2 to INV_SAP_PP_OPTIMIZATION ');
      Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_PP_OPTIMIZATION3 to INV_SAP_PP_OPTIMIZATION2 ');
      COMMIT;
      Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      COMMIT;
    END IF;
  EXCEPTION
  WHEN timeout_exception OR table_Missing THEN
    STATUS       := 'C';
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK || DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(ORACLE_ERROR) ;
    COMMIT;
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C',NULL, UPDATE_COUNT);
  WHEN OTHERS THEN
    STATUS       := 'C';
    ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK || DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
    DBMS_OUTPUT.PUT_LINE(ORACLE_ERROR ) ;
    Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C',NULL, UPDATE_COUNT);
  END;
  IF STATUS = 'IP' THEN
    STATUS := 'P';
  ELSE
    Aa_Qdw_Admin_Script_Email ( REC_ID,SCRT_GRP || ' STATUS ' ||STATUS ,'Automatic Notification Error '||CHR(13) ||'Stored Proc '||SCRT_GRP||'@QDWD ' || CHR(13)||ORACLE_ERROR_GLOBAL);
  END IF;
  Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,SCRT_IND || ' ENDED' ,SYSDATE,STATUS,INSERT_COUNT + UPDATE_COUNT,ORACLE_ERROR_GLOBAL,FALSE,NULL,NULL,NULL,NULL);
  DBMS_OUTPUT.PUT_LINE('END OF '||SCRT_GRP||' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS')) ;
  COMMIT;
EXCEPTION
WHEN OTHERS THEN
  ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK || DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  DBMS_OUTPUT.PUT_LINE(ORACLE_ERROR) ;
END;