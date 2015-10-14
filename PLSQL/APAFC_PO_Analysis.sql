CREATE OR REPLACE PROCEDURE INV_SAP_DQD_PO_IMPORT
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
  --    2010-09-10   JMEDINA           Initial procedure creation .
  --    2010-09-30   jmedina          change to use execute inmediate... the procedure becomes invalid if exceuted and the dblink is
  ---                                 is not valid ... can not be executes again.... with the execute inmediate it is
  ----                                validated at runtime...
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
  RECORD_COUNT2          NUMBER;
  RECORD_COUNT3          NUMBER;
  RECORD_COUNT4          NUMBER;
  RECORD_COUNT5          NUMBER;
  RECORD_COUNT6          NUMBER;
  RECORD_COUNT7          NUMBER;
  RECORD_COUNT8          NUMBER;
  RECORD_COUNT9          NUMBER;
  RECORD_COUNT10         NUMBER;
  RECORD_COUNT15         NUMBER;
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
  HOLDER1                NUMBER;
  REC_ID                 NUMBER;
  INSERT_COUNT1          NUMBER;
  SCRT_GRP               VARCHAR2(40)  := 'INV_SAP_DQD_PO_IMPORT';
  SCRT_IND               VARCHAR2(200) := 'INV_SAP_DQD_PO_IMPORT';
  ORACLE_ERROR           VARCHAR2(2000);
  ORACLE_ERROR_GLOBAL    VARCHAR2(2000);
  STATUS                 VARCHAR2(3);
  DT_FORMAT              VARCHAR2(22) := 'YYYY-MM-DD HH24:MI:SS';
  numofdays              NUMBER       := .5;
  EKBEDATE               DATE;
  DQDEKBEDATE            DATE;
  STMT_QUERY2            VARCHAR2(2000);
  STMT_QUERY3            VARCHAR2(2000);
BEGIN
  DBMS_OUTPUT.ENABLE (100000);
  Aa_Qdw_Script_Get_Seq (REC_ID);
  DBMS_OUTPUT.PUT_LINE('Step1 insert value checking the db ' );
  RECORD_COUNT   := 0;
  RECORD_COUNT2  := 0;
  RECORD_COUNT3  := 0;
  RECORD_COUNT4  := 0;
  RECORD_COUNT5  := 0;
  RECORD_COUNT6  := 0;
  RECORD_COUNT7  := 0;
  RECORD_COUNT8  := 0;
  RECORD_COUNT9  := 0;
  RECORD_COUNT10 := 0;
  RECORD_COUNT15 := 0;
  HOLDER1        :=0;
  INSERT_COUNT1  :=0;
  -- select count(9),max(dw_date)  into RECORD_COUNT, BEGIN_DATE
  --  from dwq$librarian.INV_SAP_PO_EKKO
  -- where trunc(dw_date) >=trunc(sysdate);
  FOR UPDATE_COUNT IN 1..3
  LOOP
    IF RECORD_COUNT9<=0 THEN -- once one goes through then check the EKET table them ignore the other attempts
      BEGIN
        -- check with if the table EKET is already there
        STMT_QUERY2:= ' (   EINDT >= TO_CHAR (SYSDATE - 1100, ''yyyymmdd'') OR BEDAT >= TO_CHAR (SYSDATE - 1100, ''yyyymmdd'') OR TRIM (EBELN) LIKE ''48%'')';
        DBMS_OUTPUT.PUT_LINE(SUBSTR(STMT_QUERY2,1,254));
        STMT_QUERY3:= 'select '||TO_CHAR( REC_ID) || ' ,  SUM(NVL(menge,0)) PO_QTY, sysdate from SAPECC_DLY_LIBRARIAN.EKET@DQD.mke.ra.rockwell.com WHERE '|| STMT_QUERY2;
        STMT_QUERY := 'insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) '||STMT_QUERY3;
        --CPZ DBMS_OUTPUT.PUT_LINE(STMT_QUERY  );
        DBMS_OUTPUT.PUT_LINE(SUBSTR(STMT_QUERY,1,200) );   --CPZ added to limit exception 20000 line too long
        DBMS_OUTPUT.PUT_LINE(SUBSTR(STMT_QUERY,201,200) ); --CPZ added to limit exception 20000 line too long
        Aaa_Qdw_Script_Exec_Immediate (STMT_QUERY);
        COMMIT;
        SELECT MAX(VALUE_NUMBER)
        INTO RECORD_COUNT9
        FROM AAA_PROCEDURE_VALUE_EXCHANGE
        WHERE PROCEDURE_RECORD_ID = REC_ID ;
        COMMIT;
        SELECT SUM(mengescheduledqty)
        INTO INSERT_COUNT1
        FROM dwq$librarian.INV_SAP_PO_EKET ;
        DBMS_OUTPUT.PUT_LINE('seleted  '||REC_ID || '   returned DQ PO_qty =' || RECORD_COUNT ||'  qdws PO_qty = '|| INSERT_COUNT );
        COMMIT;
        IF RECORD_COUNT9 <> INSERT_COUNT1 THEN
          DBMS_OUTPUT.PUT_LINE('Exit dqd FOR check' );
          EXIT ;
        END IF ;
        DBMS_OUTPUT.PUT_LINE('seleted  '||REC_ID || '   returned DQ PO_qty =' || RECORD_COUNT ||'  qdws PO_qty = '|| INSERT_COUNT );
      EXCEPTION
      WHEN timeout_exception THEN
        RECORD_COUNT9:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,STEP_NAME,SYSDATE,'C',0,ORACLE_ERROR_GLOBAL,FALSE,NULL,NULL,NULL,NULL);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,240)) ;
        COMMIT;
      WHEN table_Missing THEN
        RECORD_COUNT9:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,STEP_NAME,SYSDATE,'C',0,ORACLE_ERROR_GLOBAL,FALSE,NULL,NULL,NULL,NULL);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,240)) ;
        COMMIT;
      WHEN OTHERS THEN
        RECORD_COUNT9:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,240)) ;
        COMMIT;
      END ;
    END IF;
    COMMIT;
  END LOOP;
  --Aa_Qdw_Script_Get_Seq (REC_ID);
  --  select count(9),max(dw_date)  into RECORD_COUNT, BEGIN_DATE
  --  from dwq$librarian.INV_SAP_PO_EKKO
  -- where dw_date >=sysdate -.5 ;
  --  select count(9)  into RECORD_COUNT2
  -- from dwq$librarian.INV_SAP_PO_PLAF
  --  where dw_date >=sysdate -.5 ;
  --  select count(9) into RECORD_COUNT3
  --   from dwq$librarian.INV_SAP_PO_EKET
  --  where dw_date >=sysdate -.5 ;
  --    begin
  --             STEP_NAME := 'Check Daily PO Tables';
  --             DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
  --             STEP_BEGIN_DATE:=SYSDATE;
  --             UPDATE_COUNT :=0;
  --               UPDATE_COUNT    := 0;
  --
  --
  --             DBMS_OUTPUT.PUT_LINE(substr(STMT_QUERY3,1,2000));
  --              Aaa_Qdw_Script_Exec_Immediate (STMT_QUERY3);
  --              DBMS_OUTPUT.PUT_LINE(substr(STMT_QUERY3,1,2000));
  --
  --
  --
  --               select  SUM(NVL(menge,0)) PO_QTY into RECORD_COUNT9 from SAPECC_DLY_LIBRARIAN.EKET@DQD.mke.ra.rockwell.com WHERE  (   EINDT >= TO_CHAR (SYSDATE - 1100, 'yyyymmdd') OR BEDAT >= TO_CHAR (SYSDATE - 1100, 'yyyymmdd') OR TRIM (EBELN) LIKE '48%');
  --
  --             select sum(nvl(MENGESCHEDULEDQTY,0)) into INSERT_COUNT1 from INV_SAP_PO_EKET;
  --
  --         exception
  --                  when timeout_exception then
  --                       RECORD_COUNT9:=0;
  --                       ORACLE_ERROR :=  SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  --                        Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,STEP_NAME,SYSDATE,'C',0,ORACLE_ERROR_GLOBAL,FALSE,NULL,NULL,NULL,NULL);
  --                                    DBMS_OUTPUT.PUT_LINE(substr(ORACLE_ERROR,1,240)) ;
  --           commit;
  --              when table_Missing then
  --                      RECORD_COUNT9:=0;
  --                       ORACLE_ERROR :=  SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  --                        Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,STEP_NAME,SYSDATE,'C',0,ORACLE_ERROR_GLOBAL,FALSE,NULL,NULL,NULL,NULL);
  --                                        DBMS_OUTPUT.PUT_LINE(substr(ORACLE_ERROR,1,240)) ;
  --                           commit;
  --         when others then
  --                       ORACLE_ERROR :=  SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  --                       DBMS_OUTPUT.PUT_LINE(substr(ORACLE_ERROR,1,254));
  --                       Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
  --         end ;
  --
  --      begin
  --             STEP_NAME := 'Check Weekly PO Tables';
  --             DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
  --             STEP_BEGIN_DATE:=SYSDATE;
  --             UPDATE_COUNT :=0;
  --               UPDATE_COUNT    := 0;
  --             select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKBE@DQD.mke.ra.rockwell.com;
  --             select max(TO_DATE (TRIM (ekbe.BUDAT), 'yyyymmdd')) into DQDEKBEDATE from SAPECC_DLY_LIBRARIAN.EKBE@DQD.mke.ra.rockwell.com WHERE (BLDAT >= TO_CHAR (SYSDATE - 1100, 'yyyymmdd')
  --           OR TRIM (ekbe.EBELN) LIKE '48%') AND BWART IN('101', '102', '121', '131', '132', '141', '142', '301');
  --             select max(budatpostingdate) into EKBEDATE from INV_SAP_PO_EKBE;
  --
  --             if EKBEDATE >= DQDEKBEDATE then
  --
  --             select 1 into RECORD_COUNT4 from dual;
  --             --- data was already extracted it will not extracted again
  --
  --             DBMS_OUTPUT.PUT_LINE('Todays extract already completed ');
  --
  --             else
  --
  --
  --              commit;
  --
  --               end if;
  --         exception
  --         when timeout_exception then
  --                       --RECORD_COUNT:=0;
  --                       ORACLE_ERROR :=  SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  --                        DBMS_OUTPUT.PUT_LINE(substr('Exception timeout '||ORACLE_ERROR,1,254));
  --                        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
  --                      commit;
  --              when table_Missing then
  --                     -- RECORD_COUNT:=0;
  --                       ORACLE_ERROR :=  SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  --                         DBMS_OUTPUT.PUT_LINE(substr('Exception Tbl Missing '||ORACLE_ERROR,1,254));
  --                       Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
  --                      commit;
  --         when others then
  --                       ORACLE_ERROR :=  SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
  --                       DBMS_OUTPUT.PUT_LINE(substr(ORACLE_ERROR,1,254));
  --                       Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
  --         end ;
  --select count(9)  into RECORD_COUNT5
  -- from dwq$librarian.INV_SAP_PO_EKPO
  --  where dw_date >=sysdate -.5 ;
  --select count(9) into RECORD_COUNT6
  --from dwq$librarian.INV_SAP_PO_EBAN
  --where dw_date >=sysdate -6.5 ;
  --select count(9) into RECORD_COUNT7
  --from dwq$librarian.INV_SAP_PO_EKES
  --where dw_date >=sysdate -6.5 ;
  --  select count(9) into RECORD_COUNT8
  --  from dwq$librarian.INV_SAP_PO_EINE
  --   where dw_date >=sysdate -.5 ;
  ----- check
  IF (RECORD_COUNT9 = INSERT_COUNT1) OR record_count9 = 0 THEN
    --- data was already extracted it will not extracted again
    DBMS_OUTPUT.PUT_LINE(SUBSTR(' Todays extract already completed '|| RECORD_COUNT || 'records at ' ||TO_CHAR(BEGIN_DATE,'yyyymmdd hh24:mi') || ' SP execution date: ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') ,1,254));
  ELSE
    STEP_NAME := 'DQD check '||UPDATE_COUNT ;
    DBMS_OUTPUT.PUT_LINE(SUBSTR( STEP_NAME,1,254) );
    ROW_ERROR_COUNT := 0;
    INSERT_COUNT    := 0;
    UPDATE_COUNT    := 0;
    BEGIN_DATE      := SYSDATE;
    STATUS          := 'IP';
    Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,SCRT_IND || ' BEGIN' ,SYSDATE,STATUS,0,NULL,TRUE,'EXTRACT DATE',SYSDATE, NULL,NULL);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,250) );
    -----------------------------------------
    -----------------------------------------
    --  OPEN THE DB LINK sometimes it is closed and you have to query it to open it... i may take a few minutes
    ----------------------------------------
    RECORD_COUNT:=0;
    FOR UPDATE_COUNT IN 1..3
    LOOP
      IF RECORD_COUNT<=0 THEN -- once one goes through then check the MARC table them ignore the other attempts
        BEGIN
          STEP_NAME := 'DQD check '||UPDATE_COUNT ;
          DBMS_OUTPUT.PUT_LINE(SUBSTR( STEP_NAME,1,254) );
          --- check with Dual if the DB connetion is live...
          Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID) || ' , count(9), sysdate   from SAPECC_DLY_LIBRARIAN.PLAF@DQD.mke.ra.rockwell.com ');
          SELECT MAX(VALUE_NUMBER)
          INTO RECORD_COUNT
          FROM AAA_PROCEDURE_VALUE_EXCHANGE
          WHERE PROCEDURE_RECORD_ID = REC_ID ;
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
          COMMIT;
          IF RECORD_COUNT > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Exit dqd FOR check' );
            DBMS_OUTPUT.PUT_LINE('seleted  '||REC_ID || '   returned DQ PLAF =' || RECORD_COUNT );
            EXIT ;
          END IF ;
        EXCEPTION
        WHEN timeout_exception THEN
          RECORD_COUNT :=0;
          ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
          COMMIT;
        WHEN table_Missing THEN
          RECORD_COUNT :=0;
          ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
          DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
          Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
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
    -----------------------------------------
    -----------------------------------------
    --  if the db lik worked them go for the insert
    ----------------------------------------
    STEP_NAME := 'Check to see if Extract is required';
    DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
    IF RECORD_COUNT    > 0 OR RECORD_COUNT = 0 THEN -- CPZ added REcord_count = 0 02-01-2012
      ROW_ERROR_COUNT := 0;
      UPDATE_COUNT    := 0;
      INSERT_COUNT    := 0;
      BEGIN_DATE      := SYSDATE;
      STATUS          := 'IP';
      Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,SCRT_IND || ' BEGIN' ,SYSDATE,STATUS,0,NULL,TRUE,'EXTRACT DATE',SYSDATE, NULL,NULL);
      DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
      -----------------------------------------------------------
      ---   PLAF
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract PO PLAF';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.PLAF@DQD.mke.ra.rockwell.com;  --CPZ 02-01-2012
        --Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_DLY_LIBRARIAN.PLAF@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_PLAF');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_PLAF select * from INV_SAP_PO_PLAF_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_TP_CONVERT_CAPACITY2');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_TP_CONVERT_CAPACITY2 select * from INV_SAP_TP_CONVERT_CAPACITY_V');
        COMMIT;
        SELECT COUNT(9)
        INTO RECORD_COUNT10
        FROM dwq$librarian.INV_SAP_TP_CONVERT_CAPACITY2; --CPZ 09-13-2013 For Load Report to not query of truncated Capacity Table
        IF RECORD_COUNT10 > 0 THEN
          Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_TP_CONVERT_CAPACITY to INV_SAP_TP_CONVERT_CAPACITY3 ');
          Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_TP_CONVERT_CAPACITY2 to INV_SAP_TP_CONVERT_CAPACITY  ');
          Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_TP_CONVERT_CAPACITY3 to INV_SAP_TP_CONVERT_CAPACITY2 ');
          UPDATE_COUNT := SQL%ROWCOUNT;
        ELSE
          DBMS_OUTPUT.PUT_LINE(SUBSTR(STEP_NAME || '1-' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        END IF;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---   EKET
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract PO EKET';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        -- select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKET@DQD.mke.ra.rockwell.com;
        --  Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_DLY_LIBRARIAN.eket@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKET');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKET  select  * from  dwq$librarian.INV_SAP_PO_EKET_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      BEGIN
        STEP_NAME      := 'Obtain LIKP LIPS DAILY ';
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        DBMS_OUTPUT.PUT_LINE(SUBSTR(STEP_NAME|| ' begin  ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aaa_Qdw_Script_Exec_Immediate ('DROP INDEX  IDX_INV_SAP_LIKP_TRIM_DLY');
        Aaa_Qdw_Script_Exec_Immediate ('DROP INDEX  IDX_INV_SAP_LIKP_DLY');
        Aaa_Qdw_Script_Exec_Immediate ('DROP INDEX  IDX_INV_SAP_LIKP_DLY_DELV');
        Aaa_Qdw_Script_Exec_Immediate ('truncate table INV_SAP_LIKP_LIPS_DAILY');
        INSERT /* + append */
        INTO INV_SAP_LIKP_LIPS_DAILY
        SELECT * FROM INV_SAP_LIKP_LIPS_DAILY_V nologging; --CPZ 01-20-2012 needs nologging
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        --select count(9) into Record_Count15 from INV_SAP_LIKP_LIPS_DAILY;
        --  if RECORD_COUNT15 > 0 then
        -- Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_LIKP_LIPS to INV_SAP_LIKP_LIPS3');
        --                     Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_LIKP_LIPS2  to INV_SAP_LIKP_LIPS');
        --                  Aaa_Qdw_Script_Exec_Immediate ('rename INV_SAP_LIKP_LIPS3 to INV_SAP_LIKP_LIPS2 ');
        --  end if;
        Aaa_Qdw_Script_Exec_Immediate ('CREATE INDEX IDX_INV_SAP_LIKP_TRIM_DLY ON INV_SAP_LIKP_LIPS_DAILY (REFERENCE_DOC_TRIM,REFERENCE_DOC_ITEM_TRIM) NOLOGGING NOPARALLEL COMPUTE STATISTICS');
        Aaa_Qdw_Script_Exec_Immediate ('CREATE INDEX IDX_INV_SAP_LIKP_DLY ON INV_SAP_LIKP_LIPS_DAILY (REFERENCE_DOC,REFERENCE_DOC_ITEM) NOLOGGING NOPARALLEL COMPUTE STATISTICS');
        Aaa_Qdw_Script_Exec_Immediate ('CREATE INDEX IDX_INV_SAP_LIKP_DLY_DELV ON INV_SAP_LIKP_LIPS_DAILY (DELIVERY) NOLOGGING NOPARALLEL COMPUTE STATISTICS');
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR(STEP_NAME|| 'complete ' || UPDATE_COUNT || '    '|| TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---  EKBE
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract PO EKBE';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKBE@DQD.mke.ra.rockwell.com;
        --  select max(TO_DATE (TRIM (ekbe.BUDAT), 'yyyymmdd')) into DQDEKBEDATE from SAPECC_DLY_LIBRARIAN.EKBE@DQD.mke.ra.rockwell.com WHERE (BLDAT >= TO_CHAR (SYSDATE - 1100, 'yyyymmdd')
        --OR TRIM (ekbe.EBELN) LIKE '48%') AND BWART IN('101', '102', '121', '131', '132', '141', '142', '301');
        --select max(budatpostingdate) into EKBEDATE from INV_SAP_PO_EKBE;
        --if EKBEDATE >= DQDEKBEDATE then
        --- data was already extracted it will not extracted again
        --DBMS_OUTPUT.PUT_LINE('Todays extract already completed ');
        --else
        -- Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_LIBRARIAN.ekbe@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKBE');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKBE  select * from dwq$librarian.INV_SAP_PO_EKBE_V ');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
        -- end if;
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---   EKKO
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract PO EKKO';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        -- select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKKO@DQD.mke.ra.rockwell.com;
        --  Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_DLY_LIBRARIAN.ekko@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKKO');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKKO select * from INV_SAP_PO_EKKO_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---   EKPO
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract PO EKPO';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKPO@DQD.mke.ra.rockwell.com;
        --Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_DLY_LIBRARIAN.EKPO@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKPO');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKPO  select * from INV_SAP_PO_EKPO_V');
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---   EKPO
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract PO EKPV';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKPV@DQD.mke.ra.rockwell.com;
        --Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_DLY_LIBRARIAN.EKPV@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKPV');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKPV  select * from INV_SAP_PO_EKPV_V');
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---   EBAN
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract Pur Req PO EBAN';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        -- if RECORD_COUNT4 = 0 then
        --select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EBAN@DQD.mke.ra.rockwell.com;
        -- Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_LIBRARIAN.EBAN@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EBAN');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EBAN  select * from INV_SAP_PO_EBAN_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
        -- else
        --DBMS_OUTPUT.PUT_LINE('No Update Needed');
        --end if;
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---   EKES
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract Pur Req PO EKES';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --  if RECORD_COUNT4 = 0 then
        -- select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKES@DQD.mke.ra.rockwell.com;
        -- Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_LIBRARIAN.EKES@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKES');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKES  select * from INV_SAP_PO_EKES_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
        --else
        -- DBMS_OUTPUT.PUT_LINE('No Update Needed');
        --end if;
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      -----------------------------------------------------------
      ---  Non Critical Extract PIR
      -----------------------------------------------------------
      BEGIN
        STEP_NAME := 'Extract EKKN';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --  if RECORD_COUNT4 = 0 then
        -- select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKES@DQD.mke.ra.rockwell.com;
        -- Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_LIBRARIAN.EKES@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKKN');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKKN  select * from INV_SAP_PO_EKKN_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
        --else
        -- DBMS_OUTPUT.PUT_LINE('No Update Needed');
        --end if;
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      BEGIN
        STEP_NAME := 'Extract EKKN-EKBE';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        --  if RECORD_COUNT4 = 0 then
        -- select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EKES@DQD.mke.ra.rockwell.com;
        -- Aaa_Qdw_Script_Exec_Immediate ('insert into AAA_PROCEDURE_VALUE_EXCHANGE (PROCEDURE_RECORD_ID, VALUE_NUMBER,CREATED_DATE) select '|| TO_CHAR(REC_ID+1) || ' , count(9), sysdate   from SAPECC_LIBRARIAN.EKES@DQD.mke.ra.rockwell.com ');
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EKKNEKBE');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EKKNEKBE  select * from dwq$librarian.INV_SAP_PO_EKKNEKBE_V');
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
        --else
        -- DBMS_OUTPUT.PUT_LINE('No Update Needed');
        --end if;
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
      BEGIN
        STEP_NAME := 'Extract PO EINA-EINE';
        DBMS_OUTPUT.PUT_LINE( '' || STEP_NAME );
        STEP_BEGIN_DATE:=SYSDATE;
        UPDATE_COUNT   :=0;
        UPDATE_COUNT   := 0;
        -- select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EINE@DQD.mke.ra.rockwell.com;
        --select count(9) into HOLDER1   from SAPECC_DLY_LIBRARIAN.EINA@DQD.mke.ra.rockwell.com;
        Aaa_Qdw_Script_Exec_Immediate ('TRUNCATE TABLE dwq$librarian.INV_SAP_PO_EINE');
        Aaa_Qdw_Script_Exec_Immediate ('insert into dwq$librarian.INV_SAP_PO_EINE  select * from INV_SAP_PO_EINE_V ');
        DBMS_OUTPUT.PUT_LINE(SUBSTR('EXEC KEY '|| REC_ID || ' BEGIN OF ' ||SCRT_GRP || ' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254) );
        UPDATE_COUNT := SQL%ROWCOUNT;
        COMMIT;
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
      EXCEPTION
      WHEN timeout_exception THEN
        --RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception timeout '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN table_Missing THEN
        -- RECORD_COUNT:=0;
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR('Exception Tbl Missing '||ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'C', ORACLE_ERROR , UPDATE_COUNT);
        COMMIT;
      WHEN OTHERS THEN
        ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,254);
        DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254));
        Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'W', ORACLE_ERROR , UPDATE_COUNT);
      END ;
    ELSE
      STATUS              := 'C';
      ORACLE_ERROR_GLOBAL := 'Several attemps in opening the Dblink Failed' || ORACLE_ERROR_GLOBAL;
    END IF; ---- check the DBLink is open for business :)
    IF STATUS = 'IP' THEN
      STATUS := 'P';
      AAA_QDW_MATERIALS_SCRIPT_EMAIL ( REC_ID,SCRT_GRP || ' STATUS SUCCSESSFUL COMPLETION ' ||STATUS ,'Successful completion '||CHR(13) ||'Stored Proc '||SCRT_GRP||'@QDWD ' || CHR(13)||ORACLE_ERROR);
    END IF;
    Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,SCRT_IND || ' ENDED' ,SYSDATE,STATUS,INSERT_COUNT + UPDATE_COUNT,ORACLE_ERROR_GLOBAL,FALSE,NULL,NULL,NULL,NULL);
    DBMS_OUTPUT.PUT_LINE(SUBSTR('END OF '||SCRT_GRP||' STRORED PROC ' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),1,254)) ;
    COMMIT;
  END IF; -- data already extracted
EXCEPTION
WHEN timeout_exception THEN
  RECORD_COUNT :=0;
  ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,500);
  Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,STEP_NAME,SYSDATE,'C',0,ORACLE_ERROR,FALSE,NULL,NULL,NULL,NULL);
  Aa_Qdw_Script_Step_Log (REC_ID,STEP_NAME ,STEP_BEGIN_DATE,'P',NULL, UPDATE_COUNT);
  DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254)) ;
  COMMIT;
  AAA_QDW_MATERIALS_SCRIPT_EMAIL ( REC_ID,SCRT_GRP || ' STATUS ' ||STATUS ,'Automatic Notification Error '||CHR(13) ||'Stored Proc '||SCRT_GRP||'@QDWD ' || CHR(13)||ORACLE_ERROR);
WHEN table_Missing THEN
  RECORD_COUNT :=0;
  ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,500);
  Aa_Qdw_Script_Log (REC_ID,SCRT_GRP,SCRT_IND,STEP_NAME,SYSDATE,'C',0,ORACLE_ERROR,FALSE,NULL,NULL,NULL,NULL);
  DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254)) ;
  COMMIT;
  AAA_QDW_MATERIALS_SCRIPT_EMAIL ( REC_ID,SCRT_GRP || ' STATUS ' ||STATUS ,'Automatic Notification Error '||CHR(13) ||'Stored Proc '||SCRT_GRP||'@QDWD ' || CHR(13)||ORACLE_ERROR);
WHEN OTHERS THEN
  ORACLE_ERROR := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,1000) || SUBSTR(DBMS_UTILITY.FORMAT_CALL_STACK,1,500);
  DBMS_OUTPUT.PUT_LINE(SUBSTR(ORACLE_ERROR,1,254)) ;
  AAA_QDW_MATERIALS_SCRIPT_EMAIL ( REC_ID,SCRT_GRP || ' STATUS ' ||STATUS ,'Automatic Notification Error '||CHR(13) ||'Stored Proc '||SCRT_GRP||'@QDWD ' || CHR(13)||ORACLE_ERROR);
END;