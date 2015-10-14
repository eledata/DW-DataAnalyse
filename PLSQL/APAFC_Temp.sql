-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Description:
--
--  This file just use for test new functions, procedures, package..
--
-- Revision History:
--
--    Date        Developer         Description
--    ---------   ---------------   -------------------------------------------
--    2014-12-24  Moyue           	Initial testing area.
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Create testing table
DROP TABLE EMPLOYEE;
CREATE TABLE EMPLOYEE
  (
    ID     NUMBER(10) NOT NULL ,
    SALARY NUMBER(8, 2) DEFAULT 0 NOT NULL ,
    NAME   VARCHAR(20) NOT NULL
  );

DECLARE
   T_NAME VARCHAR(200);
   T_SQL VARCHAR(200);
   T_SQL_C VARCHAR(200);
BEGIN
  T_NAME := 'CREATE TABLE TEST_TABLE AS SELECT * FROM EMPLOYEE';
  T_SQL := 'INSERT INTO EMPLOYEE VALUES(1,22,T_NA)';
  SYS.DBMS_OUTPUT.PUT_LINE(T_NAME);
  
  APAFC_SCRIPT_EXE_IMMEDIATE(T_NAME);
  
END;


DECLARE
  RECORD_COUNT NUMBER;
BEGIN
  RECORD_COUNT := 0;
  SELECT COUNT(*) INTO RECORD_COUNT FROM INV_SAP_PP_OPTIMIZATION;
  SYS.DBMS_OUTPUT.PUT_LINE(RECORD_COUNT);
END;


INSERT INTO EMPLOYEE VALUES(1,22,TEST)


analyze table INV_SAP_PP_OPTIMIZATION estimate  statistics sample 40 percent

se
    SELECT COUNT(*)
    FROM INV_SAP_PP_OPTIMIZATION