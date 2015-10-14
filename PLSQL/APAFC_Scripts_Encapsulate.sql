-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-- Description:
--
--  This file just use for test new functions, procedures, package..
--
-- Revision History:
--
--    Date        Developer         Description
--    ---------   ---------------   ----------------------------------------------------
--    2014-12-24  Moyue           	Scripts Collections.
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------

--Execute Immediate function encapsulate.
CREATE OR REPLACE PROCEDURE APAFC_SCRIPT_EXE_IMMEDIATE(
    SQL_EXE IN VARCHAR2)
IS
  PROCESS_TERMINATED EXCEPTION;
BEGIN
  EXECUTE IMMEDIATE SQL_EXE;
EXCEPTION
WHEN OTHERS THEN
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Execute Script:'||SUBSTR(SQL_EXE,1,100));
  DBMS_OUTPUT.PUT_LINE('Execute Error:');
  DBMS_OUTPUT.PUT_LINE(SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,200)||SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200));
  RAISE PROCESS_TERMINATED;
END;


--CREATE SEQUENCE
CREATE SEQUENCE APAFC_LOGGING_SEQUENCE --SEQ NAME
MINVALUE 1  --MIN VALUE
MAXVALUE 99999999999 --MAX VALUE
START WITH 10000 -- START VALUE
INCREMENT BY 1 --STEP LEN
NOCYCLE --NO CYCLE
CACHE 20
ORDER;

--DBMS_OUTPUT PUT_LINE encapsulate
CREATE OR REPLACE PROCEDURE APAFC_MSG_PRINT(
	MSG IN VARCHAR2)
IS	
	PROCESS_TERMINATED EXCEPTION;
BEGIN
	DBMS_OUTPUT.PUT_LINE(MSG);
EXCEPTION
WHEN OTHERS THEN
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Execute Script:'||SUBSTR(SQL_EXE,1,100));
  DBMS_OUTPUT.PUT_LINE('Execute Error:');
  DBMS_OUTPUT.PUT_LINE(SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK,1,200)||SUBSTR(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,200));
  RAISE PROCESS_TERMINATED;
END;








