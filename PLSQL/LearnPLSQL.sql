--Session 3 
--The simplest PL&SQL script.
--It is a anonymous block without the declaration and exception.
BEGIN
  SYS.DBMS_OUTPUT.PUT_LINE('Hello PLSQL!');
END;

-- Add paramater in declare
DECLARE
  time_now VARCHAR2(20 CHAR);
BEGIN
  time_now := SYSDATE;
  SYS.DBMS_OUTPUT.PUT_LINE(time_now);
END;

--Add the exception
DECLARE
 time_now VARCHAR2(30);
BEGIN
  time_now := SYSDATE;
  SYS.DBMS_OUTPUT.PUT_LINE(time_now);
EXCEPTION
  WHEN VALUE_ERROR
  THEN
    SYS.DBMS_OUTPUT.PUT_LINE('It seems time_now is too small for default date format!');
END;


--Use Boolen value
DECLARE
  Is_enough BOOLEAN;
  Not_enough BOOLEAN;
BEGIN
  Is_enough := TRUE;
  Not_enough := FALSE;
  
  IF Is_enough
  THEN
  SYS.DBMS_OUTPUT.PUT_LINE('Is enough..');
  END IF;
END;


--Using the Case when, function 
DECLARE
   boolean_true BOOLEAN := TRUE;
   boolean_false BOOLEAN := FALSE;
   boolean_null BOOLEAN;
   FUNCTION boolean_to_varchar2 (flag IN BOOLEAN) RETURN VARCHAR2 IS
   BEGIN
      RETURN
         CASE flag
            WHEN TRUE THEN 'True'
            WHEN FALSE THEN 'False'
            ELSE 'NULL'
         END;
   END;
BEGIN
   DBMS_OUTPUT.PUT_LINE(boolean_to_varchar2(boolean_true));
   DBMS_OUTPUT.PUT_LINE(boolean_to_varchar2(boolean_false));
   DBMS_OUTPUT.PUT_LINE(boolean_to_varchar2(boolean_null));
END;

--Display Function or Procedures
--PARA IN DATATYPE use in (dis in VARCHAR2)~!!!
--PARAmeter
--Mode Description Parameter usage
--IN Read-only The value of the actual parameter can be referenced inside the module, but the parameter cannot
--be changed. If you do not specify the parameter mode, then it is considered an IN parameter.
--OUT Write-only The module can assign a value to the parameter, but the parameter’s value cannot be referenced.
--IN OUT Read/write The module can both reference (read) and modify (write) the parameter--

--Add Parameter in procedure
create or replace procedure insert_name_date(name_in IN VARCHAR2)
IS
  date_in VARCHAR2(20 CHAR);
BEGIN
  date_in := SYSDATE;
  INSERT INTO EMPLOYEE_NAME_BIRTHDAY
  (NAME, BIRTHDAY)
  VALUES (name_in, date_in);
END;

CREATE OR REPLACE PROCEDURE EXE_SQL(SQL_CODE IN VARCHAR2) 
IS
  EXE_SQL VARCHAR2(2000);
BEGIN
  EXE_SQL := SQL_CODE;
  EXECUTE IMMEDIATE EXE_SQL;
END;

BEGIN
EXE_SQL('SELECT * FROM INV_SAP_PP_OPT_X');
END;

CREATE OR REPLACE PROCEDURE display_total_sales(dis in VARCHAR2)
is
begin
  DBMS_OUTPUT.put_line(dis);
end;

-- How to Call proceduce
-- So we always assign value in declare state ment
DECLARE
  start_year_in PLS_INTEGER := 10;  -- assign value
  end_year_in  PLS_INTEGER := 20;
BEGIN

  display_multiple_years(start_year_in,end_year_in);
END;

------------------------------------------loop----------------------------------------
--File on web: loop_examples.sql
--while..loop.. end loop.
CREATE OR REPLACE PROCEDURE display_multiple_years(
    start_year_in IN PLS_INTEGER ,
    end_year_in   IN PLS_INTEGER )
IS
  l_current_year PLS_INTEGER := start_year_in;
BEGIN
  WHILE (l_current_year <= end_year_in)
  LOOP
    display_total_sales (l_current_year);
    l_current_year := l_current_year + 1;
  END LOOP;
END;

--Looping continue
--For i IN range loop .. end loop
BEGIN
  FOR I IN 1..10
  LOOP
   CONTINUE WHEN MOD(I,2) = 0;
   DBMS_OUTPUT.put_line(I);
  END LOOP;
END;

--Looping nest
DECLARE
  OUT_NUM_BEGIN NUMBER := 1;
  OUT_NUM_END NUMBER :=10;
  IN_NUMBER_BEGIN NUMBER :=1;
  IN_NUMEBR_END NUMBER :=10;
BEGIN
  FOR I IN OUT_NUM_BEGIN .. OUT_NUM_END
  LOOP
    FOR J IN IN_NUMBER_BEGIN .. IN_NUMEBR_END
    LOOP
      DBMS_OUTPUT.put_line(I||'_'||J);
    END LOOP;
  END LOOP;
END;

/* File on web: simple_integer_demo.sql */
-- First create a compute intensive procedure using PLS_INTEGER
-- Use For .... Loop....
CREATE OR REPLACE PROCEDURE pls_test(
    iterations IN PLS_INTEGER)
AS
  int1 PLS_INTEGER := 1;
  int2 PLS_INTEGER := 2;
  --USE TO CACL TIME USE...
  begints TIMESTAMP;
  endts   TIMESTAMP;
BEGIN
  begints := SYSTIMESTAMP;
  FOR cnt IN 1 .. iterations
  LOOP
    int1 := int1 + int2 * cnt;
    SYS.DBMS_OUTPUT.PUT_LINE(int1);
  END LOOP;
  endts := SYSTIMESTAMP;
  SYS.DBMS_OUTPUT.PUT_LINE( iterations || ' iterations had run time of:' || TO_CHAR (endts - begints));
END;

BEGIN
  pls_test(200);
END;





----------------------------------------String Usage-----------------------------------

DECLARE
  names          VARCHAR2(60) := 'Anna,Matt,Joe,Nathan,Andrew,Aaron,Jeff';
  comma_location NUMBER       := 0;
BEGIN
  LOOP
    comma_location := INSTR(names,',',comma_location+1);  ---comma_location + 1 means jump to next char
    EXIT
  WHEN comma_location = 0;
    SYS.DBMS_OUTPUT.PUT_LINE(comma_location);
  END LOOP;
END;

--Another case for string
DECLARE
  names          VARCHAR2(60) := 'Anna,Matt,Joe,Nathan,Andrew,Aaron,Jeff';
  names_adjusted VARCHAR2(61);
  comma_location NUMBER := 0;
  prev_location  NUMBER := 0;
BEGIN
  --Stick a comma after the final name
  names_adjusted := names || ',';
  LOOP
    comma_location := INSTR(names_adjusted,',',comma_location+1);
    EXIT
  WHEN comma_location = 0;
    SYS.DBMS_OUTPUT.PUT_LINE( SUBSTR(names_adjusted,prev_location+1, comma_location-prev_location-1));
    prev_location := comma_location;
  END LOOP;
END;

--String replacement
DECLARE
  names VARCHAR2(60) := 'Anna,Matt,Joe,Nathan,Andrew,Aaron,Jeff';
BEGIN
  SYS.DBMS_OUTPUT.PUT_LINE( REPLACE(names, ',', chr(10)) );
END;


--String pedding
DECLARE
  a VARCHAR2(30) := 'Jeff';
  b VARCHAR2(30) := 'Eric';
  c VARCHAR2(30) := 'Andrew';
  d VARCHAR2(30) := 'Aaron';
  e VARCHAR2(30) := 'Matt';
  f VARCHAR2(30) := 'Joe';
BEGIN
  SYS.DBMS_OUTPUT.PUT_LINE( RPAD(a,10) || LPAD(b,10));
  SYS.DBMS_OUTPUT.PUT_LINE( RPAD(a,10,'.') || LPAD(b,10,'.') );
END;

---String Trim
DECLARE
  a VARCHAR2(40) := 'This sentence has too many periods......';
  b VARCHAR2(40) := 'The nww22umber 1';
BEGIN
  DBMS_OUTPUT.PUT_LINE( RTRIM(a,'.') );
  DBMS_OUTPUT.PUT_LINE( LTRIM(b, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz') );
END;








Select to_char(sysdate,'MM') from dual;















---------------------------------------Cursor Usage------------------------------------
--Name Description
--%FOUND Returns TRUE if the record was fetched successfully, FALSE otherwise
--%NOTFOUND Returns TRUE if the record was not fetched successfully, FALSE otherwise
--%ROWCOUNT Returns the number of records fetched from cursor at that point in time
--%ISOPEN Returns TRUE if cursor is open, FALSE otherwise
--%BULK_ROWCOUNT Returns the number of records modified by the FORALL statement for each collection element
--%BULK_EXCEPTIONS Returns exception information for rows modified by the FORALL statement for each collection element

DECLARE
    v_name EMPLOYEE_NAME_BIRTHDAY.NAME%TYPE;
    CURSOR c_cursor IS SELECT NAME FROM EMPLOYEE_NAME_BIRTHDAY;
    v_date VARCHAR2(20 CHAR);
  BEGIN
    v_date := SYSDATE;
    OPEN c_cursor;
    FETCH c_cursor INTO v_name;
    SYS.DBMS_OUTPUT.PUT_LINE(v_name);
    UPDATE EMPLOYEE_NAME_BIRTHDAY SET BIRTHDAY = v_date WHERE NAME = v_name;
    CLOSE c_cursor;
END;

---Use string replacement internal function

DECLARE
    v_name EMPLOYEE_NAME_BIRTHDAY.NAME%TYPE;
    CURSOR c_cursor IS SELECT NAME FROM EMPLOYEE_NAME_BIRTHDAY;
    v_name_no_dash VARCHAR2(20 CHAR);
  BEGIN
    OPEN c_cursor;
    LOOP
    FETCH c_cursor INTO v_name;
    EXIT WHEN c_cursor%NOTFOUND; 
    v_name_no_dash := REPLACE(v_name, '-');
    SYS.DBMS_OUTPUT.PUT_LINE(v_name);
    UPDATE EMPLOYEE_NAME_BIRTHDAY SET BIRTHDAY = v_name_no_dash WHERE NAME = v_name;
    SYS.DBMS_OUTPUT.PUT_LINE(v_name_no_dash);
    END LOOP;
    CLOSE c_cursor;
END;


--Remove dash  This method is tooooooo slow..... 
DECLARE
    v_material_catalog_dash INV_SAP_MATERIAL_CATALOG.CATALOG_STRING1%TYPE;
    CURSOR c_cursor IS SELECT CATALOG_STRING1 FROM INV_SAP_MATERIAL_CATALOG;
    v_name_no_dash VARCHAR2(50 CHAR);
  BEGIN
    
    OPEN c_cursor;
    LOOP
    FETCH c_cursor INTO v_material_catalog_dash;
    --EXIT WHEN c_cursor%NOTFOUND; 
    v_name_no_dash := REPLACE(v_material_catalog_dash, '-');
    UPDATE INV_SAP_MATERIAL_CATALOG SET CATALOG_STRING2 = v_name_no_dash WHERE CATALOG_STRING2 = v_material_catalog_dash;
    END LOOP;
    CLOSE c_cursor;
END;

--Count table row number
select count(*) from INV_SAP_MATERIAL_CATALOG;
select count(*) from INV_SAP_PP_OPTIMIZATION;

select a.ROWID from INV_SAP_MATERIAL_CATALOG a;

--Use rowid to update, Great! 37seconds!!! It use rowid..

DROP TABLE CATALOG_TMP;
CREATE TABLE CATALOG_NODASH_MATERIAL AS SELECT * FROM INV_SAP_MATERIAL_CATALOG;
SELECT MATERIALID FROM CATALOG_NODASH_MATERIAL;

DECLARE
  CURSOR cur
  IS
    SELECT a.CATALOG_STRING1,
      b.ROWID ROW_ID
    FROM INV_SAP_MATERIAL_CATALOG a,
      CATALOG_NODASH_MATERIAL b
    WHERE a.MATERIALID = b.MATERIALID
    ORDER BY b.ROWID; ---order by rowid
  V_COUNTER NUMBER;
BEGIN
  V_COUNTER := 0;
  FOR row IN cur
  LOOP
    UPDATE CATALOG_NODASH_MATERIAL SET CATALOG_STRING2 = REPLACE(row.CATALOG_STRING1, '-') WHERE ROWID = row.ROW_ID;
    V_COUNTER     := V_COUNTER + 1;
    IF (V_COUNTER >= 1000) THEN
      COMMIT;
      V_COUNTER := 0;
    END IF;
  END LOOP;
  COMMIT;
END;


---Add Current Serial
ALTER TABLE CATALOG_TMP 
ADD (CURRENT_SERIES VARCHAR2(20) );

DECLARE
  CURSOR cur
  IS
    SELECT a.CURRENT_SERIES,
      b.ROWID ROW_ID
    FROM INV_SAP_PP_MVKE a,
      CATALOG_TMP b
    WHERE a.MATERIALID = b.MATERIALID
    ORDER BY b.ROWID; ---order by rowid
  V_COUNTER NUMBER;
BEGIN
  V_COUNTER := 0; --- Use to cumulate results and commit to data base.
  FOR row IN cur
  LOOP
    UPDATE CATALOG_TMP SET CURRENT_SERIES = row.CURRENT_SERIES WHERE ROWID = row.ROW_ID;
    ---1000 commit!
    V_COUNTER     := V_COUNTER + 1;
    IF (V_COUNTER >= 1000) THEN
      COMMIT;
      V_COUNTER := 0;
    END IF;
    
  END LOOP;
  COMMIT;
END;

select * from INV_SAP_PP_MVKE where MATERIALID = '42EF-C2KBA-F4 A';

--maybe, it will quicker than the pl sql 
SELECT b.CATALOG_STRING1,
  b.CATALOG_STRING2,
  b.MATERIALID,
  a.DIRECT_SHIP_PLANT,
  a.CURRENT_SERIES,
  b.ROWID ROW_ID
FROM INV_SAP_PP_MVKE a,
  CATALOG_TMP b
WHERE a.MATERIALID = b.MATERIALID;

--Join with vendor info
SELECT a.MATERIALID,a.PLANTID,a.STRATEGY_GRP,b.CURRENT_SERIES
FROM INV_SAP_VENDOR_PLANT_INFO a,
  VIEW_CTMP_MVK b
WHERE a.MATERIALID = b.MATERIALID AND a.PLANTID = b.DIRECT_SHIP_PLANT;

SELECT * FROM INV_SAP_VENDOR_PLANT_INFO where MATERIALID = 'PN-82272';

select * from INV_SAP_VENDOR_PLANT_INFO where MATERIALID = 'PN-82272' and plantid = '4000';











--------------------------------------------Numeric Datatypes------------------------------
--NUMBER PLS_INTEGER BINARY_INTEGER  SIMPLE_INTEGER BINARY_FLOAT BINARY_DOUBLE
--SIMPLE_FLOAT SIMPLE_DOUBLE

DECLARE
  tiny_nbr NUMBER := 1e-130;
  test_nbr NUMBER;
  -- 1111111111222222222233333333334
  -- 1234567890123456789012345678901234567890
  big_nbr NUMBER := 9.999999999999999999999999999999999999999e125;
  -- 1111111111222222222233333333334444444
  -- 1234567890123456789012345678901234567890123456
  fmt_nbr VARCHAR2(50) := '9.99999999999999999999999999999999999999999EEEE';
BEGIN
  DBMS_OUTPUT.PUT_LINE('tiny_nbr =' || TO_CHAR(tiny_nbr, '9.9999EEEE'));
  -- NUMBERs that are too small round down to zero
  test_nbr := tiny_nbr / 1.0001;
  DBMS_OUTPUT.PUT_LINE('tiny made smaller =' || TO_CHAR(test_nbr, fmt_nbr));
  -- NUMBERs that are too large throw an error
  DBMS_OUTPUT.PUT_LINE('big_nbr =' || TO_CHAR(big_nbr, fmt_nbr));
  test_nbr := big_nbr * 1.0001; -- too big
  DBMS_OUTPUT.PUT_LINE('big made bigger =' || TO_CHAR(test_nbr, fmt_nbr));
END;


DECLARE
  test_number1 NUMBER(9,2) := 2222.222;
  test_number2 NUMBER(9,-11) := 9999999999999999999;
BEGIN
  DBMS_OUTPUT.PUT_LINE('big made bigger =' || test_number2);
END;

DECLARE
low_nbr NUMBER(38,127);
high_nbr NUMBER(38,-84);
BEGIN
/* 127 is largest scale, so begin with 1 and move
decimal point 127 places to the left. Easy. */
low_nbr := 1E-127;
DBMS_OUTPUT.PUT_LINE('low_nbr = ' || low_nbr);
/* .84 is smallest scale value. Add 37 to normalize
the scientific-notation, and we get E+121. */
high_nbr := 9.9999999999999999999999999999999999999E+121;
DBMS_OUTPUT.PUT_LINE('high_nbr = ' || high_nbr);
END;
  
  
DECLARE
int1 PLS_INTEGER;
int2 PLS_INTEGER;
int3 PLS_INTEGER;
nbr NUMBER;
BEGIN
int1 := 100;
int2 := 49;
int3 := int2/int1;
nbr := int2/int1;
DBMS_OUTPUT.PUT_LINE('integer 100/49 =' || TO_CHAR(int3));
DBMS_OUTPUT.PUT_LINE('number 100/49 =' || TO_CHAR(nbr));
int2 := 50;
int3 := int2/int1;
nbr := int2/int1;
DBMS_OUTPUT.PUT_LINE('integer 100/50 =' || TO_CHAR(int3));
DBMS_OUTPUT.PUT_LINE('number 100/50 =' || TO_CHAR(nbr));
END;



























----------------------------------FUNCTION---------------------------------
--FUNCTION favorite_nickname (
--name_in IN VARCHAR2) RETURN VARCHAR2
--IS
--BEGIN
--...
--RETURN
--END;


CREATE OR REPLACE FUNCTION REFRESH_EXPSA RETURN NUMBER
AS
BEGIN
    UPDATE EMPLOYEE SET SALARY = SALARY*(1.05) WHERE SALARY < 20;
    IF SQL%ROWCOUNT = 0 THEN
      RETURN 0;
    ELSE
      RETURN 1;
    END IF;
END;

DECLARE
  RESULT NUMBER;
BEGIN
  RESULT := REFRESH_EXPSA();
  SYS.DBMS_OUTPUT.PUT_LINE(RESULT);
END;



DROP PROCEDURE ISEXIST;

create or replace PROCEDURE ISEXIST
IS
V_COUNT NUMBER;
BEGIN
  SELECT COUNT(*) INTO V_COUNT FROM EMPLOYEE;  
  IF V_COUNT = 0 THEN
    RETURN;
  ELSE
    EXECUTE IMMEDIATE 'DROP TABLE EMPLOYEE';
    SYS.DBMS_OUTPUT.PUT_LINE('DROP TABLE ALREADY.');    
  END IF;
END;


BEGIN
   ISEXIST();
END;




--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
CREATE
	OR REPLACE FUNCTION APAFC_SCRIPT_EXE_IMMEDIATE (SCRIPT IN VARCHAR2)

RETURN IS SCRIPT_TMP VARCHAR2;

BEGIN
	SCRIPT_TMP : = SCRIPT;

	EXECUTE IMMEDIATE SCRIPT;
END;











------------------------------------------------------------------------------------------------------------------------
--sequence

DROP TABLE EMPLOYEE;
CREATE TABLE EMPLOYEE(
ID NUMBER(10) NOT NULL,
SALARY NUMBER(8,2) DEFAULT 0 NOT NULL,
NAME VARCHAR(20) NOT NULL
);

--SEQUENCE STRUCTURE------------
CREATE SEQUENCE CUX_DEMO_SEQUENCE --SEQ NAME
MINVALUE 1  --MIN VALUE
MAXVALUE 99999999999 --MAX VALUE
START WITH 10000 -- START VALUE
INCREMENT BY 1 --STEP LEN
NOCYCLE --NO CYCLE
CACHE 20
ORDER;
---------------------------------
CREATE SEQUENCE EMP_SEQ
  INCREMENT BY 1
  NOMAXVALUE
  NOCYCLE;
  
DROP SEQUENCE EMP_SEQ;


--request
-- write a pl/sql block, insert rmployee table 100 line data. Use EMP_SEQ to calculate the id value. The salary and name
--is depended yours.

SELECT * FROM EMPLOYEE;
TRUNCATE TABLE EMPLOYEE;

DECLARE
  IN_NAME VARCHAR2(30);
  IN_SALARY NUMBER(8,2);
  I NUMBER(10);
BEGIN
  IN_NAME := 'TEST_';
  IN_SALARY := 20;
  
  FOR I IN 1..100
  LOOP
    INSERT INTO EMPLOYEE VALUES(EMP_SEQ.NEXTVAL,IN_SALARY,IN_NAME);
    COMMIT;
    IN_SALARY := IN_SALARY + 1;
  END LOOP;
END;


---define the explicit cursor, sorted by id, print top 10 data.
DECLARE
  CURSOR CUR_EXP
  IS
    SELECT ID, NAME,SALARY FROM EMPLOYEE ORDER BY ID DESC;
  CUR_ROW CUR_EXP%ROWTYPE;
  I NUMBER := 1;
BEGIN
  FOR CUR_ROW IN CUR_EXP 
  LOOP
    SYS.DBMS_OUTPUT.PUT_LINE(CUR_ROW.ID||'-'||CUR_ROW.NAME||'-'||CUR_ROW.SALARY);
    I := I + 1;
    --EXIT WHEN I > 10;
    --EXIT WHEN CUR_ROW%NOTFOUND;
    IF I > 10 THEN 
    EXIT; 
    END IF;
  END LOOP;
END;

--CREATE PL/SQL, INPUT THE EMPLOYEE SALARY scope
CREATE OR REPLACE PROCEDURE P_EXP(VSALARY NUMBER, VSALARY2 NUMBER)
AS
  CURSOR CUR_EXP
  IS
  SELECT ID, NAME,SALARY FROM EMPLOYEE WHERE SALARY BETWEEN &VSALARY AND VSALARY2 ORDER BY SALARY DESC; 
  CUR_ROW CUR_EXP%ROWTYPE;
BEGIN
  FOR CUR_ROW IN CUR_EXP
  LOOP
    SYS.DBMS_OUTPUT.PUT_LINE(CUR_ROW.ID||'-'||CUR_ROW.NAME||'-'||CUR_ROW.SALARY);
  END LOOP;
END;










