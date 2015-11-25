



select (num_rows * avg_row_len)/(1024*1024) as size_
from user_tables  
where table_name = 'INV_SAP_PP_OPT_X_STORE';



select count(*) from INV_SAP_PP_OPT_X_STORE



SELECT FILE_NAME,TABLESPACE_NAME,AUTOEXTENSIBLE FROM dba_data_files;


SELECT ddf.file_name FROM Dba_Data_Files ddf

where ddf.tablespace_name = 'SYSTEM'


ALTER DATABASE DATAFILE 'C:\APP\MHUANG1\ORADATA\APAFC\SYSTEM02.DBF' AUTOEXTEND ON;

alter tablespace SYSTEM add datafile 'C:\APP\MHUANG1\ORADATA\APAFC\SYSTEM03.DBF' size 20000M; 
alter tablespace SYSTEM add datafile 'C:\APP\MHUANG1\ORADATA\APAFC\SYSTEM04.DBF' size 20000M;
alter tablespace SYSTEM add datafile 'C:\APP\MHUANG1\ORADATA\APAFC\SYSTEM05.DBF' size 20000M;


CREATE TABLESPACE DATAMINING DATAFILE 'C:\APP\MHUANG1\ORADATA\APAFC\DATAMINING01.DBF' size 20000M; 

SELECT TABLESPACE_NAME, ROUND(SUM(BYTES/(1024*1024)),0) AS TS_SIZE FROM DBA_DATA_FILES GROUP BY TABLESPACE_NAME;
SELECT * FROM DBA_DATA_FILES

DROP TABLE tmp
create table tmp(rq varchar(10),shengfu varchar(10));
insert into tmp values('2005-05-09','win');
insert into tmp values('2005-05-09','lose');
insert into tmp values('2005-05-09','lose');
insert into tmp values('2005-05-09','lose');
insert into tmp values('2005-05-10','win');
insert into tmp values('2005-05-10','win');
insert into tmp values('2005-05-10','lose');

SELECT * FROM TMP;

SELECT TMP_WIN.RQ, TMP_WIN.WIN, TMP_LOSE.LOSE FROM 
(SELECT RQ, COUNT(SHENGFU) AS WIN FROM TMP WHERE SHENGFU = 'win' GROUP BY RQ)TMP_WIN
LEFT JOIN
(SELECT RQ, COUNT(SHENGFU) AS LOSE FROM TMP WHERE SHENGFU = 'lose' GROUP BY RQ)TMP_LOSE
ON TMP_WIN.RQ = TMP_LOSE.RQ

SELECT CASE TMP_WIN.SHENGFU WHEN 'win' THEN 



















DROP TABLE SDT_SCORE;
CREATE TABLE SDT_SCORE 
(
  SDT_NAME VARCHAR2(20), 
  COURSE VARCHAR2(20),
  SCORE VARCHAR2(20) 
);
COMMIT;
INSERT INTO SDT_SCORE VALUES('PETER', 'MATH', '90');
INSERT INTO SDT_SCORE VALUES('ALEX', 'ENGLISH', '85');
INSERT INTO SDT_SCORE VALUES('SIMON', 'ENGLISH', '88');
INSERT INTO SDT_SCORE VALUES('ALEX', 'MATH', '67');
INSERT INTO SDT_SCORE VALUES('SIMON', 'MATH', '87');
INSERT INTO SDT_SCORE VALUES('PETER', 'ENGLISH', '76');
INSERT INTO SDT_SCORE VALUES('MARLON', 'MATH', '75');
COMMIT;

SELECT SDT_NAME, SUM(CASE COURSE WHEN 'MATH' THEN SCORE ELSE '0' END) AS MATH,
      SUM(CASE COURSE WHEN 'ENGLISH' THEN SCORE ELSE '0' END) AS ENGLISH
FROM SDT_SCORE
GROUP BY SDT_NAME;

DROP TABLE SDT_SCORE_T;
CREATE TABLE SDT_SCORE_T AS
SELECT SDT_NAME,
  SUM(
  CASE COURSE
    WHEN 'MATH'
    THEN SCORE
    ELSE '0'
  END) AS MATH,
  SUM(
  CASE COURSE
    WHEN 'ENGLISH'
    THEN SCORE
    ELSE '0'
  END) AS ENGLISH
FROM SDT_SCORE
GROUP BY SDT_NAME;

SELECT * FROM SDT_SCORE_T;
SELECT * FROM SDT_SCORE;

(SELECT SDT_NAME, 'MATH' AS COURSE, MATH AS SCORE FROM SDT_SCORE_T) UNION ALL
(SELECT SDT_NAME, 'ENGLISH' AS COURSE, ENGLISH AS SCORE FROM SDT_SCORE_T)
ORDER BY COURSE DESC;




DROP TABLE YEAR_NUMBER;
CREATE TABLE YEAR_NUMBER 
(
  YER VARCHAR2(20), 
  MON VARCHAR2(20),
  AMOUNT VARCHAR2(20) 
);
COMMIT;
INSERT INTO YEAR_NUMBER VALUES('1991', '1', '90');
INSERT INTO YEAR_NUMBER VALUES('1991', '2', '85');
INSERT INTO YEAR_NUMBER VALUES('1991', '3', '88');
INSERT INTO YEAR_NUMBER VALUES('1991', '4', '67');
INSERT INTO YEAR_NUMBER VALUES('1992', '1', '87');
INSERT INTO YEAR_NUMBER VALUES('1992', '2', '76');
INSERT INTO YEAR_NUMBER VALUES('1992', '3', '75');
INSERT INTO YEAR_NUMBER VALUES('1992', '4', '75');
COMMIT;

SELECT * FROM YEAR_NUMBER;

SELECT YER, SUM(CASE MON WHEN '1' THEN AMOUNT ELSE '0' END) AS M1,
SUM(CASE MON WHEN '2' THEN AMOUNT ELSE '0' END) AS M2,
SUM(CASE MON WHEN '3' THEN AMOUNT ELSE '0' END) AS M3,
SUM(CASE MON WHEN '4' THEN AMOUNT ELSE '0' END) AS M4
FROM YEAR_NUMBER
GROUP BY YER;
