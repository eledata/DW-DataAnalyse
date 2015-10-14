--Porject Name: User Usage and Login information
--Author: Huang Moyue 
--Mail: mhuang1@ra.rockwell.com
--Date:12/10/2014
--Summary: User usage information and the login information

--1. User table in the Oracle
CREATE TABLE INV_SAP_APTOOL_USERLOG
  (
    "USERID"             VARCHAR2(100 BYTE),
    "COMPUTERID"         VARCHAR2(100 BYTE),
    "APPLICATION"        VARCHAR2(100 BYTE),
    "AUTHORIZATION_TYPE" VARCHAR2(100 BYTE),
    "LOG_DATE"           DATE DEFAULT sysdate
  )
CREATE TABLE INV_SAP_APTOOL_USERINFO
  (
    "USERID"     VARCHAR2(100 BYTE),
    "COMPUTERID" VARCHAR2(100 BYTE)
  )
  
  
--2. Autho Check

SELECT * FROM INV_SAP_APTOOL_USERINFO WHERE USERID = 'USERID_1' AND COMPUTERID = 'COMPUTERID_1';
  
  