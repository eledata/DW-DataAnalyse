

SELECT *
  FROM (SELECT ROWNUM AS rowno, t.*
          FROM emp t
         WHERE hire_date BETWEEN TO_DATE ('20060501', 'yyyymmdd')
                             AND TO_DATE ('20060731', 'yyyymmdd')
           AND ROWNUM <= 20) table_alias
 WHERE table_alias.rowno >= 10;


SELECT ROWNUM AS RN,MATERIALID  FROM INV_SAP_PP_FRCST_PBIM_PBED WHERE ROWNUM <= 20;

SELECT * FROM
(
SELECT *
FROM
  (SELECT ROWNUM AS RN,
    MATERIALID
    ||'_'
    ||PLANTID                           AS ID,
    MATERIALID                          AS MATERIALID,
    PLANTID                             AS PLANTID,
    CEIL(SUM(PLNMG_PLANNEDQUANTITY)/13) AS FC_AVG13_WEEK_QTY
  FROM INV_SAP_PP_FRCST_PBIM_PBED
  WHERE (PDATU_DELIV_ORDFINISHDATE BETWEEN TO_CHAR(sysdate) AND TO_CHAR(sysdate + 91))
  AND VERSBP_VERSION = '00'
  AND PLANTID        = '5040'
  GROUP BY MATERIALID,
    ROWNUM,
    PLANTID
  )
WHERE RN >= 20
ORDER BY RN ASC
) WHERE RN <= 40;


SELECT a.SID, spid, status, SUBSTR (a.program, 1, 40) prog, a.terminal,a.SQL_TEXT, osuser, VALUE / 60 / 100 VALUE

FROM v$session a, v$process b, v$sesstat c

WHERE c.statistic# = 12 AND c.SID = a.SID AND a.paddr = b.addr

ORDER BY VALUE DESC;


