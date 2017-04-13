-- Date : 2017/2/16
-- Author :  Huang Moyue
------------------------------------------------------------------------------------------------------------------------
-- 1. SCI ITEM MASTER INIT 数据自动生成的，设置比较的全面

DROP VIEW EBS_ECL_CHN_BASIC_EXLP_MM_VIEW;
CREATE VIEW EBS_ECL_CHN_BASIC_EXLP_MM_VIEW AS 
SELECT MM_SPEND.ID,
  MM_SPEND.LEGACY,
  MM_SPEND.EBS_CODE,
  MM_SPEND.PLANT,
  MM_SPEND.STD_POST_DATE,
  MM_SPEND.MM_TOT_SPEND,
  MM_KGS.MM_UNIT_KGS
FROM
  (SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_TOT_SPEND
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(TOT_SPEND) AS MM_TOT_SPEND
    FROM EBS_ECL_CHN_BASIC_EXLP_VIEW WHERE TOT_SPEND <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
  )MM_SPEND
LEFT JOIN
  (SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_UNIT_KGS
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      PLANT,
      STD_POST_DATE,
      SUM(UNIT_KGS) AS MM_UNIT_KGS
    FROM EBS_ECL_CHN_BASIC_EXLP_VIEW WHERE UNIT_KGS <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
  )MM_KGS
ON MM_KGS.ID = MM_SPEND.ID;

DROP VIEW EBS_ECL_CHN_BASIC_LP_MM_VIEW;
CREATE VIEW EBS_ECL_CHN_BASIC_LP_MM_VIEW AS 
SELECT MM_SPEND.ID,
  MM_SPEND.LEGACY,
  MM_SPEND.EBS_CODE,
  MM_SPEND.PLANT,
  MM_SPEND.STD_POST_DATE,
  MM_SPEND.MM_TOT_SPEND,
  MM_KGS.MM_UNIT_KGS
FROM
  (SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_TOT_SPEND
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(TOT_SPEND) AS MM_TOT_SPEND
    FROM EBS_ECL_CHN_BASIC_LP_VIEW WHERE TOT_SPEND <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
  )MM_SPEND
LEFT JOIN
  (SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_UNIT_KGS
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      PLANT,
      STD_POST_DATE,
      SUM(UNIT_KGS) AS MM_UNIT_KGS
    FROM EBS_ECL_CHN_BASIC_LP_VIEW WHERE UNIT_KGS <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
  )MM_KGS
ON MM_KGS.ID = MM_SPEND.ID;

DROP VIEW EBS_NALCO_CHN_RES_MM_VIEW;
CREATE VIEW EBS_NALCO_CHN_RES_MM_VIEW AS 
SELECT MM_SPEND.ID,
  MM_SPEND.LEGACY,
  MM_SPEND.EBS_CODE,
  MM_SPEND.PLANT,
  MM_SPEND.STD_POST_DATE,
  MM_SPEND.MM_TOT_SPEND,
  MM_KGS.MM_UNIT_KGS
FROM
  (SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_TOT_SPEND
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(TOT_SPEND) AS MM_TOT_SPEND
    FROM EBS_NALCO_CHN_RES_VIEW WHERE TOT_SPEND <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
  )MM_SPEND
LEFT JOIN
  (SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_UNIT_KGS
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      PLANT,
      STD_POST_DATE,
      SUM(UNIT_KGS) AS MM_UNIT_KGS
    FROM EBS_NALCO_CHN_RES_VIEW WHERE UNIT_KGS <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
  )MM_KGS
ON MM_KGS.ID = MM_SPEND.ID;

-- TRANSFER TO BY MM
DROP VIEW EBS_ECL_CBEXLP_KSGMM_VIEW;
CREATE VIEW EBS_ECL_CBEXLP_KGSMM_VIEW AS 
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
SUM(MM_1) AS KG_MM_1,
SUM(MM_2) AS KG_MM_2,
SUM(MM_3) AS KG_MM_3,
SUM(MM_4) AS KG_MM_4,
SUM(MM_5) AS KG_MM_5,
SUM(MM_6) AS KG_MM_6,
SUM(MM_7) AS KG_MM_7,
SUM(MM_8) AS KG_MM_8,
SUM(MM_9) AS KG_MM_9,
SUM(MM_10) AS KG_MM_10,
SUM(MM_11) AS KG_MM_11,
SUM(MM_12) AS KG_MM_12,
SUM(MM_13) AS KG_MM_13,
SUM(MM_14) AS KG_MM_14,
SUM(MM_15) AS KG_MM_15,
SUM(MM_16) AS KG_MM_16,
SUM(MM_17) AS KG_MM_17,
SUM(MM_18) AS KG_MM_18,
SUM(MM_19) AS KG_MM_19,
SUM(MM_20) AS KG_MM_20,
SUM(MM_21) AS KG_MM_21,
SUM(MM_22) AS KG_MM_22,
SUM(MM_23) AS KG_MM_23,
SUM(MM_24) AS KG_MM_24
FROM
(
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_1,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_2,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_3,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_4,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_5,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_6,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_7,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_8,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_9,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_10,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_11,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_12,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_13,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_14,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_15,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_16,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_17,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_18,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_19,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_20,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_21,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_22,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_23,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_24
FROM 
(
SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_UNIT_KGS
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(UNIT_KGS) AS MM_UNIT_KGS
    FROM EBS_ECL_CHN_BASIC_EXLP_VIEW WHERE UNIT_KGS <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
)
) GROUP BY
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE;

DROP VIEW EBS_ECL_CBEXLP_SPDMM_VIEW;
CREATE VIEW EBS_ECL_CBEXLP_SPDMM_VIEW AS 
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
SUM(MM_1) AS SPEND_MM_1,
SUM(MM_2) AS SPEND_MM_2,
SUM(MM_3) AS SPEND_MM_3,
SUM(MM_4) AS SPEND_MM_4,
SUM(MM_5) AS SPEND_MM_5,
SUM(MM_6) AS SPEND_MM_6,
SUM(MM_7) AS SPEND_MM_7,
SUM(MM_8) AS SPEND_MM_8,
SUM(MM_9) AS SPEND_MM_9,
SUM(MM_10) AS SPEND_MM_10,
SUM(MM_11) AS SPEND_MM_11,
SUM(MM_12) AS SPEND_MM_12,
SUM(MM_13) AS SPEND_MM_13,
SUM(MM_14) AS SPEND_MM_14,
SUM(MM_15) AS SPEND_MM_15,
SUM(MM_16) AS SPEND_MM_16,
SUM(MM_17) AS SPEND_MM_17,
SUM(MM_18) AS SPEND_MM_18,
SUM(MM_19) AS SPEND_MM_19,
SUM(MM_20) AS SPEND_MM_20,
SUM(MM_21) AS SPEND_MM_21,
SUM(MM_22) AS SPEND_MM_22,
SUM(MM_23) AS SPEND_MM_23,
SUM(MM_24) AS SPEND_MM_24
FROM
(
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_1,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_2,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_3,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_4,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_5,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_6,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_7,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_8,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_9,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_10,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_11,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_12,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_13,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_14,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_15,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_16,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_17,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_18,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_19,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_20,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_21,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_22,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_23,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_24
FROM 
(
SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_TOT_SPEND
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(TOT_SPEND) AS MM_TOT_SPEND
    FROM EBS_ECL_CHN_BASIC_EXLP_VIEW WHERE TOT_SPEND <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
)
) GROUP BY
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE;

DROP VIEW EBS_ECL_CBLP_KSGMM_VIEW;
CREATE VIEW EBS_ECL_CBLP_KGSMM_VIEW AS 
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
SUM(MM_1) AS KG_MM_1,
SUM(MM_2) AS KG_MM_2,
SUM(MM_3) AS KG_MM_3,
SUM(MM_4) AS KG_MM_4,
SUM(MM_5) AS KG_MM_5,
SUM(MM_6) AS KG_MM_6,
SUM(MM_7) AS KG_MM_7,
SUM(MM_8) AS KG_MM_8,
SUM(MM_9) AS KG_MM_9,
SUM(MM_10) AS KG_MM_10,
SUM(MM_11) AS KG_MM_11,
SUM(MM_12) AS KG_MM_12,
SUM(MM_13) AS KG_MM_13,
SUM(MM_14) AS KG_MM_14,
SUM(MM_15) AS KG_MM_15,
SUM(MM_16) AS KG_MM_16,
SUM(MM_17) AS KG_MM_17,
SUM(MM_18) AS KG_MM_18,
SUM(MM_19) AS KG_MM_19,
SUM(MM_20) AS KG_MM_20,
SUM(MM_21) AS KG_MM_21,
SUM(MM_22) AS KG_MM_22,
SUM(MM_23) AS KG_MM_23,
SUM(MM_24) AS KG_MM_24
FROM
(
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_1,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_2,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_3,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_4,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_5,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_6,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_7,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_8,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_9,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_10,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_11,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_12,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_13,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_14,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_15,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_16,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_17,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_18,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_19,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_20,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_21,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_22,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_23,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_24
FROM 
(
SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_UNIT_KGS
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(UNIT_KGS) AS MM_UNIT_KGS
    FROM EBS_ECL_CHN_BASIC_LP_VIEW WHERE UNIT_KGS <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
)
) GROUP BY
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE;

DROP VIEW EBS_ECL_CBLP_SPDMM_VIEW;
CREATE VIEW EBS_ECL_CBLP_SPDMM_VIEW AS 
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
SUM(MM_1) AS SPEND_MM_1,
SUM(MM_2) AS SPEND_MM_2,
SUM(MM_3) AS SPEND_MM_3,
SUM(MM_4) AS SPEND_MM_4,
SUM(MM_5) AS SPEND_MM_5,
SUM(MM_6) AS SPEND_MM_6,
SUM(MM_7) AS SPEND_MM_7,
SUM(MM_8) AS SPEND_MM_8,
SUM(MM_9) AS SPEND_MM_9,
SUM(MM_10) AS SPEND_MM_10,
SUM(MM_11) AS SPEND_MM_11,
SUM(MM_12) AS SPEND_MM_12,
SUM(MM_13) AS SPEND_MM_13,
SUM(MM_14) AS SPEND_MM_14,
SUM(MM_15) AS SPEND_MM_15,
SUM(MM_16) AS SPEND_MM_16,
SUM(MM_17) AS SPEND_MM_17,
SUM(MM_18) AS SPEND_MM_18,
SUM(MM_19) AS SPEND_MM_19,
SUM(MM_20) AS SPEND_MM_20,
SUM(MM_21) AS SPEND_MM_21,
SUM(MM_22) AS SPEND_MM_22,
SUM(MM_23) AS SPEND_MM_23,
SUM(MM_24) AS SPEND_MM_24
FROM
(
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_1,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_2,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_3,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_4,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_5,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_6,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_7,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_8,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_9,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_10,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_11,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_12,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_13,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_14,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_15,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_16,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_17,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_18,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_19,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_20,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_21,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_22,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_23,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_24
FROM 
(
SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_TOT_SPEND
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(TOT_SPEND) AS MM_TOT_SPEND
    FROM EBS_ECL_CHN_BASIC_LP_VIEW WHERE TOT_SPEND <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
)
) GROUP BY
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE;


DROP VIEW EBS_NALCO_CHN_KSGMM_VIEW;
CREATE VIEW EBS_NALCO_CHN_KSGMM_VIEW AS 
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
SUM(MM_1) AS KG_MM_1,
SUM(MM_2) AS KG_MM_2,
SUM(MM_3) AS KG_MM_3,
SUM(MM_4) AS KG_MM_4,
SUM(MM_5) AS KG_MM_5,
SUM(MM_6) AS KG_MM_6,
SUM(MM_7) AS KG_MM_7,
SUM(MM_8) AS KG_MM_8,
SUM(MM_9) AS KG_MM_9,
SUM(MM_10) AS KG_MM_10,
SUM(MM_11) AS KG_MM_11,
SUM(MM_12) AS KG_MM_12,
SUM(MM_13) AS KG_MM_13,
SUM(MM_14) AS KG_MM_14,
SUM(MM_15) AS KG_MM_15,
SUM(MM_16) AS KG_MM_16,
SUM(MM_17) AS KG_MM_17,
SUM(MM_18) AS KG_MM_18,
SUM(MM_19) AS KG_MM_19,
SUM(MM_20) AS KG_MM_20,
SUM(MM_21) AS KG_MM_21,
SUM(MM_22) AS KG_MM_22,
SUM(MM_23) AS KG_MM_23,
SUM(MM_24) AS KG_MM_24
FROM
(
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_1,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_2,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_3,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_4,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_5,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_6,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_7,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_8,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_9,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_10,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_11,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_12,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_13,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_14,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_15,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_16,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_17,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_18,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_19,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_20,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_21,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_22,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_23,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'MM') THEN MM_UNIT_KGS ELSE 0 END AS MM_24
FROM 
(
SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_UNIT_KGS
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(UNIT_KGS) AS MM_UNIT_KGS
    FROM EBS_NALCO_CHN_RES_VIEW WHERE UNIT_KGS <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
)
) GROUP BY
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE;

DROP VIEW EBS_NALCO_CHN_SPDMM_VIEW;
CREATE VIEW EBS_NALCO_CHN_SPDMM_VIEW AS 
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
SUM(MM_1) AS SPEND_MM_1,
SUM(MM_2) AS SPEND_MM_2,
SUM(MM_3) AS SPEND_MM_3,
SUM(MM_4) AS SPEND_MM_4,
SUM(MM_5) AS SPEND_MM_5,
SUM(MM_6) AS SPEND_MM_6,
SUM(MM_7) AS SPEND_MM_7,
SUM(MM_8) AS SPEND_MM_8,
SUM(MM_9) AS SPEND_MM_9,
SUM(MM_10) AS SPEND_MM_10,
SUM(MM_11) AS SPEND_MM_11,
SUM(MM_12) AS SPEND_MM_12,
SUM(MM_13) AS SPEND_MM_13,
SUM(MM_14) AS SPEND_MM_14,
SUM(MM_15) AS SPEND_MM_15,
SUM(MM_16) AS SPEND_MM_16,
SUM(MM_17) AS SPEND_MM_17,
SUM(MM_18) AS SPEND_MM_18,
SUM(MM_19) AS SPEND_MM_19,
SUM(MM_20) AS SPEND_MM_20,
SUM(MM_21) AS SPEND_MM_21,
SUM(MM_22) AS SPEND_MM_22,
SUM(MM_23) AS SPEND_MM_23,
SUM(MM_24) AS SPEND_MM_24
FROM
(
SELECT
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 730,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_1,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 700,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_2,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 669,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_3,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 639,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_4,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 608,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_5,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 578,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_6,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 547,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_7,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 517,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_8,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 486,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_9,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 456,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_10,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 425,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_11,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 395,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_12,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 364,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_13,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 334,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_14,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 303,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_15,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 273,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_16,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 242,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_17,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 212,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_18,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 181,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_19,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 151,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_20,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 120,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_21,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 90,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_22,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 60,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_23,
CASE WHEN STD_POST_DATE = TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'YYYY')||'-'||TO_CHAR(trunc(add_months(last_day(sysdate), -1) + 1) + 15 - 30,'MM') THEN MM_TOT_SPEND ELSE 0 END AS MM_24
FROM 
(
SELECT LEGACY
    ||'_'
    ||PLANT
    ||'_'
    ||EBS_CODE
    ||'_'
    ||STD_POST_DATE AS ID,
    LEGACY,
    PLANT,
    EBS_CODE,
    STD_POST_DATE,
    MM_TOT_SPEND
  FROM
    (SELECT LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT,
      SUM(TOT_SPEND) AS MM_TOT_SPEND
    FROM EBS_NALCO_CHN_RES_VIEW WHERE TOT_SPEND <> 0
    GROUP BY LEGACY,
      EBS_CODE,
      STD_POST_DATE,
      PLANT
    )
)
) GROUP BY
ID,
LEGACY,
PLANT,
EBS_CODE,
STD_POST_DATE;















select to_char(trunc(add_months(last_day(sysdate), -1) + 1) + 15, 'yyyy-mm-dd') from dual
-----------------------------------------------------------------------------------------------------------------
-- CREATE VIEW FOR SCI HISTORY DATA
DROP VIEW SCI_DATA_VIEW;
CREATE OR REPLACE VIEW SCI_DATA_VIEW  AS
  SELECT 
    SUBSTR(ITEM_DATA.PUBLISH_DATE,1,4)||''||SUBSTR(ITEM_DATA.PUBLISH_DATE,6,2)||''||BASE_ITEM.Guid AS ID,
    SUBSTR(ITEM_DATA.PUBLISH_DATE,1,4)||''||SUBSTR(ITEM_DATA.PUBLISH_DATE,6,2)                              AS YEARMONTH,
    ITEM_DATA.PUBLISH_DATE,
    BASE_ITEM.ClassName,
    BASE_ITEM.Type,
    ITEM_DATA.Max,
    ITEM_DATA.Value,
    ITEM_DATA.Min,
    BASE_ITEM.Unit,
    BASE_ITEM.ProvinceName,
    BASE_ITEM.PriceCondition,
    BASE_ITEM.CityName,
    BASE_ITEM.MarketName,
    BASE_ITEM.FactoryName,
    BASE_ITEM.Model,
    BASE_ITEM.PaymentMethod,
    BASE_ITEM.AreaName,
    BASE_ITEM.Guid,
    BASE_ITEM.Name
  FROM
    (SELECT Guid, PUBLISH_DATE, MAX, Value, MIN FROM SCI_ITEM_DATA
    )ITEM_DATA
  LEFT JOIN
    (SELECT Type,
      ProvinceName,
      PriceCondition,
      CityName,
      ClassName,
      MarketName,
      FactoryName,
      Model,
      PaymentMethod,
      AreaName,
      Guid,
      Name,
      Unit
    FROM SCI_ITEM_MASTER
    )BASE_ITEM
  ON ITEM_DATA.GUID = BASE_ITEM.GUID;
  
-- CREATE VIEW FOR SCI FORECAST DATA
DROP VIEW SCI_FC_DATA_VIEW;
CREATE OR REPLACE VIEW SCI_FC_DATA_VIEW  AS
SELECT SUBSTR(ITEM_DATA.PUBLISH_DATE,1,4)||''||SUBSTR(ITEM_DATA.PUBLISH_DATE,6,2)||''||BASE_ITEM.Guid AS ID,
    SUBSTR(ITEM_DATA.PUBLISH_DATE,1,4)||''||SUBSTR(ITEM_DATA.PUBLISH_DATE,6,2)                              AS YEARMONTH,
    ITEM_DATA.PUBLISH_DATE,
    BASE_ITEM.ClassName,
    BASE_ITEM.Type,
    ITEM_DATA.Value AS MAX,
    ITEM_DATA.Value,
    ITEM_DATA.Value AS MIN,
    BASE_ITEM.Unit,
    BASE_ITEM.ProvinceName,
    BASE_ITEM.PriceCondition,
    BASE_ITEM.CityName,
    BASE_ITEM.MarketName,
    BASE_ITEM.FactoryName,
    BASE_ITEM.Model,
    BASE_ITEM.PaymentMethod,
    BASE_ITEM.AreaName,
    BASE_ITEM.Guid,
    BASE_ITEM.Name
  FROM
    (SELECT GET_DATE,
      Guid,
      PUBLISH_DATE,
      Value
    FROM SCI_FORECAST_DATA
    )ITEM_DATA
  LEFT JOIN
    (SELECT Type,
      ProvinceName,
      PriceCondition,
      CityName,
      ClassName,
      MarketName,
      FactoryName,
      Model,
      PaymentMethod,
      AreaName,
      Guid,
      Name,
      Unit
    FROM SCI_ITEM_MASTER
    )BASE_ITEM
  ON ITEM_DATA.GUID = BASE_ITEM.GUID;
  
-- Combine FC data and His data together.
SELECT ID,
  YEARMONTH,
  PUBLISH_DATE,
  CLASSNAME,
  TYPE,
  MAX,
  VALUE,
  MIN,
  UNIT,
  PROVINCENAME,
  PRICECONDITION,
  CITYNAME,
  MARKETNAME,
  FACTORYNAME,
  MODEL,
  PAYMENTMETHOD,
  AREANAME,
  GUID,
  NAME
FROM SCI_FC_DATA_VIEW where GUID = '9e428979-6d34-4960-9754-88d24180f9a6'
UNION ALL
SELECT ID,
  YEARMONTH,
  PUBLISH_DATE,
  CLASSNAME,
  TYPE,
  MAX,
  VALUE,
  MIN,
  UNIT,
  PROVINCENAME,
  PRICECONDITION,
  CITYNAME,
  MARKETNAME,
  FACTORYNAME,
  MODEL,
  PAYMENTMETHOD,
  AREANAME,
  GUID,
  NAME
FROM SCI_DATA_VIEW;
--------------------------------------------------------------------------------------------------
-- Monthly Price For SCI
SELECT * FROM SCI_DATA_MONTHLY_VIEW WHERE GUID = '5df373b6-9d5a-4ab8-8dbe-74c050c95f78';

DROP VIEW SCI_DATA_MONTHLY_VIEW;
CREATE VIEW SCI_DATA_MONTHLY_VIEW AS
  SELECT BASE_ITEM.Type,
    BASE_ITEM.ProvinceName,
    BASE_ITEM.PriceCondition,
    BASE_ITEM.CityName,
    BASE_ITEM.ClassName,
    BASE_ITEM.MarketName,
    BASE_ITEM.FactoryName,
    BASE_ITEM.Model,
    BASE_ITEM.PaymentMethod,
    BASE_ITEM.AreaName,
    BASE_ITEM.Guid,
    BASE_ITEM.Name,
    BASE_ITEM.Unit,
    ITEM_DATA_MM.ID,
    ITEM_DATA_MM.YEARMONTH,
    ITEM_DATA_MM.VAL_AVG_MM
  FROM
    (SELECT ID,
      YEARMONTH,
      VAL_AVG_MM,
      GUID
    FROM (
      (SELECT ID,
        YEARMONTH,
        AVG(VALUE) AS VAL_AVG_MM,
        GUID
      FROM SCI_DATA_VIEW
      WHERE VALUE <> 0
      GROUP BY ID,
        GUID,
        YEARMONTH
      )
    UNION
      (SELECT ID,
        YEARMONTH,
        AVG(VALUE) AS VAL_AVG_MM,
        GUID
      FROM SCI_FC_DATA_VIEW
      WHERE value <> 0
      GROUP BY ID,
        GUID,
        YEARMONTH
      ) )
    )ITEM_DATA_MM
  LEFT JOIN
    (SELECT Type,
      ProvinceName,
      PriceCondition,
      CityName,
      ClassName,
      MarketName,
      FactoryName,
      Model,
      PaymentMethod,
      AreaName,
      Guid,
      Name,
      Unit
    FROM SCI_ITEM_MASTER
    )BASE_ITEM
  ON BASE_ITEM.GUID = ITEM_DATA_MM.GUID;

SELECT TO_CHAR(SYSDATE - 730,'YYYY')||''||TO_CHAR(SYSDATE - 730,'MM') FROM DUAL;

SELECT ID, NAME,YEARMONTH, VAL_AVG_MM FROM SCI_DATA_MONTHLY_VIEW

