SELECT CONCAT('010-','888888888')||'zhuan23' peter FROM DUAL;


select sql_id, sql_text, bind_data, hash_value 
from v$sql 
where sql_text like '%select * from DUAL where id1%';


--With As

SELECT ID,
  MAX_DIS_NAME,
  MAX_DIS_USAGE_QTY,
  TOT_USAGE_QTY,
  ROUND(PERSENTAGE,4) AS PER_BIGGEST,
  DISTRIBUTOR_COUNT
FROM
  (SELECT ID,
    MAX_DIS_USAGE_QTY,
    MAX_DIS_NAME,
    TOT_USAGE_QTY,
    PERSENTAGE,
    DISTRIBUTOR_COUNT,
    ROWID
  FROM INV_SAP_DSTR_STATS
  WHERE (ID,ROWID) IN
    (SELECT ID,MIN(ROWID) FROM INV_SAP_DSTR_STATS GROUP BY ID
    )
  )
  
  
WITH A AS
  (SELECT ID,
    MAX_DIS_USAGE_QTY,
    MAX_DIS_NAME,
    TOT_USAGE_QTY,
    PERSENTAGE,
    DISTRIBUTOR_COUNT,
    ROWID
  FROM INV_SAP_DSTR_STATS
  ),
  B AS
  (SELECT ID, MIN(ROWID) FROM INV_SAP_DSTR_STATS GROUP BY ID
  )
SELECT A.ID,
  A.MAX_DIS_USAGE_QTY,
  A.MAX_DIS_NAME,
  A.TOT_USAGE_QTY,
  A.PERSENTAGE,
  A.DISTRIBUTOR_COUNT,
  A.ROWID
FROM A
WHERE (A.ID,
  A.ROWID) IN
  (SELECT * FROM B
  );
  
-- Fenye

