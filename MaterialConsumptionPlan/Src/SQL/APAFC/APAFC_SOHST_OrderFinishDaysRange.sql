
  CREATE OR REPLACE FORCE VIEW "DWQ$LIBRARIAN"."INV_SAP_SALES_CROSS507_V" ("MATERIALID", "DEM_QTY", "ORDERAMT", "LINE_COUNT", "ORDERLINE_COUNTED", "ONTIMETO_CONFIRMED", "DEM_AMT", "WEEK", "WEEK_DATE", "RANK_WEEK", "MAX_WEEK_DAY", "MATL_FIRST_USED", "PLANTID", "OUTLIER_CURRECTED") AS 
  SELECT Lst.Materialid Materialid,
          NVL (usg.ORDERQTY, 0) dem_QTY,
          NVL (OrderAmt, 0) OrderAmt,
          NVL (line_count, 0) line_count,
          NVL (ORDERLINE_COUNTED, 0) ORDERLINE_COUNTED,
          NVL (Ontimeto_Confirmed, 0) Ontimeto_Confirmed,
          NVL (No_Corr_OrderQty, 0) Dem_amt,
          lst.WWYEAR week,
          lst.WEEK_DATE,
          lst.rank_week,
          Lst.Max_Week_Day,
          Matl_First_Used,
          Lst.Plantid,
          NVL (Outlier_Currected, 0) Outlier_Currected
     FROM (  SELECT Materialid Materialid,
                    plantid plantid,
                    SUM (line_count) line_count,
                    SUM (ORDERQTY) OrderQty,
                    SUM (OrderAmt) OrderAmt,
                    SUM (ORDERLINE_COUNTED) ORDERLINE_COUNTED,
                    SUM (Ontimeto_Confirmed) Ontimeto_Confirmed,
                    Week_Yyyyww,
                    MAX (NVL (No_Corr_Orderqty, 0)) No_Corr_Orderqty,
                    MAX (NVL (Outlier_Currected, 0)) Outlier_Currected
               FROM INV_SAP_SALES_WKS507
           GROUP BY MATERIALID, plantid, WEEK_YYYYWW) usg,
          (SELECT Materialid,
                  plantid,
                  weeklist.WWYEAR,
                  weeklist.WEEK_YYYYWW,
                  weeklist.week_date,
                  weeklist.rank_week,
                  matlist.MATL_FIRST_USED MATL_FIRST_USED,
                  MAX_WEEK_DAY
             FROM (  SELECT Materialid,
                            Expected_Ship_plant plantid,
                            TO_DATE (MIN (First_used), 'yyyymmdd')
                               MATL_FIRST_USED
                       FROM (SELECT *
                               FROM INV_SAP_SALES_VBAP_1STUSED
                              WHERE materialid IS NOT NULL
                             UNION ALL
                             SELECT *
                               FROM INV_PS_SALES_CEDC_1STUSED --CPZ Added 02-07-2012 for CEDC Data
                              WHERE materialid IS NOT NULL
                             UNION ALL
                             SELECT *
                               FROM INV_COM_SALES_SA_1STUSED
                              WHERE materialid IS NOT NULL
                             UNION ALL
                             SELECT *
                               FROM INV_MPRO_SALES_SEA_1STUSED
                              WHERE materialid IS NOT NULL
                             UNION ALL
                             SELECT *
                               FROM INV_SAP_SALES_R8_DC1STUSED
                              WHERE materialid IS NOT NULL
                             UNION ALL
                             SELECT *
                               FROM INV_SAP_SALES_R9_DC1STUSED
                              WHERE materialid IS NOT NULL) combined
                   GROUP BY MATERIALID, Expected_Ship_plant) matlist,
                  (SELECT DISTINCT WWYEAR,
                                   rank_week,
                                   MAX_WEEK_DAY,
                                   week_date,
                                   WEEK_YYYYWW
                     FROM INV_SAP_DATE_PVT
                    WHERE WEEK_YYYYWW <=
                             (SELECT MAX (WEEK_YYYYWW) --- in case missing weeks of data for refresh
                                                      FROM INV_SAP_SALES_WKS))
                  weeklist
            WHERE matlist.MATL_FIRST_USED <= weeklist.week_date ---- avg will be calculated based on the first time used
                                                               ) lst
    WHERE     Lst.Materialid = Usg.Materialid(+)
          AND lst.plantid = usg.plantid(+)
          
          AND lst.WEEK_YYYYWW = usg.WEEK_YYYYWW(+);



select       Materialid,
      percent_rank() over(partition by deptno order by sal desc) p_rank,
      PERCENTILE_CONT(0) within group(order by line_count desc)
        over(partition by deptno) max_sal ,
      PERCENTILE_CONT(0.25) within group(order by sal desc)
        over(partition by deptno) max_sal_25,
      PERCENTILE_CONT(0.5) within group(order by sal desc)
        over(partition by deptno) max_sal_50,
      PERCENTILE_CONT(0.75) within group(order by sal desc)
        over(partition by deptno) max_sal_75