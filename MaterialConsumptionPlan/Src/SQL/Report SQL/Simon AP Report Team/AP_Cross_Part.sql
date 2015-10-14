
DROP VIEW VIEW_INV_SAP_AP_CROSS_PART;
DROP TABLE INV_SAP_AP_CROSS_PART;
CREATE TABLE INV_SAP_AP_CROSS_PART AS SELECT * FROM VIEW_INV_SAP_AP_CROSS_PART;

SELECT * FROM INV_SAP_AP_CROSS_PART WHERE MATERIAL = '1756-IB16 A';
CREATE VIEW VIEW_INV_SAP_AP_CROSS_PART AS  
SELECT 
  MATERIAL,
  PLANT,
  LPAD(GREATEST (LPAD (ROUND (AVG26_USAGE_QTY_5120, 0), 12, 0)
  || '-5120', LPAD (ROUND (AVG26_USAGE_QTY_5160, 0), 12, 0)
  || '-5160', LPAD (ROUND (AVG26_USAGE_QTY_5200, 0), 12, 0)
  || '-5200',LPAD (ROUND (AVG26_USAGE_QTY_5040, 0), 12, 0)
  || '-5040', LPAD (ROUND (AVG26_USAGE_QTY_5050, 0), 12, 0)
  || '-5050', LPAD (ROUND (AVG26_USAGE_QTY_5100, 0), 12, 0)
  || '-5100', LPAD (ROUND (AVG26_USAGE_QTY_5110, 0), 12, 0)
  || '-5110', LPAD (ROUND (AVG26_USAGE_QTY_5190, 0), 12, 0)
  || '-5190', LPAD (ROUND (AVG26_USAGE_QTY_5070, 0), 12, 0)
  || '-5070', LPAD (ROUND (AVG26_USAGE_QTY_5140, 0), 12, 0)
  || '-5140'),12,0) AS MAX_USER,
  CATALOG_DASH,
  UNIT,
  PROD_BU,
  AVG_MONTHLY_DEM_5040,
  AVG_MONTHLY_DEM_5050,
  AVG_MONTHLY_DEM_5100,
  AVG_MONTHLY_DEM_5110,
  AVG_MONTHLY_DEM_5120,
  AVG_MONTHLY_DEM_5160,
  AVG_MONTHLY_DEM_5190,
  AVG_MONTHLY_DEM_5200,
  AVG_MONTHLY_DEM_5070,
  AVG_MONTHLY_DEM_5140,
  AVG13_USAGE_QTY_5040,
  AVG13_USAGE_QTY_5050,
  AVG13_USAGE_QTY_5100,
  AVG13_USAGE_QTY_5110,
  AVG13_USAGE_QTY_5120,
  AVG13_USAGE_QTY_5160,
  AVG13_USAGE_QTY_5190,
  AVG13_USAGE_QTY_5200,
  AVG13_USAGE_QTY_5070,
  AVG13_USAGE_QTY_5140,
  AVG26_USAGE_QTY_5040,
  AVG26_USAGE_QTY_5050,
  AVG26_USAGE_QTY_5100,
  AVG26_USAGE_QTY_5110,
  AVG26_USAGE_QTY_5120,
  AVG26_USAGE_QTY_5160,
  AVG26_USAGE_QTY_5190,
  AVG26_USAGE_QTY_5200,
  AVG26_USAGE_QTY_5070,
  AVG26_USAGE_QTY_5140,
  AVG52_USAGE_QTY_5040,
  AVG52_USAGE_QTY_5050,
  AVG52_USAGE_QTY_5100,
  AVG52_USAGE_QTY_5110,
  AVG52_USAGE_QTY_5120,
  AVG52_USAGE_QTY_5160,
  AVG52_USAGE_QTY_5190,
  AVG52_USAGE_QTY_5200,
  AVG52_USAGE_QTY_5070,
  AVG52_USAGE_QTY_5140,
  D_CHAIN_BLK_5040,
  D_CHAIN_BLK_5050,
  D_CHAIN_BLK_5100,
  D_CHAIN_BLK_5110,
  D_CHAIN_BLK_5120,
  D_CHAIN_BLK_5160,
  D_CHAIN_BLK_5190,
  D_CHAIN_BLK_5200,
  D_CHAIN_BLK_5070,
  D_CHAIN_BLK_5140,
  LEAD_TIME_5040,
  LEAD_TIME_5050,
  LEAD_TIME_5100,
  LEAD_TIME_5110,
  LEAD_TIME_5120,
  LEAD_TIME_5160,
  LEAD_TIME_5190,
  LEAD_TIME_5200,
  LEAD_TIME_5070,
  LEAD_TIME_5140,
  PDT_5040,
  PDT_5050,
  PDT_5100,
  PDT_5110,
  PDT_5120,
  PDT_5160,
  PDT_5190,
  PDT_5200,
  PDT_5070,
  PDT_5140,
  GRT_5040,
  GRT_5050,
  GRT_5100,
  GRT_5110,
  GRT_5120,
  GRT_5160,
  GRT_5190,
  GRT_5200,
  GRT_5070,
  GRT_5140,
  MRP_CONTROLLER_5040,
  MRP_CONTROLLER_5050,
  MRP_CONTROLLER_5100,
  MRP_CONTROLLER_5110,
  MRP_CONTROLLER_5120,
  MRP_CONTROLLER_5160,
  MRP_CONTROLLER_5190,
  MRP_CONTROLLER_5200,
  MRP_CONTROLLER_5070,
  MRP_CONTROLLER_5140,
  MRP_TYPE_5040,
  MRP_TYPE_5050,
  MRP_TYPE_5100,
  MRP_TYPE_5110,
  MRP_TYPE_5120,
  MRP_TYPE_5160,
  MRP_TYPE_5190,
  MRP_TYPE_5200,
  MRP_TYPE_5070,
  MRP_TYPE_5140,
  OH_QTY_5040,
  OH_QTY_5050,
  OH_QTY_5100,
  OH_QTY_5110,
  OH_QTY_5120,
  OH_QTY_5160,
  OH_QTY_5190,
  OH_QTY_5200,
  OH_QTY_5070,
  OH_QTY_5140,
  SAFETY_STOCK_5040,
  SAFETY_STOCK_5050,
  SAFETY_STOCK_5100,
  SAFETY_STOCK_5110,
  SAFETY_STOCK_5120,
  SAFETY_STOCK_5160,
  SAFETY_STOCK_5190,
  SAFETY_STOCK_5200,
  SAFETY_STOCK_5070,
  SAFETY_STOCK_5140,
  STRATEGY_GRP_5040,
  STRATEGY_GRP_5050,
  STRATEGY_GRP_5100,
  STRATEGY_GRP_5110,
  STRATEGY_GRP_5120,
  STRATEGY_GRP_5160,
  STRATEGY_GRP_5190,
  STRATEGY_GRP_5200,
  STRATEGY_GRP_5070,
  STRATEGY_GRP_5140,
  UNIT_COST_5040,
  UNIT_COST_5050,
  UNIT_COST_5100,
  UNIT_COST_5110,
  UNIT_COST_5120,
  UNIT_COST_5160,
  UNIT_COST_5190,
  UNIT_COST_5200,
  UNIT_COST_5070,
  UNIT_COST_5140,
  VENDOR_ITEM_5040,
  VENDOR_ITEM_5050,
  VENDOR_ITEM_5100,
  VENDOR_ITEM_5110,
  VENDOR_ITEM_5120,
  VENDOR_ITEM_5160,
  VENDOR_ITEM_5190,
  VENDOR_ITEM_5200,
  VENDOR_ITEM_5070,
  VENDOR_ITEM_5140
FROM
  (SELECT PP_OPT.MATERIAL    AS MATERIAL,
    PP_OPT.PLANT             AS PLANT,
    PP_OPT.CATALOG_DASH      AS CATALOG_DASH,
    PP_OPT.UNIT              AS UNIT,
    PP_OPT.PROD_BU           AS PROD_BU,
    OPT_5040.SAFETY_STOCK    AS SAFETY_STOCK_5040,
    OPT_5040.OH_QTY          AS OH_QTY_5040,
    OPT_5040.UNIT_COST       AS UNIT_COST_5040,
    OPT_5040.STRATEGY_GRP    AS STRATEGY_GRP_5040,
    OPT_5040.MRP_TYPE        AS MRP_TYPE_5040,
    OPT_5040.VENDOR_ITEM     AS VENDOR_ITEM_5040,
    OPT_5040.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5040,
    OPT_5040.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5040,
    OPT_5040.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5040,
    OPT_5040.MRP_CONTROLLER  AS MRP_CONTROLLER_5040,
    OPT_5040.LEAD_TIME       AS LEAD_TIME_5040,
    OPT_5040.PDT             AS PDT_5040,
    OPT_5040.GRT             AS GRT_5040,
    OPT_5040.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5040,
    OPT_5040.D_CHAIN_BLK     AS D_CHAIN_BLK_5040,
    OPT_5050.SAFETY_STOCK    AS SAFETY_STOCK_5050,
    OPT_5050.OH_QTY          AS OH_QTY_5050,
    OPT_5050.UNIT_COST       AS UNIT_COST_5050,
    OPT_5050.STRATEGY_GRP    AS STRATEGY_GRP_5050,
    OPT_5050.MRP_TYPE        AS MRP_TYPE_5050,
    OPT_5050.VENDOR_ITEM     AS VENDOR_ITEM_5050,
    OPT_5050.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5050,
    OPT_5050.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5050,
    OPT_5050.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5050,
    OPT_5050.MRP_CONTROLLER  AS MRP_CONTROLLER_5050,
    OPT_5050.LEAD_TIME       AS LEAD_TIME_5050,
    OPT_5050.PDT             AS PDT_5050,
    OPT_5050.GRT             AS GRT_5050,
    OPT_5050.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5050,
    OPT_5050.D_CHAIN_BLK     AS D_CHAIN_BLK_5050,
    OPT_5100.SAFETY_STOCK    AS SAFETY_STOCK_5100,
    OPT_5100.OH_QTY          AS OH_QTY_5100,
    OPT_5100.UNIT_COST       AS UNIT_COST_5100,
    OPT_5100.STRATEGY_GRP    AS STRATEGY_GRP_5100,
    OPT_5100.MRP_TYPE        AS MRP_TYPE_5100,
    OPT_5100.VENDOR_ITEM     AS VENDOR_ITEM_5100,
    OPT_5100.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5100,
    OPT_5100.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5100,
    OPT_5100.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5100,
    OPT_5100.MRP_CONTROLLER  AS MRP_CONTROLLER_5100,
    OPT_5100.LEAD_TIME       AS LEAD_TIME_5100,
    OPT_5100.PDT             AS PDT_5100,
    OPT_5100.GRT             AS GRT_5100,
    OPT_5100.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5100,
    OPT_5100.D_CHAIN_BLK     AS D_CHAIN_BLK_5100,
    OPT_5110.SAFETY_STOCK    AS SAFETY_STOCK_5110,
    OPT_5110.OH_QTY          AS OH_QTY_5110,
    OPT_5110.UNIT_COST       AS UNIT_COST_5110,
    OPT_5110.STRATEGY_GRP    AS STRATEGY_GRP_5110,
    OPT_5110.MRP_TYPE        AS MRP_TYPE_5110,
    OPT_5110.VENDOR_ITEM     AS VENDOR_ITEM_5110,
    OPT_5110.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5110,
    OPT_5110.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5110,
    OPT_5110.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5110,
    OPT_5110.MRP_CONTROLLER  AS MRP_CONTROLLER_5110,
    OPT_5110.LEAD_TIME       AS LEAD_TIME_5110,
    OPT_5110.PDT             AS PDT_5110,
    OPT_5110.GRT             AS GRT_5110,
    OPT_5110.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5110,
    OPT_5110.D_CHAIN_BLK     AS D_CHAIN_BLK_5110,
    OPT_5120.SAFETY_STOCK    AS SAFETY_STOCK_5120,
    OPT_5120.OH_QTY          AS OH_QTY_5120,
    OPT_5120.UNIT_COST       AS UNIT_COST_5120,
    OPT_5120.STRATEGY_GRP    AS STRATEGY_GRP_5120,
    OPT_5120.MRP_TYPE        AS MRP_TYPE_5120,
    OPT_5120.VENDOR_ITEM     AS VENDOR_ITEM_5120,
    OPT_5120.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5120,
    OPT_5120.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5120,
    OPT_5120.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5120,
    OPT_5120.MRP_CONTROLLER  AS MRP_CONTROLLER_5120,
    OPT_5120.LEAD_TIME       AS LEAD_TIME_5120,
    OPT_5120.PDT             AS PDT_5120,
    OPT_5120.GRT             AS GRT_5120,
    OPT_5120.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5120,
    OPT_5120.D_CHAIN_BLK     AS D_CHAIN_BLK_5120,
    OPT_5160.SAFETY_STOCK    AS SAFETY_STOCK_5160,
    OPT_5160.OH_QTY          AS OH_QTY_5160,
    OPT_5160.UNIT_COST       AS UNIT_COST_5160,
    OPT_5160.STRATEGY_GRP    AS STRATEGY_GRP_5160,
    OPT_5160.MRP_TYPE        AS MRP_TYPE_5160,
    OPT_5160.VENDOR_ITEM     AS VENDOR_ITEM_5160,
    OPT_5160.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5160,
    OPT_5160.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5160,
    OPT_5160.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5160,
    OPT_5160.MRP_CONTROLLER  AS MRP_CONTROLLER_5160,
    OPT_5160.LEAD_TIME       AS LEAD_TIME_5160,
    OPT_5160.PDT             AS PDT_5160,
    OPT_5160.GRT             AS GRT_5160,
    OPT_5160.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5160,
    OPT_5160.D_CHAIN_BLK     AS D_CHAIN_BLK_5160,
    OPT_5190.SAFETY_STOCK    AS SAFETY_STOCK_5190,
    OPT_5190.OH_QTY          AS OH_QTY_5190,
    OPT_5190.UNIT_COST       AS UNIT_COST_5190,
    OPT_5190.STRATEGY_GRP    AS STRATEGY_GRP_5190,
    OPT_5190.MRP_TYPE        AS MRP_TYPE_5190,
    OPT_5190.VENDOR_ITEM     AS VENDOR_ITEM_5190,
    OPT_5190.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5190,
    OPT_5190.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5190,
    OPT_5190.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5190,
    OPT_5190.MRP_CONTROLLER  AS MRP_CONTROLLER_5190,
    OPT_5190.LEAD_TIME       AS LEAD_TIME_5190,
    OPT_5190.PDT             AS PDT_5190,
    OPT_5190.GRT             AS GRT_5190,
    OPT_5190.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5190,
    OPT_5190.D_CHAIN_BLK     AS D_CHAIN_BLK_5190,
    OPT_5200.SAFETY_STOCK    AS SAFETY_STOCK_5200,
    OPT_5200.OH_QTY          AS OH_QTY_5200,
    OPT_5200.UNIT_COST       AS UNIT_COST_5200,
    OPT_5200.STRATEGY_GRP    AS STRATEGY_GRP_5200,
    OPT_5200.MRP_TYPE        AS MRP_TYPE_5200,
    OPT_5200.VENDOR_ITEM     AS VENDOR_ITEM_5200,
    OPT_5200.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5200,
    OPT_5200.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5200,
    OPT_5200.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5200,
    OPT_5200.MRP_CONTROLLER  AS MRP_CONTROLLER_5200,
    OPT_5200.LEAD_TIME       AS LEAD_TIME_5200,
    OPT_5200.PDT             AS PDT_5200,
    OPT_5200.GRT             AS GRT_5200,
    OPT_5200.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5200,
    OPT_5200.D_CHAIN_BLK     AS D_CHAIN_BLK_5200,
    OPT_5070.SAFETY_STOCK    AS SAFETY_STOCK_5070,
    OPT_5070.OH_QTY          AS OH_QTY_5070,
    OPT_5070.UNIT_COST       AS UNIT_COST_5070,
    OPT_5070.STRATEGY_GRP    AS STRATEGY_GRP_5070,
    OPT_5070.MRP_TYPE        AS MRP_TYPE_5070,
    OPT_5070.VENDOR_ITEM     AS VENDOR_ITEM_5070,
    OPT_5070.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5070,
    OPT_5070.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5070,
    OPT_5070.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5070,
    OPT_5070.MRP_CONTROLLER  AS MRP_CONTROLLER_5070,
    OPT_5070.LEAD_TIME       AS LEAD_TIME_5070,
    OPT_5070.PDT             AS PDT_5070,
    OPT_5070.GRT             AS GRT_5070,
    OPT_5070.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5070,
    OPT_5070.D_CHAIN_BLK     AS D_CHAIN_BLK_5070,
    OPT_5140.SAFETY_STOCK    AS SAFETY_STOCK_5140,
    OPT_5140.OH_QTY          AS OH_QTY_5140,
    OPT_5140.UNIT_COST       AS UNIT_COST_5140,
    OPT_5140.STRATEGY_GRP    AS STRATEGY_GRP_5140,
    OPT_5140.MRP_TYPE        AS MRP_TYPE_5140,
    OPT_5140.VENDOR_ITEM     AS VENDOR_ITEM_5140,
    OPT_5140.AVG52_USAGE_QTY AS AVG52_USAGE_QTY_5140,
    OPT_5140.AVG26_USAGE_QTY AS AVG26_USAGE_QTY_5140,
    OPT_5140.AVG13_USAGE_QTY AS AVG13_USAGE_QTY_5140,
    OPT_5140.MRP_CONTROLLER  AS MRP_CONTROLLER_5140,
    OPT_5140.LEAD_TIME       AS LEAD_TIME_5140,
    OPT_5140.PDT             AS PDT_5140,
    OPT_5140.GRT             AS GRT_5140,
    OPT_5140.AVG_MONTHLY_DEM AS AVG_MONTHLY_DEM_5140,
    OPT_5140.D_CHAIN_BLK     AS D_CHAIN_BLK_5140
  FROM
    (SELECT MATERIAL,PLANT, CATALOG_DASH, UNIT, PROD_BU FROM INV_SAP_PP_OPT_X WHERE PLANT IN ('5040', '5050', '5100', '5110', '5120', '5160', '5190', '5200','5070','5140')
    )PP_OPT
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5040'
    )OPT_5040
  ON OPT_5040.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5050'
    )OPT_5050
  ON OPT_5050.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5100'
    )OPT_5100
  ON OPT_5100.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5110'
    )OPT_5110
  ON OPT_5110.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5120'
    )OPT_5120
  ON OPT_5120.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5160'
    )OPT_5160
  ON OPT_5160.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5190'
    )OPT_5190
  ON OPT_5190.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5200'
    )OPT_5200
  ON OPT_5200.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5070'
    )OPT_5070
  ON OPT_5070.MATERIAL = PP_OPT.MATERIAL
  LEFT JOIN
    (SELECT MATERIAL,
      SAFETY_STOCK,
      OH_QTY,
      UNIT_COST,
      STRATEGY_GRP,
      MRP_TYPE,
      VENDOR_ITEM,
      AVG52_USAGE_QTY,
      AVG26_USAGE_QTY,
      AVG13_USAGE_QTY,
      MRP_CONTROLLER,
      LEAD_TIME,
      PDT,
      GRT,
      AVG_MONTHLY_DEM,
      D_CHAIN_BLK
    FROM INV_SAP_PP_OPT_X
    WHERE PLANT = '5140'
    )OPT_5140
  ON OPT_5140.MATERIAL = PP_OPT.MATERIAL
  );

SELECT * FROM INV_SAP_PP_OPT_X WHERE MATERIAL = '1756-IB16 A';

















