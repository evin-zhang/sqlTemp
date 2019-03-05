CREATE OR REPLACE PACKAGE BODY PROGRAM.OLTP_RPT_LB_REPORT_COUNT_PKG IS
  /***************************************************************************************************/
  --   NAME:     OLTP_RPT_LB_REPORT_COUNT_PKG
  --   PURPOSE:  报告单数统计
  --   VER        DATE        AUTHOR           DESCRIPTION
  --   ---------  ----------  ---------------  ------------------------------------
  --   1.0        2018-03-15  LQS          CREATED THIS PROCEDURE.
  --   NOTES:
  --   1. [实验室]报告单数统计
  /***************************************************************************************************/
  /*

  KMCS-54007  报告单数统计

  取值逻辑：取已发布界面，已发布的正常报告单，迟发通知书，退单通知书，终止报告单。
  小计：统计每个科室各类报告单的总数；
  合计：统计所有科室小计之和；
  整合报告单&人工综合报告单，单独统计；
  共计：合计+整合报告单+人工综合报告单=共计
  整合报告单&人工综合报告单，单独统计。
  PS：各子公司数据隔离，
      例如：郑州外包给广州10个项目甲状腺功五项，其中广州正常发单8个项目，退单2个项目，回传到郑州，郑州未进行退单，正常发单8个项目。

  REPORT_TYPE --报告单类型0普通组合报告单（正常），1迟发报告单（迟发），2退单通知书（退单），3修改通知单（终止报告），4初步报告单
  REPORT_CATEGORY --0.GPS 1.单张报告单 2.整合报告单 3.人工综合报告单
  REPORT_STATUS--报告单状态0保存，1已批准，2已发布，3已终止，4已销毁，5已冻结，6复核

  */
  PROCEDURE RPT_LB_REPORT_COUNT_PRC(P_RUN_DATE IN DATE DEFAULT NULL) IS
    L_LOG_CON NUMBER;
    L_CUSTOMIZE_EXP EXCEPTION; --自定义异常
    L_LAST_DATE DATE;
    L_RUN_ID    NUMBER;
    L_ERROR_MSG VARCHAR2(3000);
    L_SYSDATE   DATE;
    V_SQL       VARCHAR2(32700);
    V_PROG_NAME VARCHAR2(300);
  BEGIN

    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT=''YYYY-MM-DD''';
    V_PROG_NAME := 'OLTP_RPT_LB_REPORT_COUNT_PKG.RPT_LB_REPORT_COUNT_PRC';

    SELECT COUNT(1)
      INTO L_LOG_CON
      FROM OLTP.RPT_OLTP_RUN_LOG_T T
     WHERE T.EXTRACT_DATE = TRUNC(SYSDATE) - 1
       AND T.RUN_PROGRAM = V_PROG_NAME;
    IF (L_LOG_CON >= 1) THEN
      RAISE L_CUSTOMIZE_EXP; -- 抛出自定义异常
    END IF;

    --获取上次取数时间
    IF (P_RUN_DATE IS NOT NULL) THEN
      L_LAST_DATE := TRUNC(P_RUN_DATE);
      L_SYSDATE   := TRUNC(P_RUN_DATE) + 1;
    ELSE
      RPT_OLTP_PUB.GET_LAST_DATE_PRC(P_RUN_PROGRAM => 'OLTP_RPT_LB_REPORT_COUNT_PKG.RPT_LB_REPORT_COUNT_PRC',
                                     X_LAST_DATE   => L_LAST_DATE);
    END IF;
    --生成本次日志记录
    RPT_OLTP_PUB.GEN_OLTP_LOG_PRC(P_RUN_PROGRAM => 'OLTP_RPT_LB_REPORT_COUNT_PKG.RPT_LB_REPORT_COUNT_PRC',
                                  X_RUN_ID      => L_RUN_ID,
                                  X_SYSDATE     => L_SYSDATE);


    --抽取数据
    BEGIN

    --删除临时表以便重建
    BEGIN
      V_SQL := 'DROP TABLE OLTP.RPT_LB_REPORT_STATISTICS_T1 PURGE ';
      EXECUTE  IMMEDIATE V_SQL;
      EXCEPTION
      WHEN OTHERS THEN
        NULL;
      END;

      V_SQL := '
                CREATE TABLE OLTP.RPT_LB_REPORT_STATISTICS_T1 AS
                  SELECT /*+PARALLEL(6)   index(A IDX_LB_REPORT_P2)  index(B IDX_PK_LB_TASK_ID_P1) */
                         DISTINCT TRUNC(A.RELEASE_TIME) RELEASE_TIME, --发布时间
                                  A.REPORT_ODD,
                                  C.SUB_CODE, --子公司CODE
                                  C.SUB_NAME, --子公司
                                  C.LAB_CODE, --实验室CODE
                                  C.LAB_NAME, --实验室
/*
                                  DEPARTMENT_CODE, --科室CODE
                                  DEPARTMENT_NAME, --科室
                                  ORG_CODE, -- 检测组CODE
                                  ORG_NAME, --检测组
*/
                                  CASE WHEN REPORT_CATEGORY NOT IN (''2'',''3'') THEN C.DEPARTMENT_CODE END DEPARTMENT_CODE, --科室CODE
                                  CASE WHEN REPORT_CATEGORY NOT IN (''2'',''3'') THEN C.DEPARTMENT_NAME END DEPARTMENT_NAME, --科室
                                  CASE WHEN REPORT_CATEGORY NOT IN (''2'',''3'') THEN C.ORG_CODE END ORG_CODE, -- 检测组CODE
                                  CASE WHEN REPORT_CATEGORY NOT IN (''2'',''3'') THEN C.ORG_NAME END ORG_NAME, --检测组

                                  DECODE(A.REPORT_TYPE, ''5'' ,''0'', A.REPORT_TYPE) REPORT_TYPE,  --综合报告单没有迟发和退单一说，只有正常和终止的,当保存5的时候置换成0当做是正常数据统计
                                  DECODE(A.REPORT_CATEGORY,''2'',''2'',''3'',''3'',''1'') REPORT_CATEGORY
                    FROM LB.LB_REPORT@RPT2LB A
                    JOIN LB.LB_REPORT_TASK@RPT2LB B
                      ON A.REPORT_ID = B.REPORT_ID
                     AND B.DELETED_FLAG = ''0''
                    JOIN OLTP.RPT_PL_ORG_V C
                      ON B.TEST_ORG_CODE = C.ORG_CODE
                   WHERE A.RELEASE_TIME >= DATE''' ||L_LAST_DATE || '''
                     AND A.RELEASE_TIME < DATE''' || L_SYSDATE ||'''
                     AND A.DELETED_FLAG = ''0''
                     AND A.REPORT_STATUS = ''2'' --报告单状态0保存，1已批准，2已发布，3已终止，4已销毁，5已冻结，6复核
--                     AND A.REPORT_TYPE IN (''0'',''1'',''2'',''3'') --报告单类型0普通组合报告单（正常），1迟发报告单（迟发），2退单通知书（退单），3修改通知单（终止报告），4初步报告单,5  人工综合报告单  报告单类型
                 ';
      PROGRAM.RPT_PROC_EXE_PRC(V_PROG_NAME, V_SQL);



      --删除跑数日期数据
      V_SQL := '
                DELETE FROM OLTP.RPT_LB_REPORT_STATISTICS_T A
                WHERE A.RELEASE_TIME >= DATE''' ||L_LAST_DATE || '''
                  AND A.RELEASE_TIME < DATE''' || L_SYSDATE ||'''
               ';
      PROGRAM.RPT_PROC_EXE_PRC(V_PROG_NAME, V_SQL);


      --插入跑数日期目标表数据
      V_SQL := 'INSERT INTO OLTP.RPT_LB_REPORT_STATISTICS_T
                  SELECT *
                    FROM (SELECT RELEASE_TIME,
                                 SUB_CODE,
                                 SUB_NAME,
                                 LAB_CODE,
                                 LAB_NAME,
                                 DEPARTMENT_CODE,
                                 DEPARTMENT_NAME,
                                 ORG_CODE,
                                 ORG_NAME,
                                 COUNT(1) AMOUNT,
                                 REPORT_TYPE,
                                 REPORT_CATEGORY
                            FROM OLTP.RPT_LB_REPORT_STATISTICS_T1
                           GROUP BY RELEASE_TIME,
                                    SUB_CODE,
                                    SUB_NAME,
                                    LAB_CODE,
                                    LAB_NAME,
                                    DEPARTMENT_CODE,
                                    DEPARTMENT_NAME,
                                    ORG_CODE,
                                    ORG_NAME,
                                    REPORT_TYPE,
                                    REPORT_CATEGORY)
                  PIVOT(SUM(AMOUNT)
                     FOR REPORT_TYPE IN(''0'' AS REPORT_TYPE_0, ''1'' REPORT_TYPE_1, ''3'' REPORT_TYPE_2, ''2'' REPORT_TYPE_3))
                   ';
      PROGRAM.RPT_PROC_EXE_PRC(V_PROG_NAME, V_SQL);

      --更新运行日志信息
      RPT_OLTP_PUB.UPDATE_OLTP_LOG_PRC(P_RUN_ID    => L_RUN_ID,
                                       P_END_DATE  => SYSDATE,
                                       P_STATUS    => 'S',
                                       P_ERROR_MSG => NULL);
    EXCEPTION
      WHEN OTHERS THEN
        --更新运行日志信息
        L_ERROR_MSG := SUBSTR(SQLERRM, 1, 3000);
        RPT_OLTP_PUB.UPDATE_OLTP_LOG_PRC(P_RUN_ID    => L_RUN_ID,
                                         P_END_DATE  => SYSDATE,
                                         P_STATUS    => 'E',
                                         P_ERROR_MSG => L_ERROR_MSG);

    END;

  EXCEPTION
    WHEN L_CUSTOMIZE_EXP THEN
      DBMS_OUTPUT.PUT_LINE('当天日志重复');

    WHEN OTHERS THEN
      --更新运行日志信息
      L_ERROR_MSG := SUBSTR(SQLERRM, 1, 3000);
      RPT_OLTP_PUB.UPDATE_OLTP_LOG_PRC(P_RUN_ID    => L_RUN_ID,
                                       P_END_DATE  => SYSDATE,
                                       P_STATUS    => 'E',
                                       P_ERROR_MSG => L_ERROR_MSG);
  END RPT_LB_REPORT_COUNT_PRC;
END OLTP_RPT_LB_REPORT_COUNT_PKG;
