---外呼存在的时候，一个orderid可以对应多个callid，连接cc_call与order 表， 存在很多2W多条cust_no为空，waittime非空的记录
drop table tmp.zhu_secondcall_zs_01
CREATE TABLE tmp.zhu_secondcall_zs_01 AS
SELECT T2.CUST_NO,
        T2.CUST_NAME ,
        '0' IS_SECOND,
        T1.WAIT_TIME,
        (CASE
           WHEN T2.CATEGORY = 'businessWorkflow' THEN
            '受理'
           WHEN T2.CATEGORY = 'adviceWorkflow' THEN
            '咨询'
           WHEN T2.CATEGORY = 'appealWorkflow' THEN
            '投诉'
           WHEN T2.CATEGORY = 'faultWorkflow' THEN
            '故障'
           ELSE
            T2.CATEGORY
         END) maintype,
            t2.stype
  from ODS.TO_CC_CALL_20150324 T1, ODS.TO_Z_WO_ORDER_TOTAL T2
  WHERE T1.CALL_ID = T2.CALL_ID(+)
    and t1.direct='I'
    AND T1.START_TIME >= TO_DATE('20141101', 'yyyymmdd')
    AND T1.START_TIME < TO_DATE('20150301', 'yyyymmdd')
    ORDER BY T2.CUST_NO, T1.WAIT_TIME
    
 ---将主子类型分割，要用表连接，在select 实施效率低
 --- drop TABLE tmp.zhu_secondcall_zs_02 
 CREATE TABLE tmp.zhu_secondcall_zs_02 AS 
 select t.*, s1.OPTION_NAME subtype1, s2.OPTION_NAME subtype2,s3.OPTION_NAME subtype3,s4.OPTION_NAME subtype4      
 from tmp.zhu_secondcall_zs_01 t, 
       ODS.TO_SYS_MULT_OPTION_20150324 s1,
       ODS.TO_SYS_MULT_OPTION_20150324 s2,
       ODS.TO_SYS_MULT_OPTION_20150324 s3,
       ODS.TO_SYS_MULT_OPTION_20150324 s4
 where SPLIT(T.STYPE, ',', 1)=s1.option_id(+)
       and SPLIT(T.STYPE, ',', 2)=s2.option_id(+) 
       and SPLIT(T.STYPE, ',', 3)=s3.option_id(+) 
       and SPLIT(T.STYPE, ',', 4)=s4.option_id(+)
 
---复制表2 更新二次呼入标识、普通二次呼入
--- CREATE TABLE tmp.zhu_secondcall_zs_03_01 AS (SELECT * FROM tmp.zhu_secondcall_zs_02) 
UPDATE tmp.zhu_secondcall_zs_03_01 T
SET IS_SECOND='1' 
WHERE EXISTS (SELECT 1   FROM tmp.zhu_secondcall_zs_03_01 B
                          WHERE B.CUST_NO = T.CUST_NO
                            AND TRUNC(T.WAIT_TIME) - TRUNC(B.WAIT_TIME) < '8'
                            AND T.WAIT_TIME > B.WAIT_TIME )
AND  TRIM(T.CUST_NO) IS NOT NULL    

--复制表2 更新二次呼入标识、二次呼入-同主类型
--- CREATE TABLE tmp.zhu_secondcall_zs_03_02 AS (SELECT * FROM tmp.zhu_secondcall_zs_02) 
UPDATE tmp.zhu_secondcall_zs_03_02 T
SET IS_SECOND='1' 
WHERE EXISTS (SELECT 1   FROM tmp.zhu_secondcall_zs_03_02 B
                          WHERE B.CUST_NO = T.CUST_NO
                            AND TRUNC(T.WAIT_TIME) - TRUNC(B.WAIT_TIME) < '8' AND T.MAINTYPE=B.MAINTYPE
                            AND T.WAIT_TIME > B.WAIT_TIME )
AND  TRIM(T.CUST_NO) IS NOT NULL  

---复制表2 更新二次呼入标识、二次呼入-同主子类型
--- CREATE TABLE tmp.zhu_secondcall_zs_03_03 AS (SELECT * FROM tmp.zhu_secondcall_zs_02) 
UPDATE tmp.zhu_secondcall_zs_03_03 T
SET IS_SECOND='1' 
WHERE EXISTS (SELECT 1   FROM tmp.zhu_secondcall_zs_03_03 B
                          WHERE B.CUST_NO = T.CUST_NO
                            AND TRUNC(T.WAIT_TIME) - TRUNC(B.WAIT_TIME) < '8' AND T.SUBTYPE1=B.SUBTYPE1
                            AND T.WAIT_TIME > B.WAIT_TIME )
AND  TRIM(T.CUST_NO) IS NOT NULL  

---分月查询二次呼入
SELECT TO_CHAR(WAIT_TIME,'YYYYMM')MONTH , COUNT(1) 
FROM TMP.ZHU_SECONDCALL_ZS_03_&NUM WHERE IS_SECOND=1 GROUP BY TO_CHAR(WAIT_TIME,'YYYYMM')  ---AND SUBTYPE1<> '无效电话'
ORDER BY MONTH

--
SELECT TO_CHAR(WAIT_TIME,'YYYYMM')MONTH,COUNT(1) FROM TMP.ZHU_SECONDCALL_ZS_02 GROUP BY TO_CHAR(WAIT_TIME,'YYYYMM')
ORDER BY MONTH 
 
