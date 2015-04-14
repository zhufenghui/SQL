--�ظ����Ͽ۷ֶ��壺���¹鵵�Ĺ��Ϲ����У��ڳɹ�������һ����ͬһ�û������ظ����ϵĹ������������κα���ԭ��
 drop table tmp.zhu_secondfalut_01
 create table tmp.zhu_secondfault_01 as 
 SELECT t1.*, '��' is_second
    FROM EDS.Tw_Oss_Workform_Zs_20150405 T1
   WHERE T1.ORDER_STATE_NAME = 1
   AND TO_CHAR(T1.FINISH_DATE, 'YYYYMMDD') <> '20991231'
   AND T1.ISHITCH = 1
   AND T1.LIABILITY NOT IN (1, 4) --�޳����ι���Ϊ������ά�������������
   and t1.accept_date>=date'2015-01-01'
   order by t1.accept_date desc
 
 update tmp.zhu_secondfault_01 a
 set is_second='��'
 where exists (SELECT 1   FROM tmp.zhu_secondfault_01 b 
                          WHERE b.custid = a.custid
                            AND TRUNC(a.accept_date) - TRUNC(b.accept_date) < '8'
                            AND a.accept_date > b.accept_date )
                            
 select tt.ordercode,area_name(tt.areaid),tt.custid,c.name,c.linkaddr, tt.accept_date,tt.finish_date,tt.is_second,t2.order_state_name,t2.fault_kind_name,t3.recover_reason,t3.path_name
   from tmp.zhu_secondfault_01     tt,  
        ods.to_oss_fault_order_zs  t2, --�ɵ�����
        ods.to_oss_fault_reason_zs t3,  ---����ԭ��
        ods.to_sys_cust_zs c
  where tt.ordercode = t2.order_code(+)  and tt.custid=c.custid
    and tt.recover_reason_id = t3.recover_reason_id(+)
    and tt.accept_date between date'2015-03-01' and date '2015-04-01'
  order by tt.custid 
 
 
 
 
