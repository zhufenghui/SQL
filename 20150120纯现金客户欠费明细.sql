 
 
select distinct c.custid,a.servid,c.linkaddr,c.mobile , a.billmon �˵��·�, a.fees ���, patch_name(s.patchid), serv_status(s.servstatus) 
  from ods.TO_ARREAR_TOTAL_ZS a,
       tmp.zhu_south_cashcust t,
        ods.to_sys_cust_zs     c,
        eds.tw_serv_state_zs_20150118 s
 where a.billmon = '201412'
   and a.fees != 0
   and a.servid = t.servid
   and a.custid = c.custid
   and a.servid =  s.servid
   order by custid
 
 --- servid ��Ӧ payway=1 or 0 
create table   tmp.zhu_south_cashcust as ( 
select *
  from ods.to_sys_servst_zs s 
 where s.custid in (select custid
                      from (select custid, payway, count(1) freq
                              from ods.to_sys_servst_zs
                             where areaid = 719
                             group by custid, payway      ---��������
                             order by custid)
                     group by custid
                    having count(1) = 1)   --- ˢѡ��һ��
and payway=0  ---ˢѡΪ�ֽ�
 )

