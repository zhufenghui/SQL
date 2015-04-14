 
select  c.custid,c.name,c.linkaddr,c.phone,c.mobile,area_name(c.areaid)
  from ods.to_sys_cust_zs c
 where custid in (select custid
                    from eds.tw_serv_state_zs_20150323 s
                   where s.openday >= '20150101' 
                   group by s.custid
                  having count(distinct s.permark) = 2) ---时间限制
  order by custid
 
