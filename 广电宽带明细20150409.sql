--drop table tmp.zhu_kuandai_0409
create table tmp.zhu_kuandai_0413 as 
select s.custid,
       c.name,
       s.servid,
       s.openday ,
       c.linkaddr,
       c.mobile, 
      serv_status(s.servstatus)状态, 
       serv_feekind(s.feekind)收费类型, 
       prod_name(p.pid)产品名称,
       t.最近缴费时间,
       '否' 客户是否开通过移动宽带
  from eds.TW_SERV_STATE_ZS_20150412 s,
       ods.to_biz_product_zs_20150412 p,
       ods.to_sys_cust_zs c,
       (select custid, max(optime) 最近缴费时间
          from ods.to_bil_payed_zs
         group by custid) t
 where s.custid = c.custid(+)
   and s.servid=p.servid
   and s.custid = t.custid(+)
   and s.permark = 2
   and submark not in ('UW')
   and SERVSTATUS not in ('5', '6')
 
update  tmp.zhu_kuandai_0413 tmp
set 客户是否开通过移动宽带='是'
where tmp.custid in (select custid from eds.TW_SERV_STATE_ZS_20150412 where permark=2 group by custid having count(distinct submark)>1  )

select * from tmp.zhu_kuandai_0413  where 产品名称 not like '%U宽频%' --视讯宽带

---给财务部出一下广电宽带明细：客户编号、用户编号、姓名、 地址、开通日期，缴费截止日期，欠费再判断一下这些客户是否有开通移动宽带？
