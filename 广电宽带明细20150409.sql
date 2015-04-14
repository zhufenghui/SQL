--drop table tmp.zhu_kuandai_0409
create table tmp.zhu_kuandai_0413 as 
select s.custid,
       c.name,
       s.servid,
       s.openday ,
       c.linkaddr,
       c.mobile, 
      serv_status(s.servstatus)״̬, 
       serv_feekind(s.feekind)�շ�����, 
       prod_name(p.pid)��Ʒ����,
       t.����ɷ�ʱ��,
       '��' �ͻ��Ƿ�ͨ���ƶ����
  from eds.TW_SERV_STATE_ZS_20150412 s,
       ods.to_biz_product_zs_20150412 p,
       ods.to_sys_cust_zs c,
       (select custid, max(optime) ����ɷ�ʱ��
          from ods.to_bil_payed_zs
         group by custid) t
 where s.custid = c.custid(+)
   and s.servid=p.servid
   and s.custid = t.custid(+)
   and s.permark = 2
   and submark not in ('UW')
   and SERVSTATUS not in ('5', '6')
 
update  tmp.zhu_kuandai_0413 tmp
set �ͻ��Ƿ�ͨ���ƶ����='��'
where tmp.custid in (select custid from eds.TW_SERV_STATE_ZS_20150412 where permark=2 group by custid having count(distinct submark)>1  )

select * from tmp.zhu_kuandai_0413  where ��Ʒ���� not like '%U��Ƶ%' --��Ѷ���

---�����񲿳�һ�¹������ϸ���ͻ���š��û���š������� ��ַ����ͨ���ڣ��ɷѽ�ֹ���ڣ�Ƿ�����ж�һ����Щ�ͻ��Ƿ��п�ͨ�ƶ������
