--�˲���Ϊĳ���ͻ�ĳ��ͬʱ�����ˣ��ɷѣ��������򣨽ɷѡ������������豸��������
SELECT count(1) FROM 
(
 --�˲��ֲ�ѯ���Ϊ��ĳ���ͻ�ĳ��ͬʱ�����ˣ��ɷѣ��������򣨽ɷѡ������������豸������ϸ��
 SELECT c.custid
       ,c.opdate
       ,str_link((SELECT mname FROM ods.to_prv_sysparam_zs where gcode='SYS_OPCODE' and mcode=c.opcode)) 
   FROM (
       select distinct  a.opcode
                       ,a.custid
                       ,to_char(a.optime,'yyyymmdd') opdate
                       ,(SELECT case when mname like '%�ɷ�%' then 0
                                 else case when mname like '%����%' then 1 
                                 --ȡ���±�һ��ע�ͼ�Ϊͳ�ơ������˽ɷѡ������������豸��
                                 --else case when mname like '%�豸����%' then 2 end
                                 end end 
                            FROM  ods.to_prv_sysparam_zs 
                           where gcode='SYS_OPCODE' 
                             and mname not like '%����%'
                             and ((mname like '%�ɷ�%' and mname <> '�û��ɷѷ�ʽ���')
                                 or mname like '%����%'
                                 --ȡ���±�һ��ע�ͼ�Ϊͳ�ơ������˽ɷѡ������������豸��
                                 --or mname like '%�豸����%'
                                 )
                             and mcode=a.opcode    
                         ) myoptype
          from ods.to_biz_log_zs a
         where optime between date '2014-10-01' and date  '2014-11-01'
           and a.operator <> 0
           and a.isback in ('N','B')
           order by myoptype
     ) c
     where myoptype is not null
     group by c.custid,c.opdate having count(distinct c.myoptype)>1--ͳ�ơ������˽ɷѡ������������豸��ʱ�Ѵ˴��޸�Ϊ2
)