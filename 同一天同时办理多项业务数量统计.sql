--此部分为某个客户某天同时办理了（缴费，订购）或（缴费、订购、更换设备）的总数
SELECT count(1) FROM 
(
 --此部分查询结果为：某个客户某天同时办理了（缴费，订购）或（缴费、订购、更换设备）的明细，
 SELECT c.custid
       ,c.opdate
       ,str_link((SELECT mname FROM ods.to_prv_sysparam_zs where gcode='SYS_OPCODE' and mcode=c.opcode)) 
   FROM (
       select distinct  a.opcode
                       ,a.custid
                       ,to_char(a.optime,'yyyymmdd') opdate
                       ,(SELECT case when mname like '%缴费%' then 0
                                 else case when mname like '%订购%' then 1 
                                 --取消下边一行注释即为统计“办理了缴费、订购、更换设备”
                                 --else case when mname like '%设备更换%' then 2 end
                                 end end 
                            FROM  ods.to_prv_sysparam_zs 
                           where gcode='SYS_OPCODE' 
                             and mname not like '%回退%'
                             and ((mname like '%缴费%' and mname <> '用户缴费方式变更')
                                 or mname like '%订购%'
                                 --取消下边一行注释即为统计“办理了缴费、订购、更换设备”
                                 --or mname like '%设备更换%'
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
     group by c.custid,c.opdate having count(distinct c.myoptype)>1--统计“办理了缴费、订购、更换设备”时把此处修改为2
)