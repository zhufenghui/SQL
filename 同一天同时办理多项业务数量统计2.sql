 SELECT opcodelist opcode列表
       ,opnamelis 操作列表
       ,count(1)  num
   FROM (
		   SELECT custid
				 ,opdate
				 ,str_link(opcode) opcodelist
				 ,str_link(mname) opnamelis
			from (
			   select distinct  a.custid
							   ,to_char(a.optime,'yyyymmdd') opdate
							   ,a.opcode
							   ,b.mname
				  from ods.to_biz_log_zs a,(SELECT * FROM  ods.to_prv_sysparam_zs where gcode='SYS_OPCODE' and mname not like '%回退%') b
				 where a.opcode = b.mcode
				   and optime between date '2014-10-01' and date  '2014-11-01'
				   and a.operator <> 0
				   and a.isback in ('N','B')
				   order by opcode
				   )
			 group by custid,opdate having count(distinct opcode)>1
     )
     group by opcodelist,opnamelis  
     order by num desc