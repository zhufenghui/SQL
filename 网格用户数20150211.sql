drop table  tmp.zhu_gridinfo  
create table  tmp.zhu_gridinfo as 
(
 select g.grid, a.name,a.houseid osshouseid , a.id, a.gridcode , b.houseid, b.houseno 
 from ods.to_oss_org_grid_zs g ,ods.to_oss_address_tbl_zs a, ODS.TO_RES_HOUSE_ZS B -- connect a and b to get houseid , houseid in a is like '-80000' ,we want the abs like 8000
 where g.grid_id=a.gridcode
  and B.HOUSENO = A.ID 
  AND G.AREAID=715
  and g.grid like '%SQ%'
)
--select count(1) from t

select T.GRID, p.mname,count(1)在用用户
 from eds.tw_serv_state_zs_20150201 s, tmp.zhu_gridinfo t , (select mcode,MNAME from ods.to_prv_sysparam_zs where gcode='SYS_PERMARK')p 
where s.houseid=t.houseid and p.mcode=s.permark
AND S.SERVSTATUS=2 
group by T.GRID, p.mname 


