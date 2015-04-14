with t as
(
 select g.*, a.name,a.houseid osshouseid , b.*
 from ods.to_oss_org_grid_zs g ,ods.to_oss_address_tbl_zs a, ODS.TO_RES_HOUSE_ZS B  ---????? A?houseid?'-8400646753'?????????????B?????????houseid
 where g.grid_id=a.gridcode
  and B.HOUSENO = A.ID
   and a.name like '%ÖÐÉ½ÊÐ%' and g.grid in ('SQNJ001')
)
--select count(1) from t

select permark, count(1)--s.*, t.name ,t.houseid  13904,
 from eds.tw_serv_state_zs_20150201 s, t
where s.houseid=t.houseid 
group by permark 
