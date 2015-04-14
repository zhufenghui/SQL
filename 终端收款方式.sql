 ---   用NODS_ZS 库， bidb2数据库出现严重超时，跑不出来
 with tt as (SELECT b.payway  , sum(b.payfees) FEES
  FROM  biz_fee a,  biz_fee_payway b  
 WHERE a.bizfeedetid = b.bizfeedetid
   AND a.optime < to_date('20150201 00:00:00', 'yyyymmdd HH24:mi:ss')
   AND a.optime >= to_date('20150101 00:00:00', 'yyyymmdd HH24:mi:ss')
   and a.city='ZS'
 GROUP BY b.payway
 )

 select t.mname, tt.fees  from  (select * from  prv_sysparam  where gcode='SYS_PAYWAY')t left join  tt  on
 t.mcode= tt.payway 
 order by tt.fees desc 