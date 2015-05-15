/*extract April data 
从直播明细表中抽取4月数据，指定表空间，用户默认TBS_ORI空间不足，并行，使用nologging 加快速度*/
create table recommendation.live_201504 tablespace TBS_ODS
parallel (degree 8) 
nologging  as 
select * from eds.tw_live_detail_zs where stime between date'2015-04-01' and date'2015-05-01' ;

/*  drop table recommendation.channel_classify_02
clssify表示的频道是channellist 的子集，但是带有频道的主、子分类（本省、动漫），
channellist 中有些频道如数字电视导视在classify中没有*/
create table recommendation.channel_classify_02 as 
select a.*, b.channelid channelid2, b.name channelname2
  from recommendation.channel_classify a,  ---type data
       ods.to_tv_channellist_zs b          ---more channel
 where a.channelid(+) = b.channelid  ; 
 
 
--- select  * from recommendation.live_201504_wide where channelname2 ='TVS-1'
-- drop table recommendation.live_201504_wide
/*构建宽表数据，包括将时间段切成天，时段，小时，是否周末，频道类型，观看时长等*/
create table recommendation.live_201504_wide
tablespace TBS_ODS
parallel (degree 8) 
nologging  
as select a.*,
       to_char(a.stime, 'yyyymmdd') day,
       to_char(a.stime, 'HH24') Hour, 
         (case
         when to_char(a.stime, 'HH24') in ('01', '02', '03', '04', '05') then
          '凌晨'
         when to_char(a.stime, 'HH24') in ('06', '07') then
          '早晨'
         when to_char(a.stime, 'HH24') in ('08', '09', '10', '11') then
          '上午'
         when to_char(a.stime, 'HH24') in ('12', '13') then
          '中午'
         when to_char(a.stime, 'HH24') in ('14', '15', '16') then
          '下午'
         when to_char(a.stime, 'HH24') in ('17', '18') then
          '傍晚'
         when to_char(a.stime, 'HH24') in ('19', '20', '21', '22') then
          '晚上'
         when to_char(a.stime, 'HH24') in ('23', '00') then
          '深夜'
         else
          null
       end) interval,
       (case when to_char(stime,'yyyymmdd') in ('20150404','20150405','20150411','20150412','20150419','20150418','20150426','20150425')
             then 'Y' 
             else 'N'
        end  ) is_weekend,
       b.channelname2,
       b.type1,
       b.type2,
       (etime - stime) watchtime,
       (epgetime - epgstime) epgtime, ((etime - stime)/(epgetime - epgstime))valide
  from recommendation.live_201504 a, recommendation.channel_classify_02 b 
 where a.channelid = b.channelid2(+) ;
 
/*-  valide table (1000w+) based on wide 
观看时长/总时长大于10%记录为有效*/
create table recommendation.live_201504_wide_valide
tablespace TBS_ODS
parallel (degree 8) 
nologging
as select * from  recommendation.live_201504_wide where valide > 0.1 ;


/* select * from  recommendation.live_user_classify_sub_01 
   drop table from recommendation.live_user_classify_sub_01 
用户分类表包含用户，频道，类型，观看时长占比等*/
create table recommendation.live_user_classify_sub_01  tablespace TBS_ODS  parallel (degree 8)  nologging
as select t.*, ratio_to_report(watchtime_sub) over(partition by devno) percent_sub, '0'rule1,'0'rule2,'0'rule3,'0'rule_combine 
   from (select devno,type1,type2,
                sum(watchtime)watchtime_sub, count(1) touchfreq_sub, count(distinct day)livedays
          from recommendation.live_201504_wide_valide  
          group by devno, type1,type2
          ) t  ;

---- update table based on rules
update recommendation.live_user_classify_sub_01 
set rule1='1' 
where percent_sub >0.2   ;


------  insert into UI table 
/* SELECT * from recommendation.people_type  
SELECT * from recommendation.channel_classify_02 */

--  insert into cardrole
INSERT INTO recommendation.cardrole(cr_id,cardno,role_name)   
SELECT  DISTINCT devno AS   cr_id, devno AS  carddo, NULL  role_name from recommendation.live_user_classify_sub_01

--  insert into pepopletype_cardrole 
/*alter table recommendation.peopletype_cardrole MODIFY( cr_id varchar2(20)) 
select * from recommendation.peopletype_cardrole 
DROP TABLE recommendation.peopletype_cardrole
INSERT INTO recommendation.peopletype_cardrole(pt_id,cr_id) */

CREATE TABLE recommendation.peopletype_cardrole AS 
SELECT  pt_id, devno AS cr_id from recommendation.live_user_classify_sub_01,recommendation.people_type  
WHERE rule1='1'  AND pt_name(+)=type2 AND type2 IS  NOT NULL
UNION ALL
SELECT 24 pt_id , devno AS cr_id FROM recommendation.live_user_classify_sub_01 WHERE rule1='1' AND type2 IS NULL --其他，数字的电视导数
UNION ALL
SELECT 25 pt_id ,devno AS cr_id  FROM recommendation.live_user_classify_sub_01 GROUP BY devno HAVING SUM(rule1)=0 -- 未归类

/*用户群分析  select * from recommendation.live_user_classify_sub_01 */
select type2, count(distinct devno) cluster_nums from recommendation.live_user_classify_sub_01 
where  rule1='1'  
group by  type2 ;
 
---unclassified user nums
select count(1) unclassify_nums
  from (select devno
         from recommendation.live_user_classify_sub_01
         group by devno
        having sum(rule1) = 0 ) ;
        
-- 频道排行
select channelname2,
       sum(watchtime) watchtime_channel,
       count(1) touchfreq_channel
  from (select *                   ---get the detail of cluster 
          from recommendation.live_201504_wide_valide
         where devno in (select distinct devno  --- do not forget distinct 
                           from recommendation.live_user_classify_sub_01
                          where rule1 = '1'
                           and type1 in ('香港','本省','本地')
                         --   and type2 = '&typename'  
                         )
                         )
 group by channelname2
 order by watchtime_channel desc ;

-- 节目排行
select pname, sum(watchtime) watchtime_pname, count(1) touchfreq_pname
  from (select *
          from recommendation.live_201504_wide_valide
         where devno in (select distinct devno
                           from recommendation.live_user_classify_sub_01
                          where rule1 = '1'
                          and type1 in ('香港','本省','本地')
                         --   and type2 = '&typename'
                         )
                         )
 group by pname
 order by watchtime_pname desc
;

--时段分析
select  is_weekend, interval,count(1)touchfreq
 from (select *
          from recommendation.live_201504_wide_valide
         where devno in (select distinct devno
                           from recommendation.live_user_classify_sub_01
                          where rule1 = '1'
                           and type1 in ('香港','本省','本地')
                          --  and type2 = '&typename'
                          )
                          )
group by is_weekend, interval
order by touchfreq
;
--平均收视时长33.4182993720944
select avg(devno_watchtime) * 24
  from (select devno, sum(watchtime) devno_watchtime
          from (select *
                  from recommendation.live_201504_wide_valide
                 where devno in (select distinct devno
                                   from recommendation.live_user_classify_sub_01
                                  where rule1 = '1'
                                   and type1 in ('香港','本省','本地')
                                 --   and type2 = '&typename'
                                 )
                                 )
         group by devno) t

/*
 select count(distinct devno) from recommendation.live_user_classify_sub_01
*/

----体育用户分析
WITH sp AS (
SELECT * from recommendation.live_201504_wide_valide
         where devno in (select distinct devno
                           from recommendation.live_user_classify_sub_01
                          where rule1 = '1'
                           --and type1 in ('香港','本省','本地')
                            and type2 = '体育'
                          ) )
SELECT sp.*, SUM(watchtime)over(PARTITION BY devno  )watchtime_devno  FROM  sp
ORDER BY  watchtime_devno DESC
 










