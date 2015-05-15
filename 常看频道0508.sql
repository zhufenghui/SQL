/*target: 用户（devno）最近七天常看频道分析
指标：时间段内观看用户频道的总时长，观看频道的天数
算法：watchtime_channel*(watchchnnel_days/interval_days)
输出：按照devno-channelname格式输出用户的TOP10数据
注：  要要求输出10个频道，所以考虑不用有效定义来刷选数据了，分析的时候用
      dropifexists为存储过程，如果存在表则删除
*/


/*从收视数据中提取前七天数据，ods数据更新为系统日期前一天*/
call dropifexists('oftenwatch_01');
create table recommendation.oftenwatch_01  tablespace TBS_EDS  parallel (degree 8)  nologging
as select * from eds.tw_live_detail_zs a
where a.stime between (sysdate-8) and sysdate ;

/*刷选出有效记录：观看时长/节目时长 大于10%
统计用户的观看频道总时长，接触频次，观看频道天数*/
call dropifexists('oftenwatch_02');  
create table recommendation.oftenwatch_02  tablespace TBS_EDS  parallel (degree 8)  nologging
as 
select distinct devno,channelid,
       sum((etime-stime)) watchtime_channel,
       count(distinct to_char(stime,'yyyymmdd'))watchchannel_days
  from recommendation.oftenwatch_01 
group by devno,channelid ;


select * from  recommendation.oftenwatch_04 
call dropifexists('oftenwatch_03');
create table recommendation.oftenwatch_03  tablespace TBS_EDS  parallel (degree 8)  nologging
as 
select t1.devno,
       t2.name,
       row_number()over(partition by t1.devno order by t1.devno,(watchtime_channel*watchchannel_days)desc) rank
  from recommendation.oftenwatch_02 t1, ods.to_tv_channellist_zs t2
 where t1.channelid = t2.channelid(+) ;



call dropifexists('oftenwatch_04'); 
create table recommendation.oftenwatch_04  tablespace TBS_EDS  parallel (degree 8)  nologging
as select *
from recommendation.oftenwatch_03
where rank<=10 ;
 
