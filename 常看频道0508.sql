/*target: �û���devno��������쳣��Ƶ������
ָ�꣺ʱ����ڹۿ��û�Ƶ������ʱ�����ۿ�Ƶ��������
�㷨��watchtime_channel*(watchchnnel_days/interval_days)
���������devno-channelname��ʽ����û���TOP10����
ע��  ҪҪ�����10��Ƶ�������Կ��ǲ�����Ч������ˢѡ�����ˣ�������ʱ����
      dropifexistsΪ�洢���̣�������ڱ���ɾ��
*/


/*��������������ȡǰ�������ݣ�ods���ݸ���Ϊϵͳ����ǰһ��*/
call dropifexists('oftenwatch_01');
create table recommendation.oftenwatch_01  tablespace TBS_EDS  parallel (degree 8)  nologging
as select * from eds.tw_live_detail_zs a
where a.stime between (sysdate-8) and sysdate ;

/*ˢѡ����Ч��¼���ۿ�ʱ��/��Ŀʱ�� ����10%
ͳ���û��Ĺۿ�Ƶ����ʱ�����Ӵ�Ƶ�Σ��ۿ�Ƶ������*/
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
 
