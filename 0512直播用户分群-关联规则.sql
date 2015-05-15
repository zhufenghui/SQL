/*ȫ���û���ϸ����������������
���˵��Ӵ�Ƶ����2���µ�����*/
SELECT * from (
SELECT devno ,type2,COUNT(1)touchfreq,SUM(watchtime)watchtime_channel 
  FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
/*  WHERE DEVNO IN (SELECT DISTINCT DEVNO
                   FROM RECOMMENDATION.LIVE_USER_CLASSIFY_SUB_01
                  WHERE RULE1 = '1'
                    AND TYPE1 IN ('���', '��ʡ', '����')
                  --and type2 = '&typename' 
                 )  */
GROUP BY devno , type2
ORDER BY devno, touchfreq DESC
)
WHERE touchfreq>1

/*�����û��������  
select * from recommendation.live_user_classify_sub_01
*/
SELECT devno ,channelname2, SUM(watchtime)watchtime_channel 
  FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
  WHERE DEVNO IN (SELECT DISTINCT DEVNO
                   FROM RECOMMENDATION.LIVE_USER_CLASSIFY_SUB_01
                  WHERE RULE1 = '1'
                    AND TYPE1 IN ('���', '��ʡ', '����')
                  --and type2 = '&typename' 
                 )  
GROUP BY devno , channelname2
ORDER BY devno, watchtime_channel DESC

/*ȫ���û��������� select DISTINCT type2 FROM recommendation.channel_classify_02 */

SELECT * from 
(
SELECT devno,
sum(decode(type2, '�ƾ�', watchtime_type2, 0)) as caijing,
sum(decode(type2, 'Ӱ�Ӿ�', watchtime_type2, 0)) as yingshiju,
sum(decode(type2, '����', watchtime_type2, 0)) as dushu,
sum(decode(type2, '��Ӱ', watchtime_type2, 0)) as dianying,
sum(decode(type2, '����뷨', watchtime_type2, 0)) as shehuiyufa,
sum(decode(type2, '�����Ӽ�', watchtime_type2, 0)) as jiankang,
sum(decode(type2, '��Ϸ', watchtime_type2, 0)) as youxi,
sum(decode(type2, 'Ϸ��', watchtime_type2, 0)) as xiqu,
sum(decode(type2, '����', watchtime_type2, 0)) as zongyi,
sum(decode(type2, '¥��', watchtime_type2, 0)) as loushi,
sum(decode(type2, '����ũҵ', watchtime_type2, 0)) as junshi,
sum(decode(type2, '����', watchtime_type2, 0)) as gouwu ,
sum(decode(type2, '����', watchtime_type2, 0)) as jiaoyu,
sum(decode(type2, '��¼', watchtime_type2, 0)) as jilu,
sum(decode(type2, '����', watchtime_type2, 0)) as yingyue,
sum(decode(type2, '�ƽ�', watchtime_type2, 0)) as kejiao,
sum(decode(type2, '�ۺ�', watchtime_type2, 0)) as zonghe,
sum(decode(type2, '����', watchtime_type2, 0)) as xinwen,
sum(decode(type2, '����', watchtime_type2, 0)) as lvyou,
sum(decode(type2, '����', watchtime_type2, 0)) as dongman,
sum(decode(type2, '����', watchtime_type2, 0)) as tiyu,
sum(decode(type2, '��Ʊ', watchtime_type2, 0)) as gupiao,
sum(decode(type2, 'ʱ��', watchtime_type2, 0)) as shishang
  FROM (SELECT DEVNO, TYPE2, SUM(WATCHTIME) WATCHTIME_TYPE2
          FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
         GROUP BY DEVNO, TYPE2) 
GROUP BY devno 		 
) t1, 
(
SELECT  devno,
sum(decode(interval, '�賿', touchfreq_interval, 0)) as lingcheng,
sum(decode(interval, '�糿', touchfreq_interval, 0)) as zaocheng,
sum(decode(interval, '����', touchfreq_interval, 0)) as shangwu,
sum(decode(interval, '����', touchfreq_interval, 0)) as zhongwu,
sum(decode(interval, '����', touchfreq_interval, 0)) as xiawu,
sum(decode(interval, '����', touchfreq_interval, 0)) as bangwan,
sum(decode(interval, '����', touchfreq_interval, 0)) as wanshang,
sum(decode(interval, '��ҹ', touchfreq_interval, 0)) as shenye 
FROM (
SELECT devno , INTERVAL ,COUNT(1)touchfreq_interval 
  FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
GROUP BY devno , INTERVAL)
GROUP BY devno
)t2
 WHERE t1.devno=t2.devno




