/*全体用户明细，用来做关联规则
过滤掉接触频次在2以下的类型*/
SELECT * from (
SELECT devno ,type2,COUNT(1)touchfreq,SUM(watchtime)watchtime_channel 
  FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
/*  WHERE DEVNO IN (SELECT DISTINCT DEVNO
                   FROM RECOMMENDATION.LIVE_USER_CLASSIFY_SUB_01
                  WHERE RULE1 = '1'
                    AND TYPE1 IN ('香港', '本省', '本地')
                  --and type2 = '&typename' 
                 )  */
GROUP BY devno , type2
ORDER BY devno, touchfreq DESC
)
WHERE touchfreq>1

/*粤语用户聚类分析  
select * from recommendation.live_user_classify_sub_01
*/
SELECT devno ,channelname2, SUM(watchtime)watchtime_channel 
  FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
  WHERE DEVNO IN (SELECT DISTINCT DEVNO
                   FROM RECOMMENDATION.LIVE_USER_CLASSIFY_SUB_01
                  WHERE RULE1 = '1'
                    AND TYPE1 IN ('香港', '本省', '本地')
                  --and type2 = '&typename' 
                 )  
GROUP BY devno , channelname2
ORDER BY devno, watchtime_channel DESC

/*全体用户聚类数据 select DISTINCT type2 FROM recommendation.channel_classify_02 */

SELECT * from 
(
SELECT devno,
sum(decode(type2, '财经', watchtime_type2, 0)) as caijing,
sum(decode(type2, '影视剧', watchtime_type2, 0)) as yingshiju,
sum(decode(type2, '读书', watchtime_type2, 0)) as dushu,
sum(decode(type2, '电影', watchtime_type2, 0)) as dianying,
sum(decode(type2, '社会与法', watchtime_type2, 0)) as shehuiyufa,
sum(decode(type2, '健康居家', watchtime_type2, 0)) as jiankang,
sum(decode(type2, '游戏', watchtime_type2, 0)) as youxi,
sum(decode(type2, '戏曲', watchtime_type2, 0)) as xiqu,
sum(decode(type2, '综艺', watchtime_type2, 0)) as zongyi,
sum(decode(type2, '楼市', watchtime_type2, 0)) as loushi,
sum(decode(type2, '军事农业', watchtime_type2, 0)) as junshi,
sum(decode(type2, '购物', watchtime_type2, 0)) as gouwu ,
sum(decode(type2, '教育', watchtime_type2, 0)) as jiaoyu,
sum(decode(type2, '纪录', watchtime_type2, 0)) as jilu,
sum(decode(type2, '音乐', watchtime_type2, 0)) as yingyue,
sum(decode(type2, '科教', watchtime_type2, 0)) as kejiao,
sum(decode(type2, '综合', watchtime_type2, 0)) as zonghe,
sum(decode(type2, '新闻', watchtime_type2, 0)) as xinwen,
sum(decode(type2, '旅游', watchtime_type2, 0)) as lvyou,
sum(decode(type2, '动漫', watchtime_type2, 0)) as dongman,
sum(decode(type2, '体育', watchtime_type2, 0)) as tiyu,
sum(decode(type2, '股票', watchtime_type2, 0)) as gupiao,
sum(decode(type2, '时尚', watchtime_type2, 0)) as shishang
  FROM (SELECT DEVNO, TYPE2, SUM(WATCHTIME) WATCHTIME_TYPE2
          FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
         GROUP BY DEVNO, TYPE2) 
GROUP BY devno 		 
) t1, 
(
SELECT  devno,
sum(decode(interval, '凌晨', touchfreq_interval, 0)) as lingcheng,
sum(decode(interval, '早晨', touchfreq_interval, 0)) as zaocheng,
sum(decode(interval, '上午', touchfreq_interval, 0)) as shangwu,
sum(decode(interval, '中午', touchfreq_interval, 0)) as zhongwu,
sum(decode(interval, '下午', touchfreq_interval, 0)) as xiawu,
sum(decode(interval, '傍晚', touchfreq_interval, 0)) as bangwan,
sum(decode(interval, '晚上', touchfreq_interval, 0)) as wanshang,
sum(decode(interval, '深夜', touchfreq_interval, 0)) as shenye 
FROM (
SELECT devno , INTERVAL ,COUNT(1)touchfreq_interval 
  FROM RECOMMENDATION.LIVE_201504_WIDE_VALIDE
GROUP BY devno , INTERVAL)
GROUP BY devno
)t2
 WHERE t1.devno=t2.devno




