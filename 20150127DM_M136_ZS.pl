#!/usr/bin/perl
###############################################################################
# PERL数据库应用程序
#
# FILENAME:  DM_M136_ZS.pl
# PURPOSE:   工单统计月报表TM_WORKFORM_MONTH_ZS)
# AUTHOR:    于斯文
# DESCRIPTION:
#
# PARAMETERS:
#    $vDate           统计日期（YYYYMMDD）
# EXIT STATUS:
#    0                成功
#    其它             失败
# HISTORY:
# DATE       AUTHOR   VERSION    MODIFICATIONS
# ########## ######## ########## #############################################
# 2012-05-11 刘畅     V01.00.000 新建程序
###############################################################################

use strict;
use dss_common;

# 版本号约束
use constant VERSION => "V01.00.000";

# 入参判断
my $vDate = shift || die "usage: $0 <YYYYMMDD>\n";
my $vBranch = shift || "ZS";
my $yyyymmdd = $vDate;
my $vMonth = substr($vDate,0,6);

# 创建数据库连接
my $conn = dss_common->new();

# 日志级别为: EBUG, INFO, WARNING, ERROR, FATAL
$conn->writelog(dss_common::INFO, "VERSION=" . VERSION);
$conn->writelog(dss_common::INFO, "Data_Date=" .$vDate."  Branch=".$vBranch);

# 执行数据库连接, 默认设置为自动提交事务
$conn->connect_dwdb($vBranch, $vDate);

# 获取当前连接唯一标志
my $ssid = $conn->get_sessionid();

############################
# 设置脚本退出清理数据范围 #
############################

END {
  # 非法入参直接退出
  exit 1 if (!defined($conn));

  # 成功执行脚本则提交否则回滚
  $conn->cleanup();

  # 退出写状态日志
  $conn->writelog(INFO, "exit($?)");
}

###################################
# 执行主体sql配置, 必须设置并行度 #
# 默认语句执行选项为autocommit    #
# 跟踪类信息必须用[分号]做结束符!!#
###################################

my $de = dss_common::get_parallel($vBranch,"M");

# 上个月底:yyyymmdd
my $vPreLastDay = dss_common::addMonths($yyyymmdd,-1);
   	  $vPreLastDay = dss_common::getLastDateOfMonth($vPreLastDay);

# 上上个月底:vPre2LastDay
my $vPre2LastDay = dss_common::addMonths($vPreLastDay,-1);
	  $vPre2LastDay = dss_common::getLastDateOfMonth($vPre2LastDay);

# 上个月初:vPreFirstDay
my $vPreFirstDay =  dss_common::addDays($vPre2LastDay,+1);

# 上个月:vPreMonth
my $vPreMonth = substr($vPreFirstDay,0,6);

# 本月5号:vFifthday
my $vFifthday = dss_common::addDays($vPreLastDay,5);

if ( $vFifthday  eq $yyyymmdd ) {
my $sql = <<EOF
  #DEBUG: 设置并行度;
  ALTER SESSION ENABLE PARALLEL DML;
  ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  ALTER SESSION FORCE PARALLEL DML PARALLEL $de;

#DEBUG: 汇总安装单月报表数据;
#IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M136_ZS_1 PURGE;
CREATE TABLE TMP.TMP_DM_M136_ZS_1 NOLOGGING AS
SELECT AREAID,
       GRID_ID,
       ISHITCH,
       SUBKIND_CODE,
       COUNT(1) SENDNUMS,                     	--派发工单数(非执行中工单且无回单及退单操作的工单已经在明细表过滤掉)
       SUM(CASE WHEN ORDER_STATE_NAME IN (1,5) THEN 1
                ELSE 0 END) HANDLENUMS,       --处理工单数
       SUM(CASE WHEN (ORDER_STATE_NAME = 1 AND TO_CHAR(FINISH_DATE,'YYYYMMDD') <> 20991231) THEN 1
                ELSE 0 END) SUCCNUMS,         	--处理成功数
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1,2,3,4,5)) THEN  1
                ELSE 0 END) OUTTIMENUMS,      --超时处理工单数
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1,2,3,4,5)) THEN 1
                ELSE 0 END) OUTNOHANDLES,    --超时未处理工单数
       SUM(CASE WHEN (SYSDATE < LIMIT_DATE AND ORDER_STATE_NAME NOT IN (1,2,3,4)) THEN 1
                ELSE 0 END) NOHANDLES,        	 --时限内未处理数
       SUM(CASE WHEN (ORDER_STATE_NAME IN (1,5) AND TO_CHAR(FINISH_DATE,'YYYYMMDD') <> 20991231)
                THEN ( CASE
                 WHEN FINISH_DATE >
                      TO_DATE('$yyyymmdd'||'000000', 'YYYY/MM/DD HH24:MI:SS') THEN
                  NVL(ROUND(TO_NUMBER(TO_DATE('$yyyymmdd'||'000000',
                                              'YYYY/MM/DD HH24:MI:SS') -
                                      ACCEPT_DATE) * 24 * 60),
                      0)
                 ELSE
                  NVL(ROUND(TO_NUMBER(FINISH_DATE - ACCEPT_DATE) * 24 * 60),
                      0)
               END) ELSE 0 END) TOTAL_ONTIME,     --工单处理总时长(分钟)不包括待回访工单
       SUM(CASE WHEN (ORDER_STATE_NAME IN (1, 2, 3) AND OUTTIME_FLAG = 1)
                THEN LIMIT_TIME ELSE 0 END) TOTAL_OUTTIME,        --工单超时时长(分钟)不包括待回访工单
       SUM(CASE WHEN (OUTTIME_FLAG <= 0 AND  ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENZI,     --及时率分子
       SUM(CASE WHEN (ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENMU	--及时率分母
  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd
  WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
  AND   TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
  AND	ISHITCH = 0
  AND ORDER_STATE_NAME NOT IN (2, 3)
  GROUP BY  AREAID,
            GRID_ID,
            ISHITCH,
            SUBKIND_CODE;
  COMMIT;
  
  #DEBUG:删除上月超时处理安装单明细;
  DELETE DM.TM_OSS_OVERTIMEDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 0;
  COMMIT;
  
  #DEBUG:插入上月超时处理安装单明细;
	INSERT INTO DM.TM_OSS_OVERTIMEDO_DETAIL 
	SELECT '$vPreMonth' STAMONTH,
	       ID,
	       AREAID,
	       CUSTID,
	       ORDER_STATE_NAME,
	       SUBKIND_CODE,
         TRACK_ORG,
	       ISHITCH,
	       ACCEPT_DATE,
	       FINISH_DATE,
	       LIMIT_DATE,
	       (SELECT T.ARCH_DATE FROM ODS.TO_OSS_ORDER_ZS T WHERE T.ID = S.ID) ARCH_DATE
	  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd S
	 WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
	   AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
	   AND ISHITCH = 0
	   AND (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1, 4, 5));
	COMMIT;
	
	#DEBUG:删除上月超时未处理安装单明细;
  DELETE DM.TM_OSS_OVERTIMEUNDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 0;
  COMMIT;
  
  #DEBUG:插入上月超时未处理安装单明细;
	INSERT INTO DM.TM_OSS_OVERTIMEUNDO_DETAIL 
	SELECT '$vPreMonth' STAMONTH,
	       ID,
	       AREAID,
	       CUSTID,
	       ORDER_STATE_NAME,
	       SUBKIND_CODE,
         TRACK_ORG,
	       ISHITCH,
	       ACCEPT_DATE,
	       FINISH_DATE,
	       LIMIT_DATE,
	       (SELECT T.ARCH_DATE FROM ODS.TO_OSS_ORDER_ZS T WHERE T.ID = S.ID) ARCH_DATE
	  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd S
	 WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
	   AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
	   AND ISHITCH = 0
	   AND (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1, 4, 5));
	COMMIT;

  #DEBUG: 汇总故障单月报表数据,剔除责任归属为网络运维组和其他的数据;
  INSERT INTO TMP.TMP_DM_M136_ZS_1 
  SELECT AREAID,
       GRID_ID,
       ISHITCH,
       SUBKIND_CODE,
       COUNT(1) SENDNUMS,                     	--派发工单数(非执行中工单且无回单及退单操作的工单已经在明细表过滤掉)
       SUM(CASE WHEN ORDER_STATE_NAME IN (1,6) THEN 1
                ELSE 0 END) HANDLENUMS,       --处理工单数
       SUM(CASE WHEN (ORDER_STATE_NAME = 1 AND TO_CHAR(FINISH_DATE,'YYYYMMDD') <> 20991231) THEN 1
                ELSE 0 END) SUCCNUMS,         	--处理成功数
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1,2,3,4,6)) THEN  1
                ELSE 0 END) OUTTIMENUMS,      --超时处理工单数
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1,2,3,4,6)) THEN 1
                ELSE 0 END) OUTNOHANDLES,    --超时未处理工单数
       SUM(CASE WHEN (SYSDATE < LIMIT_DATE AND ORDER_STATE_NAME NOT IN (1,2,3,4)) THEN 1
                ELSE 0 END) NOHANDLES,        	 --时限内未处理数
       SUM(CASE WHEN (ORDER_STATE_NAME IN (1,6) AND TO_CHAR(FINISH_DATE,'YYYYMMDD') <> 20991231)
                THEN (CASE
                 WHEN FINISH_DATE >
                      TO_DATE('$yyyymmdd'||'000000', 'YYYY/MM/DD HH24:MI:SS') THEN
                  NVL(ROUND(TO_NUMBER(TO_DATE('$yyyymmdd'||'000000',
                                              'YYYY/MM/DD HH24:MI:SS') -
                                      ACCEPT_DATE) * 24 * 60),
                      0)
                 ELSE
                  NVL(ROUND(TO_NUMBER(FINISH_DATE - ACCEPT_DATE) * 24 * 60),
                      0)
               END)  ELSE 0 END) TOTAL_ONTIME,     --工单处理总时长(分钟)不包括待回访工单
       SUM(CASE WHEN (ORDER_STATE_NAME IN (1, 2, 3) AND OUTTIME_FLAG = 1)
                THEN LIMIT_TIME ELSE 0 END) TOTAL_OUTTIME,    	--工单超时时长(分钟)不包括待回访工单
       SUM(CASE WHEN (OUTTIME_FLAG <= 0 AND  ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENZI,     --及时率分子
       SUM(CASE WHEN (ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENMU	--及时率分母
  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd
  WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
  AND   TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
  AND	ISHITCH = 1
  AND   LIABILITY NOT IN ( 1 ,4 )					--剔除责任归属为网络运维组和其他的数据
  AND 	ORDER_STATE_NAME NOT IN (2, 3)
  GROUP BY  AREAID,
           	GRID_ID,
            ISHITCH,
            SUBKIND_CODE;
  COMMIT;
  
  #DEBUG:删除上月超时处理故障单明细;
  DELETE DM.TM_OSS_OVERTIMEDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 1;
  COMMIT;
  
  #DEBUG:插入上月超时处理故障单明细;
	INSERT INTO DM.TM_OSS_OVERTIMEDO_DETAIL 
	SELECT '$vPreMonth' STAMONTH,
	       ID,
	       AREAID,
	       CUSTID,
	       ORDER_STATE_NAME,
	       SUBKIND_CODE,
         TRACK_ORG,
	       ISHITCH,
	       ACCEPT_DATE,
	       FINISH_DATE,
	       LIMIT_DATE,
	       (SELECT T.ARCH_DATE FROM ODS.TO_OSS_FAULT_ORDER_ZS T WHERE T.ID = S.ID) ARCH_DATE
	  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd S
	 WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
	   AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
	   AND ISHITCH = 1
	   AND  LIABILITY NOT IN ( 1 ,4 )					--剔除责任归属为网络运维组和其他的数据
	   AND (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1, 4, 6));
	COMMIT;
	
  #DEBUG:删除上月超时未处理故障单明细;
  DELETE DM.TM_OSS_OVERTIMEUNDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 1;
  COMMIT;
  
  #DEBUG:插入上月超时未处理故障单明细;
	INSERT INTO DM.TM_OSS_OVERTIMEUNDO_DETAIL 
	SELECT '$vPreMonth' STAMONTH,
	       ID,
	       AREAID,
	       CUSTID,
	       ORDER_STATE_NAME,
	       SUBKIND_CODE,
         TRACK_ORG,
	       ISHITCH,
	       ACCEPT_DATE,
	       FINISH_DATE,
	       LIMIT_DATE,
	       (SELECT T.ARCH_DATE FROM ODS.TO_OSS_FAULT_ORDER_ZS T WHERE T.ID = S.ID) ARCH_DATE
	  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd S
	 WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
	   AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
	   AND ISHITCH = 1
	   AND  LIABILITY NOT IN ( 1 ,4 )					--剔除责任归属为网络运维组和其他的数据
	   AND (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1,2,3,4,6));
	COMMIT;

  #DEBUG: 删除旧数据;
  DELETE  FROM DM.TM_WORKFORM_MONTH_ZS WHERE STAMONTH = '$vPreMonth';
  COMMIT;
  
  #DEBUG: 插入新数据;
  INSERT INTO DM.TM_WORKFORM_MONTH_ZS
  SELECT '$vPreMonth' STAMONTH,
          AREAID,
         GRID_ID,
         SUBKIND_CODE,
         SENDNUMS,
         HANDLENUMS,
         SUCCNUMS,
         OUTTIMENUMS,
         OUTNOHANDLES,
         NOHANDLES,
         ROUND((TOTAL_ONTIME / DECODE(HANDLENUMS ,0,1,HANDLENUMS))/60,2) AVG_TIME,
         ROUND((TOTAL_OUTTIME / DECODE(OUTTIMENUMS,0,1,OUTTIMENUMS))/60,2) OUT_AVG_TIME,
         ISHITCH,
         RATE_FENZI,
         RATE_FENMU,
         TOTAL_ONTIME,
         TOTAL_OUTTIME
    FROM TMP.TMP_DM_M136_ZS_1
  COMMIT;

EOF
;

# 提交批量sql的执行;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);
}
exit 0;

