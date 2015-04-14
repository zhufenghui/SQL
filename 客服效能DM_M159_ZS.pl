#!/usr/bin/perl
###############################################################################
# PERL数据库应用程序
#
# PURPOSE:   KPI图谱-客服效能统计(TM_REPEATCALL_STA_ZS)
#
# AUTHOR:    杨俊华
# DESCRIPTION:
#
# PARAMETERS:
#    $vDate           统计日期（YYYYMMDD）
#    $vBranch         业务区
# EXIT STATUS:
#    0                成功
#    其它             失败
###############################################################################

use strict;
use dss_common;

# 版本号约束
use constant VERSION => "V01.00.000";

# 入参判断
my $vDate = shift || die "usage: $0 <yyyymmdd>\n";
my $vBranch = shift || "ZS";
my $vMonth = substr($vDate,0,6);
my $yyyymmdd = $vDate;

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

my $predate = $conn->execute_select("SELECT to_char(to_date('$yyyymmdd','yyyymmdd')-1,'yyyymmdd') FROM dual");
if ($predate eq "") {
  print "无法获取系统时间。\n";
  exit 1;
}

my $nowDay = dss_common::addDays($yyyymmdd,1);
# 本月月底:yyyymmdd
my $vMonthLastDay = dss_common::getLastDateOfMonth($yyyymmdd);

#7天数据
my $nowDay = dss_common::addDays($yyyymmdd,1);
# 上一月底:yyyymmdd
my $vPreLastDay = dss_common::addMonths($yyyymmdd,-1);
   $vPreLastDay = dss_common::getLastDateOfMonth($vPreLastDay);
my $preMonth = substr($vPreLastDay,0,6);

if ($vMonthLastDay eq $yyyymmdd){  
my $sql = <<EOF
	#DEBUG: 设置并行度;
	ALTER SESSION ENABLE PARALLEL DML;
	ALTER SESSION FORCE PARALLEL query PARALLEL $de;
	ALTER SESSION FORCE PARALLEL DML PARALLEL $de;


  #DEBUG: 客户地址信息;
  #IGNORE_ERROR:drop table tmp.tmp_m159_custaddr_zs purge;
  CREATE TABLE tmp.tmp_m159_custaddr_zs NOLOGGING AS
  SELECT to_char(CUSTID) custid, AREAID, PATCHID, HOUSEID,GRIDID
  FROM (SELECT CUSTID,
               AREAID,
               PATCHID,
               HOUSEID,
               NVL((SELECT GRIDCODE
                     FROM ODS.TO_ADDRESS_COMMUNITY_ZS
                    WHERE TO_CHAR(HOUSEID) = TO_CHAR(serv.HOUSEID)),
                   -9) GRIDID,
               RANK() OVER(PARTITION BY CUSTID ORDER BY SERVID ASC) ORDNO
          FROM EDS.TW_SERV_STATE_ZS_$yyyymmdd serv)
  WHERE ORDNO = 1;
  
  ALTER TABLE tmp.tmp_m159_custaddr_zs ADD
  CONSTRAINTS pk_m159_custaddr_zs PRIMARY KEY(custid);
   
  #DEBUG:客服信息关联工单获取客户信息;
  #IGNORE_ERROR:drop table tmp.tmp_m159_callorder_zs purge;
  CREATE TABLE tmp.tmp_m159_callorder_zs NOLOGGING AS
  SELECT t.*,a.cust_no,a.con_addr
    FROM ODS.TO_CC_CALL_$yyyymmdd t, ODS.TO_Z_WO_ORDER_TOTAL a
   WHERE t.DIRECT='I'  ---排除外呼 
    AND to_char(t.WAIT_TIME,'yyyymm') IN('$vMonth','$preMonth')
	AND t.ORDER_ID = a.ORDER_ID(+);
  

  #DEBUG:获取地址信息;
  #IGNORE_ERROR:drop table tmp.tmp_m159_callinfo_zs purge;
  CREATE TABLE tmp.tmp_m159_callinfo_zs NOLOGGING AS
  SELECT c.*,
         nvl(addr.areacode,caddr.patchid)  patchid,
         nvl(addr.areaid,caddr.areaid ) areaid,
         nvl(addr.gridcode,caddr.gridid) gridid,
		 'N' isrepeat
  FROM tmp.tmp_m159_callorder_zs c
      ,ods.to_oss_address_tbl_zs addr
      ,tmp.tmp_m159_custaddr_zs caddr
  WHERE c.con_addr = addr.id(+)
    AND c.cust_no = caddr.custid(+);
 
  UPDATE tmp.tmp_m159_callinfo_zs t
	 SET isrepeat = 'Y'
   WHERE EXISTS(SELECT 1 FROM tmp.tmp_m159_callinfo_zs B
				 WHERE cust_no=t.cust_no
				   AND TRUNC(T.WAIT_TIME) - TRUNC(B.WAIT_TIME) < '8'
                   AND T.WAIT_TIME > B.WAIT_TIME
			   );
  COMMIT;
   
  DELETE FROM dm.tm_repeatcall_sta_zs
   WHERE stamonth = '$vMonth';
  COMMIT;
  
  INSERT INTO dm.tm_repeatcall_sta_zs NOLOGGING
  SELECT '$vMonth' stamonth,
          nvl(areaid,-9),
		  nvl(patchid,-9),
		  nvl(gridid,'-9'),
		  COUNT(1),
		  SUM(DECODE(isrepeat,'Y',1,0))
    FROM tmp.tmp_m159_callinfo_zs c
   WHERE to_char(c.WAIT_TIME,'yyyymm') = '$vMonth'
   GROUP BY areaid,
		  patchid,
		  gridid;
   COMMIT;

	 
EOF
;

# 提交批量sql的执行;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);

}
exit 0;

