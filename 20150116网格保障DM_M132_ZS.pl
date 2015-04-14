#!/usr/bin/perl
###############################################################################
# PERL数据库应用程序
#
# FILENAME:  DM_M132_ZS.pl
# PURPOSE:    	支公司故障数/故障率日汇总(TM_FAULTRATE_DAY_ZS)
#						 网格故障率月汇总(TM_FAULTRATE_MONTH_ZS)
#					   	 支公司故障率月汇总(TM_FAULTRATE_AREA_MONTH_ZS)
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
# 2014-09-01 DHG      工单20140725105400，修改250行的用户状态条件
#【S.SERVSTATUS NOT IN ('5', '6')>>>S.SERVSTATUS IN ('0','1','2','4')】
###############################################################################

use strict;
use dss_common;

# 版本号约束
use constant VERSION => "V01.00.000";

# 入参判断
my $vDate = shift || die "usage: $0 <YYYYMMDD>\n";
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
my $yesterday = dss_common::addDays($vDate,-1);

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

my $sql = <<EOF

	#DEBUG: 设置并行度;
  	ALTER SESSION ENABLE PARALLEL DML;
  	ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  	ALTER SESSION FORCE PARALLEL DML PARALLEL $de;

 	#DEBUG: 清理支公司故障率旧数据;
  	DELETE FROM DM.TM_FAULTRATE_DAY_ZS WHERE STADATE = '$yyyymmdd';
  	COMMIT;

  	#DEBUG: STEP1.临时表6统计当天各支公司的故障工单数;
	#IGNORE_ERROR:DROP TABLE TMP.TMP_DM_M132_6 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_6 NOLOGGING AS
 	SELECT T.AREAID,
       			DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4) SUBKIND,
         		COUNT(T.ID) WFO_NUMS
  	FROM EDS.TW_OSS_WORKFORM_ZS T
 	WHERE T.ISHITCH = 1
    AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD')  =  '$yyyymmdd'
 	AND order_state_name NOT IN(2,3)  --剔除已撤单和作废的工单
 	AND   LIABILITY NOT IN ( 1 ,4 )					--剔除责任归属为网络运维组和其他的数据
 	GROUP BY AREAID, DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4)
 	ORDER BY AREAID, SUBKIND;
 	COMMIT;

  #DEBUG: STEP2. 临时表7生成当天用户明细;
  #IGNORE_ERROR: DROP TABLE tmp.tmp_dm_m132_7 PURGE;
  CREATE TABLE TMP.TMP_DM_M132_7 NOLOGGING AS
  SELECT  S.SERVID,
  				S.CUSTID,
         		S.AREAID,
         		S.PERMARK,
         		C.CUSTTYPE,
         		S.CDEVID,
         		S.SUBMARK
    FROM EDS.TW_SERV_STATE_ZS_$yyyymmdd S,
    			EDS.TW_CUST_STATE_ZS_$yyyymmdd C
         	--	(SELECT DISTINCT (HOUSEID),
          --                GRIDCODE GRID_ID,
          --                AREAID,
          --                AREACODE PATCHID
          -- 	FROM ODS.TO_OSS_ADDRESS_TBL_ZS
          -- 	WHERE STATE = 1
          -- 	AND DELETESTATE = 0
          -- 	AND GRIDCODE IS NOT NULL) A --获取带有网格信息的有效地址
          --(SELECT HOUSEID,
          --                GRIDCODE GRID_ID,
          --                AREAID,
          --                PATCHID FROM ods.to_address_community_zs WHERE GRIDCODE IS NOT NULL) A --获取带有网格信息的有效地址
   WHERE S.CUSTID = C.CUSTID
  	 AND S.PERMARK <> 0 --模拟用户不统计
     AND S.SERVSTATUS NOT IN ('5', '6');
  COMMIT;

  #DEBUG: STEP3. 临时表8汇总当天支公司各种故障对应终端数;
  #DEBUG: 高清宽带终端数 = wifi互动用户数+广电宽带用户数;
  #IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M132_8 PURGE;
  CREATE TABLE TMP.TMP_DM_M132_8 NOLOGGING AS
  SELECT '$yyyymmdd' STADATE,AREAID,
          		3 SUBKIND,
           		COUNT(1) NUMS
    FROM TMP.TMP_DM_M132_7
  WHERE  (submark = 'UW') or (permark='2' AND submark <> 'UW')
  GROUP BY AREAID;
  COMMIT;

  #DEBUG: 高清互动终端数 = 互动用户数+kpi广电宽带用户数;
  INSERT INTO TMP.TMP_DM_M132_8
  SELECT '$yyyymmdd' STADATE,AREAID,
            	2 SUBKIND,
          		COUNT(1) NUMS
    FROM TMP.TMP_DM_M132_7
   WHERE (permark = '3')  or (permark='2' AND submark <> 'UW')
   GROUP BY AREAID;
  COMMIT;

  	#DEBUG: 单向数字电视终端数 = 公众数字用户+商业数字用户;
    INSERT INTO TMP.TMP_DM_M132_8
    SELECT '$yyyymmdd' STADATE,AREAID,
              	1 SUBKIND,
          		COUNT(1) NUMS
    FROM TMP.TMP_DM_M132_7 T
    WHERE T.PERMARK = 1
    AND T.CUSTTYPE IN (0,2)
   	GROUP BY AREAID;
	COMMIT;

	#DEBUG: STEP6.插入支公司故障率汇总数据;
	INSERT INTO DM.TM_FAULTRATE_DAY_ZS
	SELECT T8.STADATE, T8.AREAID, T8.SUBKIND, 0, T8.NUMS, substr($yyyymmdd, 0 , 6), substr($yyyymmdd, 0 , 4)
 	 FROM TMP.TMP_DM_M132_8 T8;
  	COMMIT;

  MERGE INTO DM.TM_FAULTRATE_DAY_ZS F
  USING TMP.TMP_DM_M132_6 T6
 	ON (F.AREAID = T6.AREAID
   AND F.SUBKIND = T6.SUBKIND
   AND F.STADATE = '$yyyymmdd')
   WHEN MATCHED THEN
   UPDATE SET F.WFO_NUMS = T6.WFO_NUMS
   WHEN NOT MATCHED THEN
   INSERT VALUES ('$yyyymmdd', T6.AREAID, T6.SUBKIND, T6.WFO_NUMS, 0, substr($yyyymmdd, 0, 6), substr($yyyymmdd, 0, 4));
  COMMIT;

EOF
;

# 提交批量sql的执行;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);

if ( $vFifthday  eq $yyyymmdd ) {
my $sql = <<EOF
  #DEBUG: 设置并行度;
  ALTER SESSION ENABLE PARALLEL DML;
  ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  ALTER SESSION FORCE PARALLEL DML PARALLEL $de;

	#DEBUG: 清理网格故障率旧数据;
  	DELETE FROM DM.TM_FAULTRATE_MONTH_ZS WHERE STAMONTH = '$vPreMonth';
  	COMMIT;

  	#DEBUG: STEP1.临时表0统计上月各网格的故障工单数;
	#IGNORE_ERROR:DROP TABLE TMP.TMP_DM_M132_0 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_0 NOLOGGING AS
 	SELECT T.AREAID,
       			T.GRID_ID,
       			DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4) SUBKIND,
         		COUNT(T.ID) WFO_NUMS
  	FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd T
 	WHERE T.ISHITCH = 1
    AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD')  >=  '$vPreFirstDay'
    AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD')  <=  '$vPreLastDay'
 	AND ORDER_STATE_NAME NOT IN(2,3)  --去掉无效的工单
 	AND LIABILITY NOT IN ( 1 ,4 )					--剔除责任归属为网络运维组和其他的数据
 	GROUP BY AREAID,GRID_ID,DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4)
 	ORDER BY AREAID,GRID_ID, SUBKIND;
 	COMMIT;

  #DEBUG: STEP2.临时表1生成用户及网格明细;
  #IGNORE_ERROR: DROP TABLE tmp.tmp_dm_m132_1 PURGE;
  CREATE TABLE TMP.TMP_DM_M132_1 NOLOGGING AS
	SELECT S.SERVID,
	       S.CUSTID,
	       S.AREAID,
	       T.GRIDCODE  GRID_ID,
	       S.PATCHID,
	       S.PERMARK,
	       S.SERVSTATUS,
	       S.BUSISTATUS,
	       C.CUSTTYPE,
	       S.CDEVID,
	       S.SUBMARK,
	       T.HOUSENO,
	       T.DELETESTATE
	  FROM EDS.TW_SERV_STATE_ZS_$vPreLastDay S,
	       EDS.TW_CUST_STATE_ZS_$vPreLastDay C,
	       ODS.TO_ADDRESS_COMMUNITY_ZS T
	 WHERE S.HOUSEID = T.HOUSEID(+)
	   AND S.CUSTID = C.CUSTID;
  COMMIT;

  	#DEBUG: 临时表3使用OSS运维组和BOSS片区对应关系表生成用户表分表，用于统计各网格(或运维组)终端数;
  	#IGNORE_ERROR: DROP TABLE tmp.tmp_dm_m132_3 PURGE;
    CREATE TABLE TMP.TMP_DM_M132_3 NOLOGGING AS
    SELECT  T.SERVID,
            T.CUSTID,
            T.AREAID,
            T.PATCHID,
            T.GRID_ID,
            T.PERMARK,
            T.SERVSTATUS,
            T.BUSISTATUS,
            T.CUSTTYPE,
            T.CDEVID,
            T.SUBMARK,
            T.HOUSENO,
            T.DELETESTATE
     FROM TMP.TMP_DM_M132_1 T
     WHERE T.SERVSTATUS IN ('0', '1', '2', '4')
       AND T.PERMARK <> 0;
     
  #DEBUG:插入没关联到网格地址或关联到无效地址的用户;
  INSERT INTO TMP.TMP_DM_M132_3 NOLOGGING
  SELECT S.SERVID,
         S.CUSTID,
         S.AREAID,
         S.PATCHID,
         '' GRID_ID,
         S.PERMARK,
         S.SERVSTATUS,
         S.BUSISTATUS,
         C.CUSTTYPE,
         S.CDEVID,
         S.SUBMARK,
         '' HOUSENO,
         '' DELETESTATE
    FROM EDS.TW_SERV_STATE_ZS_$vPreLastDay S,
         EDS.TW_CUST_STATE_ZS_$vPreLastDay C
   WHERE S.CUSTID = C.CUSTID
     AND S.SERVID NOT IN
         (SELECT SERVID FROM TMP.TMP_DM_M132_1 T)
     AND S.SERVSTATUS IN ('0', '1', '2', '4')
     AND S.PERMARK <> 0;
   COMMIT;
   
   #DEBUG: 增加字段 OSS部门，OSS片区运维组，OSS网格运维组（网格），展示所用ID(用于前台最终展示);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD OSS_DEPT NUMBER(16);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD OSS_PATCHORG NUMBER(16);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD OSS_GRIDORG NUMBER(16);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD DISPDIMID NUMBER(16);

  #DEBUG:更新北部支公司 片区运维组 -> 网格运维组;
  #DEBUG:更新石歧支公司 部门->片区运维组;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET (OSS_PATCHORG, OSS_GRIDORG) =
	      (SELECT ORGCODE, GRIDCODE
	         FROM ODS.TO_OSS_ORG_GRID_ZS B
	        WHERE A.GRID_ID = B.GRID_ID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID IN (715,720);
	COMMIT;
   
  #DEBUG:石歧找不到网格的部分，默认归入建维部;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET OSS_DEPT =
	      (SELECT ORGID
	         FROM ODS.TO_OSS_ORGPATCHDEF_ZS B
	        WHERE A.PATCHID = B.PATCHID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID  IN (715) AND A.GRID_ID IS NULL ;
	COMMIT;

	#DEBUG:北部支公司找不到网格找不到片区运维组的部分，按默认表TO_OSS_ORGPATCHDEF_ZS(此表为局方提供的片区与运维组对应数据);
	UPDATE TMP.TMP_DM_M132_3 A
	  SET (OSS_PATCHORG) =
	      (SELECT ORGID
	         FROM ODS.TO_OSS_ORGPATCHDEF_ZS B
	        WHERE A.PATCHID = B.PATCHID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID  IN (720) AND A.GRID_ID IS NULL;
	COMMIT;

	#DEBUG:用默认的片区与片区运维组对应表更新其他支公司的OSS片区运维组;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET (OSS_PATCHORG) =
	      (SELECT ORGID
	         FROM ODS.TO_OSS_ORGPATCHDEF_ZS B
	        WHERE A.PATCHID = B.PATCHID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID NOT IN (715,720);
	COMMIT;

	#DEBUG:更新OSS部门;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET OSS_DEPT =
	      (SELECT PARENT_ID
	         FROM ODS.TO_OSS_ORG_ZS T
	        WHERE T.ORG_ID = A.OSS_PATCHORG)
	     WHERE A.OSS_DEPT IS NULL;
	COMMIT;
	
  #DEBUG:更新展示维度ID（北部 片区运维组 -> 网格运维组，其他支公司 部门-片区运维组）;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET DISPDIMID =
	      (CASE
             WHEN OSS_GRIDORG IS NOT NULL THEN
              OSS_GRIDORG
             ELSE
              (CASE
                WHEN OSS_PATCHORG IS NOT NULL AND OSS_GRIDORG IS NULL THEN
                 OSS_PATCHORG
                ELSE
                 OSS_DEPT
              END)
           END)
     WHERE AREAID NOT IN ('715');
	COMMIT;
	
  UPDATE TMP.TMP_DM_M132_3 A
	SET DISPDIMID =
	      (CASE
             WHEN OSS_PATCHORG IS NOT NULL THEN
              OSS_PATCHORG
             ELSE
              (CASE
                WHEN OSS_PATCHORG IS NULL THEN
                 OSS_DEPT
                ELSE
                 NULL
              END)
           END)
     WHERE AREAID IN ('715');
	COMMIT;
	
	#DEBUG: 将基数明细表保存为月表;
	#IGNORE_ERROR: DROP TABLE DM.TM_SERVGRID_$vPreMonth PURGE;
	CREATE TABLE DM.TM_SERVGRID_$vPreMonth NOLOGGING AS
	SELECT * FROM  TMP.TMP_DM_M132_3;

  #DEBUG: STEP3.临时表2汇总各网格三种故障对应的用户数;
  #DEBUG: 高清宽带终端数:wifi互动用户数+广电宽带用户数;
  #IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M132_2 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_2 NOLOGGING AS
	SELECT '$vPreMonth' STAMONTH,AREAID,DISPDIMID GRID_ID,
				 	COUNT(1) NUMS,
					3 SUBKIND
    FROM TMP.TMP_DM_M132_3
	WHERE  (submark = 'UW') or (permark='2' AND submark <> 'UW')
	AND OSS_DEPT IS NOT NULL --交界地址不统计
	GROUP BY AREAID,DISPDIMID
 	ORDER BY AREAID,DISPDIMID;
 	COMMIT;

  #DEBUG: 高清互动终端数: 互动用户数+kpi广电宽带用户数;
  INSERT INTO TMP.TMP_DM_M132_2
  SELECT '$vPreMonth' STAMONTH,AREAID,DISPDIMID GRID_ID,
  				COUNT(1) NUMS,
  				2 SUBKIND
    FROM TMP.TMP_DM_M132_3
   WHERE (permark = '3')  or (permark='2' AND submark <> 'UW')
   AND OSS_DEPT IS NOT NULL --交界地址不统计
   GROUP BY AREAID, DISPDIMID
   ORDER BY AREAID, DISPDIMID;
	COMMIT;

	#DEBUG: 单向数字电视终端数=公众数字用户+商业数字用户;
  	INSERT INTO TMP.TMP_DM_M132_2
  	SELECT '$vPreMonth' STAMONTH,AREAID,DISPDIMID GRID_ID,
  				 COUNT(1) NUMS,
  				1 SUBKIND
    FROM TMP.TMP_DM_M132_3 T
    WHERE T.PERMARK = 1
    AND T.CUSTTYPE IN (0,2)
    AND T.OSS_DEPT IS NOT NULL --交界地址不统计
   	GROUP BY AREAID, DISPDIMID
   	ORDER BY AREAID, DISPDIMID;
	COMMIT;

	#DEBUG: STEP6.插入网格故障率汇总数据;
	INSERT INTO DM.TM_FAULTRATE_MONTH_ZS
	SELECT '$vPreMonth',T0.*,0
 	 FROM TMP.TMP_DM_M132_0 T0;
  	COMMIT;

  MERGE INTO DM.TM_FAULTRATE_MONTH_ZS F
  USING TMP.TMP_DM_M132_2 T2
 	ON (F.AREAID = T2.AREAID
   AND F.GRID_ID = T2.GRID_ID
   AND F.SUBKIND = T2.SUBKIND
   AND F.STAMONTH = '$vPreMonth')
   WHEN MATCHED THEN
   UPDATE SET F.DEV_NUMS = T2.NUMS
   WHERE F.STAMONTH = '$vPreMonth'
   WHEN NOT MATCHED THEN
   INSERT VALUES ('$vPreMonth',T2.AREAID,T2.GRID_ID,T2.SUBKIND,0,T2.NUMS);
 	COMMIT;

 	#DEBUG: 清理支公司故障率旧数据;
  	DELETE FROM DM.TM_FAULTRATE_AREA_MONTH_ZS WHERE STAMONTH = '$vPreMonth';
  	COMMIT;

  	#DEBUG: STEP7. 临时表4统计上月各支公司的故障工单数;
	#IGNORE_ERROR:DROP TABLE TMP.TMP_DM_M132_4 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_4 NOLOGGING AS
 	SELECT areaid, subkind, SUM(wfo_nums) wfo_nums
  		FROM tmp.tmp_dm_m132_0
 	GROUP BY areaid, subkind;
 	COMMIT;

  #DEBUG: STEP8. 临时表5汇总上月支公司各种故障对应终端数;
  #DEBUG: 高清宽带终端数:wifi互动用户数+广电宽带用户数;
  #IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M132_5 PURGE;
  CREATE TABLE TMP.TMP_DM_M132_5 NOLOGGING AS
  SELECT '$vPreMonth' STAMONTH,AREAID,
           COUNT(1) NUMS,
          3 SUBKIND
    FROM TMP.TMP_DM_M132_3
  WHERE  (submark = 'UW') or (permark='2' AND submark <> 'UW')
  GROUP BY AREAID;
  COMMIT;

  #DEBUG: 高清互动终端数: 互动用户数+kpi广电宽带用户数;
  INSERT INTO TMP.TMP_DM_M132_5
  SELECT '$vPreMonth' STAMONTH,AREAID,
          COUNT(1) NUMS,
          2 SUBKIND
    FROM TMP.TMP_DM_M132_3
   WHERE (permark = '3')  or (permark='2' AND submark <> 'UW')
   GROUP BY AREAID;
  COMMIT;

  	#DEBUG: 单向数字电视终端数=公众数字用户+商业数字用户;
    INSERT INTO TMP.TMP_DM_M132_5
    SELECT '$vPreMonth' STAMONTH,AREAID,
          COUNT(1) NUMS,
          1 SUBKIND
    FROM TMP.TMP_DM_M132_3 T
    WHERE T.PERMARK = 1
    AND T.CUSTTYPE IN (0,2)
   	GROUP BY AREAID;
	COMMIT;

	#DEBUG: STEP6.插入支公司故障率汇总数据;
	INSERT INTO DM.TM_FAULTRATE_AREA_MONTH_ZS
	SELECT '$vPreMonth',T4.*,0
 	 FROM TMP.TMP_DM_M132_4 T4;
  	COMMIT;

  MERGE INTO DM.TM_FAULTRATE_AREA_MONTH_ZS F
  USING TMP.TMP_DM_M132_5 T5
 	ON (F.AREAID = T5.AREAID
   AND F.SUBKIND = T5.SUBKIND
   AND F.STAMONTH = '$vPreMonth')
   WHEN MATCHED THEN
   UPDATE SET F.DEV_NUMS = T5.NUMS
   WHERE F.STAMONTH = '$vPreMonth'
   WHEN NOT MATCHED THEN
   INSERT VALUES ('$vPreMonth',T5.AREAID,T5.SUBKIND,0,T5.NUMS);
 	COMMIT;

EOF
;

# 提交批量sql的执行;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);
}
exit 0;
