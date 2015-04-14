#!/usr/bin/perl
###############################################################################
# PERL���ݿ�Ӧ�ó���
#
# FILENAME:  DM_M132_ZS.pl
# PURPOSE:    	֧��˾������/�������ջ���(TM_FAULTRATE_DAY_ZS)
#						 ����������»���(TM_FAULTRATE_MONTH_ZS)
#					   	 ֧��˾�������»���(TM_FAULTRATE_AREA_MONTH_ZS)
# AUTHOR:    ��˹��
# DESCRIPTION:
#
# PARAMETERS:
#    $vDate           ͳ�����ڣ�YYYYMMDD��
# EXIT STATUS:
#    0                �ɹ�
#    ����             ʧ��
# HISTORY:
# DATE       AUTHOR   VERSION    MODIFICATIONS
# ########## ######## ########## #############################################
# 2012-05-11 ����     V01.00.000 �½�����
# 2014-09-01 DHG      ����20140725105400���޸�250�е��û�״̬����
#��S.SERVSTATUS NOT IN ('5', '6')>>>S.SERVSTATUS IN ('0','1','2','4')��
###############################################################################

use strict;
use dss_common;

# �汾��Լ��
use constant VERSION => "V01.00.000";

# ����ж�
my $vDate = shift || die "usage: $0 <YYYYMMDD>\n";
my $vBranch = shift || "ZS";
my $vMonth = substr($vDate,0,6);
my $yyyymmdd = $vDate;

# �������ݿ�����
my $conn = dss_common->new();

# ��־����Ϊ: EBUG, INFO, WARNING, ERROR, FATAL
$conn->writelog(dss_common::INFO, "VERSION=" . VERSION);
$conn->writelog(dss_common::INFO, "Data_Date=" .$vDate."  Branch=".$vBranch);

# ִ�����ݿ�����, Ĭ������Ϊ�Զ��ύ����
$conn->connect_dwdb($vBranch, $vDate);

# ��ȡ��ǰ����Ψһ��־
my $ssid = $conn->get_sessionid();

############################
# ���ýű��˳��������ݷ�Χ #
############################

END {
  # �Ƿ����ֱ���˳�
  exit 1 if (!defined($conn));

  # �ɹ�ִ�нű����ύ����ع�
  $conn->cleanup();

  # �˳�д״̬��־
  $conn->writelog(INFO, "exit($?)");
}

###################################
# ִ������sql����, �������ò��ж� #
# Ĭ�����ִ��ѡ��Ϊautocommit    #
# ��������Ϣ������[�ֺ�]��������!!#
###################################

my $de = dss_common::get_parallel($vBranch,"M");
my $yesterday = dss_common::addDays($vDate,-1);

# �ϸ��µ�:yyyymmdd
my $vPreLastDay = dss_common::addMonths($yyyymmdd,-1);
   	  $vPreLastDay = dss_common::getLastDateOfMonth($vPreLastDay);

# ���ϸ��µ�:vPre2LastDay
my $vPre2LastDay = dss_common::addMonths($vPreLastDay,-1);
	  $vPre2LastDay = dss_common::getLastDateOfMonth($vPre2LastDay);

# �ϸ��³�:vPreFirstDay
my $vPreFirstDay =  dss_common::addDays($vPre2LastDay,+1);

# �ϸ���:vPreMonth
my $vPreMonth = substr($vPreFirstDay,0,6);

# ����5��:vFifthday
my $vFifthday = dss_common::addDays($vPreLastDay,5);

my $sql = <<EOF

	#DEBUG: ���ò��ж�;
  	ALTER SESSION ENABLE PARALLEL DML;
  	ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  	ALTER SESSION FORCE PARALLEL DML PARALLEL $de;

 	#DEBUG: ����֧��˾�����ʾ�����;
  	DELETE FROM DM.TM_FAULTRATE_DAY_ZS WHERE STADATE = '$yyyymmdd';
  	COMMIT;

  	#DEBUG: STEP1.��ʱ��6ͳ�Ƶ����֧��˾�Ĺ��Ϲ�����;
	#IGNORE_ERROR:DROP TABLE TMP.TMP_DM_M132_6 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_6 NOLOGGING AS
 	SELECT T.AREAID,
       			DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4) SUBKIND,
         		COUNT(T.ID) WFO_NUMS
  	FROM EDS.TW_OSS_WORKFORM_ZS T
 	WHERE T.ISHITCH = 1
    AND TO_CHAR(ACCEPT_DATE, 'YYYYMMDD')  =  '$yyyymmdd'
 	AND order_state_name NOT IN(2,3)  --�޳��ѳ��������ϵĹ���
 	AND   LIABILITY NOT IN ( 1 ,4 )					--�޳����ι���Ϊ������ά�������������
 	GROUP BY AREAID, DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4)
 	ORDER BY AREAID, SUBKIND;
 	COMMIT;

  #DEBUG: STEP2. ��ʱ��7���ɵ����û���ϸ;
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
          -- 	AND GRIDCODE IS NOT NULL) A --��ȡ����������Ϣ����Ч��ַ
          --(SELECT HOUSEID,
          --                GRIDCODE GRID_ID,
          --                AREAID,
          --                PATCHID FROM ods.to_address_community_zs WHERE GRIDCODE IS NOT NULL) A --��ȡ����������Ϣ����Ч��ַ
   WHERE S.CUSTID = C.CUSTID
  	 AND S.PERMARK <> 0 --ģ���û���ͳ��
     AND S.SERVSTATUS NOT IN ('5', '6');
  COMMIT;

  #DEBUG: STEP3. ��ʱ��8���ܵ���֧��˾���ֹ��϶�Ӧ�ն���;
  #DEBUG: �������ն��� = wifi�����û���+������û���;
  #IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M132_8 PURGE;
  CREATE TABLE TMP.TMP_DM_M132_8 NOLOGGING AS
  SELECT '$yyyymmdd' STADATE,AREAID,
          		3 SUBKIND,
           		COUNT(1) NUMS
    FROM TMP.TMP_DM_M132_7
  WHERE  (submark = 'UW') or (permark='2' AND submark <> 'UW')
  GROUP BY AREAID;
  COMMIT;

  #DEBUG: ���廥���ն��� = �����û���+kpi������û���;
  INSERT INTO TMP.TMP_DM_M132_8
  SELECT '$yyyymmdd' STADATE,AREAID,
            	2 SUBKIND,
          		COUNT(1) NUMS
    FROM TMP.TMP_DM_M132_7
   WHERE (permark = '3')  or (permark='2' AND submark <> 'UW')
   GROUP BY AREAID;
  COMMIT;

  	#DEBUG: �������ֵ����ն��� = ���������û�+��ҵ�����û�;
    INSERT INTO TMP.TMP_DM_M132_8
    SELECT '$yyyymmdd' STADATE,AREAID,
              	1 SUBKIND,
          		COUNT(1) NUMS
    FROM TMP.TMP_DM_M132_7 T
    WHERE T.PERMARK = 1
    AND T.CUSTTYPE IN (0,2)
   	GROUP BY AREAID;
	COMMIT;

	#DEBUG: STEP6.����֧��˾�����ʻ�������;
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

# �ύ����sql��ִ��;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);

if ( $vFifthday  eq $yyyymmdd ) {
my $sql = <<EOF
  #DEBUG: ���ò��ж�;
  ALTER SESSION ENABLE PARALLEL DML;
  ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  ALTER SESSION FORCE PARALLEL DML PARALLEL $de;

	#DEBUG: ������������ʾ�����;
  	DELETE FROM DM.TM_FAULTRATE_MONTH_ZS WHERE STAMONTH = '$vPreMonth';
  	COMMIT;

  	#DEBUG: STEP1.��ʱ��0ͳ�����¸�����Ĺ��Ϲ�����;
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
 	AND ORDER_STATE_NAME NOT IN(2,3)  --ȥ����Ч�Ĺ���
 	AND LIABILITY NOT IN ( 1 ,4 )					--�޳����ι���Ϊ������ά�������������
 	GROUP BY AREAID,GRID_ID,DECODE(T.SUBKIND_CODE,'10',1,'111',1,'20',1,'30',1,'40',2,'100',2,'50',3,'60',3,'70',3,'80',3,'110',3,'280',3,4)
 	ORDER BY AREAID,GRID_ID, SUBKIND;
 	COMMIT;

  #DEBUG: STEP2.��ʱ��1�����û���������ϸ;
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

  	#DEBUG: ��ʱ��3ʹ��OSS��ά���BOSSƬ����Ӧ��ϵ�������û���ֱ�����ͳ�Ƹ�����(����ά��)�ն���;
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
     
  #DEBUG:����û�����������ַ���������Ч��ַ���û�;
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
   
   #DEBUG: �����ֶ� OSS���ţ�OSSƬ����ά�飬OSS������ά�飨���񣩣�չʾ����ID(����ǰ̨����չʾ);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD OSS_DEPT NUMBER(16);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD OSS_PATCHORG NUMBER(16);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD OSS_GRIDORG NUMBER(16);
   ALTER TABLE TMP.TMP_DM_M132_3 ADD DISPDIMID NUMBER(16);

  #DEBUG:���±���֧��˾ Ƭ����ά�� -> ������ά��;
  #DEBUG:����ʯ��֧��˾ ����->Ƭ����ά��;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET (OSS_PATCHORG, OSS_GRIDORG) =
	      (SELECT ORGCODE, GRIDCODE
	         FROM ODS.TO_OSS_ORG_GRID_ZS B
	        WHERE A.GRID_ID = B.GRID_ID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID IN (715,720);
	COMMIT;
   
  #DEBUG:ʯ���Ҳ�������Ĳ��֣�Ĭ�Ϲ��뽨ά��;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET OSS_DEPT =
	      (SELECT ORGID
	         FROM ODS.TO_OSS_ORGPATCHDEF_ZS B
	        WHERE A.PATCHID = B.PATCHID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID  IN (715) AND A.GRID_ID IS NULL ;
	COMMIT;

	#DEBUG:����֧��˾�Ҳ��������Ҳ���Ƭ����ά��Ĳ��֣���Ĭ�ϱ�TO_OSS_ORGPATCHDEF_ZS(�˱�Ϊ�ַ��ṩ��Ƭ������ά���Ӧ����);
	UPDATE TMP.TMP_DM_M132_3 A
	  SET (OSS_PATCHORG) =
	      (SELECT ORGID
	         FROM ODS.TO_OSS_ORGPATCHDEF_ZS B
	        WHERE A.PATCHID = B.PATCHID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID  IN (720) AND A.GRID_ID IS NULL;
	COMMIT;

	#DEBUG:��Ĭ�ϵ�Ƭ����Ƭ����ά���Ӧ���������֧��˾��OSSƬ����ά��;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET (OSS_PATCHORG) =
	      (SELECT ORGID
	         FROM ODS.TO_OSS_ORGPATCHDEF_ZS B
	        WHERE A.PATCHID = B.PATCHID
	          AND A.AREAID = B.AREAID)
	WHERE A.AREAID NOT IN (715,720);
	COMMIT;

	#DEBUG:����OSS����;
	UPDATE TMP.TMP_DM_M132_3 A
	  SET OSS_DEPT =
	      (SELECT PARENT_ID
	         FROM ODS.TO_OSS_ORG_ZS T
	        WHERE T.ORG_ID = A.OSS_PATCHORG)
	     WHERE A.OSS_DEPT IS NULL;
	COMMIT;
	
  #DEBUG:����չʾά��ID������ Ƭ����ά�� -> ������ά�飬����֧��˾ ����-Ƭ����ά�飩;
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
	
	#DEBUG: ��������ϸ����Ϊ�±�;
	#IGNORE_ERROR: DROP TABLE DM.TM_SERVGRID_$vPreMonth PURGE;
	CREATE TABLE DM.TM_SERVGRID_$vPreMonth NOLOGGING AS
	SELECT * FROM  TMP.TMP_DM_M132_3;

  #DEBUG: STEP3.��ʱ��2���ܸ��������ֹ��϶�Ӧ���û���;
  #DEBUG: �������ն���:wifi�����û���+������û���;
  #IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M132_2 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_2 NOLOGGING AS
	SELECT '$vPreMonth' STAMONTH,AREAID,DISPDIMID GRID_ID,
				 	COUNT(1) NUMS,
					3 SUBKIND
    FROM TMP.TMP_DM_M132_3
	WHERE  (submark = 'UW') or (permark='2' AND submark <> 'UW')
	AND OSS_DEPT IS NOT NULL --�����ַ��ͳ��
	GROUP BY AREAID,DISPDIMID
 	ORDER BY AREAID,DISPDIMID;
 	COMMIT;

  #DEBUG: ���廥���ն���: �����û���+kpi������û���;
  INSERT INTO TMP.TMP_DM_M132_2
  SELECT '$vPreMonth' STAMONTH,AREAID,DISPDIMID GRID_ID,
  				COUNT(1) NUMS,
  				2 SUBKIND
    FROM TMP.TMP_DM_M132_3
   WHERE (permark = '3')  or (permark='2' AND submark <> 'UW')
   AND OSS_DEPT IS NOT NULL --�����ַ��ͳ��
   GROUP BY AREAID, DISPDIMID
   ORDER BY AREAID, DISPDIMID;
	COMMIT;

	#DEBUG: �������ֵ����ն���=���������û�+��ҵ�����û�;
  	INSERT INTO TMP.TMP_DM_M132_2
  	SELECT '$vPreMonth' STAMONTH,AREAID,DISPDIMID GRID_ID,
  				 COUNT(1) NUMS,
  				1 SUBKIND
    FROM TMP.TMP_DM_M132_3 T
    WHERE T.PERMARK = 1
    AND T.CUSTTYPE IN (0,2)
    AND T.OSS_DEPT IS NOT NULL --�����ַ��ͳ��
   	GROUP BY AREAID, DISPDIMID
   	ORDER BY AREAID, DISPDIMID;
	COMMIT;

	#DEBUG: STEP6.������������ʻ�������;
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

 	#DEBUG: ����֧��˾�����ʾ�����;
  	DELETE FROM DM.TM_FAULTRATE_AREA_MONTH_ZS WHERE STAMONTH = '$vPreMonth';
  	COMMIT;

  	#DEBUG: STEP7. ��ʱ��4ͳ�����¸�֧��˾�Ĺ��Ϲ�����;
	#IGNORE_ERROR:DROP TABLE TMP.TMP_DM_M132_4 PURGE;
	CREATE TABLE TMP.TMP_DM_M132_4 NOLOGGING AS
 	SELECT areaid, subkind, SUM(wfo_nums) wfo_nums
  		FROM tmp.tmp_dm_m132_0
 	GROUP BY areaid, subkind;
 	COMMIT;

  #DEBUG: STEP8. ��ʱ��5��������֧��˾���ֹ��϶�Ӧ�ն���;
  #DEBUG: �������ն���:wifi�����û���+������û���;
  #IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M132_5 PURGE;
  CREATE TABLE TMP.TMP_DM_M132_5 NOLOGGING AS
  SELECT '$vPreMonth' STAMONTH,AREAID,
           COUNT(1) NUMS,
          3 SUBKIND
    FROM TMP.TMP_DM_M132_3
  WHERE  (submark = 'UW') or (permark='2' AND submark <> 'UW')
  GROUP BY AREAID;
  COMMIT;

  #DEBUG: ���廥���ն���: �����û���+kpi������û���;
  INSERT INTO TMP.TMP_DM_M132_5
  SELECT '$vPreMonth' STAMONTH,AREAID,
          COUNT(1) NUMS,
          2 SUBKIND
    FROM TMP.TMP_DM_M132_3
   WHERE (permark = '3')  or (permark='2' AND submark <> 'UW')
   GROUP BY AREAID;
  COMMIT;

  	#DEBUG: �������ֵ����ն���=���������û�+��ҵ�����û�;
    INSERT INTO TMP.TMP_DM_M132_5
    SELECT '$vPreMonth' STAMONTH,AREAID,
          COUNT(1) NUMS,
          1 SUBKIND
    FROM TMP.TMP_DM_M132_3 T
    WHERE T.PERMARK = 1
    AND T.CUSTTYPE IN (0,2)
   	GROUP BY AREAID;
	COMMIT;

	#DEBUG: STEP6.����֧��˾�����ʻ�������;
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

# �ύ����sql��ִ��;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);
}
exit 0;
