#!/usr/bin/perl
###############################################################################
# PERL���ݿ�Ӧ�ó���
#
# FILENAME:  DM_M136_ZS.pl
# PURPOSE:   ����ͳ���±���TM_WORKFORM_MONTH_ZS)
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
###############################################################################

use strict;
use dss_common;

# �汾��Լ��
use constant VERSION => "V01.00.000";

# ����ж�
my $vDate = shift || die "usage: $0 <YYYYMMDD>\n";
my $vBranch = shift || "ZS";
my $yyyymmdd = $vDate;
my $vMonth = substr($vDate,0,6);

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

if ( $vFifthday  eq $yyyymmdd ) {
my $sql = <<EOF
  #DEBUG: ���ò��ж�;
  ALTER SESSION ENABLE PARALLEL DML;
  ALTER SESSION FORCE PARALLEL query PARALLEL $de;
  ALTER SESSION FORCE PARALLEL DML PARALLEL $de;

#DEBUG: ���ܰ�װ���±�������;
#IGNORE_ERROR: DROP TABLE TMP.TMP_DM_M136_ZS_1 PURGE;
CREATE TABLE TMP.TMP_DM_M136_ZS_1 NOLOGGING AS
SELECT AREAID,
       GRID_ID,
       ISHITCH,
       SUBKIND_CODE,
       COUNT(1) SENDNUMS,                     	--�ɷ�������(��ִ���й������޻ص����˵������Ĺ����Ѿ�����ϸ����˵�)
       SUM(CASE WHEN ORDER_STATE_NAME IN (1,5) THEN 1
                ELSE 0 END) HANDLENUMS,       --��������
       SUM(CASE WHEN (ORDER_STATE_NAME = 1 AND TO_CHAR(FINISH_DATE,'YYYYMMDD') <> 20991231) THEN 1
                ELSE 0 END) SUCCNUMS,         	--����ɹ���
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1,2,3,4,5)) THEN  1
                ELSE 0 END) OUTTIMENUMS,      --��ʱ��������
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1,2,3,4,5)) THEN 1
                ELSE 0 END) OUTNOHANDLES,    --��ʱδ��������
       SUM(CASE WHEN (SYSDATE < LIMIT_DATE AND ORDER_STATE_NAME NOT IN (1,2,3,4)) THEN 1
                ELSE 0 END) NOHANDLES,        	 --ʱ����δ������
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
               END) ELSE 0 END) TOTAL_ONTIME,     --����������ʱ��(����)���������طù���
       SUM(CASE WHEN (ORDER_STATE_NAME IN (1, 2, 3) AND OUTTIME_FLAG = 1)
                THEN LIMIT_TIME ELSE 0 END) TOTAL_OUTTIME,        --������ʱʱ��(����)���������طù���
       SUM(CASE WHEN (OUTTIME_FLAG <= 0 AND  ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENZI,     --��ʱ�ʷ���
       SUM(CASE WHEN (ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENMU	--��ʱ�ʷ�ĸ
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
  
  #DEBUG:ɾ�����³�ʱ����װ����ϸ;
  DELETE DM.TM_OSS_OVERTIMEDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 0;
  COMMIT;
  
  #DEBUG:�������³�ʱ����װ����ϸ;
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
	
	#DEBUG:ɾ�����³�ʱδ����װ����ϸ;
  DELETE DM.TM_OSS_OVERTIMEUNDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 0;
  COMMIT;
  
  #DEBUG:�������³�ʱδ����װ����ϸ;
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

  #DEBUG: ���ܹ��ϵ��±�������,�޳����ι���Ϊ������ά�������������;
  INSERT INTO TMP.TMP_DM_M136_ZS_1 
  SELECT AREAID,
       GRID_ID,
       ISHITCH,
       SUBKIND_CODE,
       COUNT(1) SENDNUMS,                     	--�ɷ�������(��ִ���й������޻ص����˵������Ĺ����Ѿ�����ϸ����˵�)
       SUM(CASE WHEN ORDER_STATE_NAME IN (1,6) THEN 1
                ELSE 0 END) HANDLENUMS,       --��������
       SUM(CASE WHEN (ORDER_STATE_NAME = 1 AND TO_CHAR(FINISH_DATE,'YYYYMMDD') <> 20991231) THEN 1
                ELSE 0 END) SUCCNUMS,         	--����ɹ���
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1,2,3,4,6)) THEN  1
                ELSE 0 END) OUTTIMENUMS,      --��ʱ��������
       SUM(CASE WHEN (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1,2,3,4,6)) THEN 1
                ELSE 0 END) OUTNOHANDLES,    --��ʱδ��������
       SUM(CASE WHEN (SYSDATE < LIMIT_DATE AND ORDER_STATE_NAME NOT IN (1,2,3,4)) THEN 1
                ELSE 0 END) NOHANDLES,        	 --ʱ����δ������
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
               END)  ELSE 0 END) TOTAL_ONTIME,     --����������ʱ��(����)���������طù���
       SUM(CASE WHEN (ORDER_STATE_NAME IN (1, 2, 3) AND OUTTIME_FLAG = 1)
                THEN LIMIT_TIME ELSE 0 END) TOTAL_OUTTIME,    	--������ʱʱ��(����)���������طù���
       SUM(CASE WHEN (OUTTIME_FLAG <= 0 AND  ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENZI,     --��ʱ�ʷ���
       SUM(CASE WHEN (ORDER_STATE_NAME NOT IN (2, 3)) THEN 1
       			ELSE 0 END) RATE_FENMU	--��ʱ�ʷ�ĸ
  FROM EDS.TW_OSS_WORKFORM_ZS_$yyyymmdd
  WHERE TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') >= '$vPreFirstDay'
  AND   TO_CHAR(ACCEPT_DATE, 'YYYYMMDD') <= '$vPreLastDay'
  AND	ISHITCH = 1
  AND   LIABILITY NOT IN ( 1 ,4 )					--�޳����ι���Ϊ������ά�������������
  AND 	ORDER_STATE_NAME NOT IN (2, 3)
  GROUP BY  AREAID,
           	GRID_ID,
            ISHITCH,
            SUBKIND_CODE;
  COMMIT;
  
  #DEBUG:ɾ�����³�ʱ������ϵ���ϸ;
  DELETE DM.TM_OSS_OVERTIMEDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 1;
  COMMIT;
  
  #DEBUG:�������³�ʱ������ϵ���ϸ;
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
	   AND  LIABILITY NOT IN ( 1 ,4 )					--�޳����ι���Ϊ������ά�������������
	   AND (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME IN (1, 4, 6));
	COMMIT;
	
  #DEBUG:ɾ�����³�ʱδ������ϵ���ϸ;
  DELETE DM.TM_OSS_OVERTIMEUNDO_DETAIL WHERE STAMONTH = '$vPreMonth' AND ISHITCH = 1;
  COMMIT;
  
  #DEBUG:�������³�ʱδ������ϵ���ϸ;
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
	   AND  LIABILITY NOT IN ( 1 ,4 )					--�޳����ι���Ϊ������ά�������������
	   AND (OUTTIME_FLAG = 1 AND ORDER_STATE_NAME NOT IN (1,2,3,4,6));
	COMMIT;

  #DEBUG: ɾ��������;
  DELETE  FROM DM.TM_WORKFORM_MONTH_ZS WHERE STAMONTH = '$vPreMonth';
  COMMIT;
  
  #DEBUG: ����������;
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

# �ύ����sql��ִ��;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);
}
exit 0;

