#!/usr/bin/perl
###############################################################################
# PERL���ݿ�Ӧ�ó���
#
# PURPOSE:   KPIͼ��-�ͷ�Ч��ͳ��(TM_REPEATCALL_STA_ZS)
#
# AUTHOR:    ���
# DESCRIPTION:
#
# PARAMETERS:
#    $vDate           ͳ�����ڣ�YYYYMMDD��
#    $vBranch         ҵ����
# EXIT STATUS:
#    0                �ɹ�
#    ����             ʧ��
###############################################################################

use strict;
use dss_common;

# �汾��Լ��
use constant VERSION => "V01.00.000";

# ����ж�
my $vDate = shift || die "usage: $0 <yyyymmdd>\n";
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

my $predate = $conn->execute_select("SELECT to_char(to_date('$yyyymmdd','yyyymmdd')-1,'yyyymmdd') FROM dual");
if ($predate eq "") {
  print "�޷���ȡϵͳʱ�䡣\n";
  exit 1;
}

my $nowDay = dss_common::addDays($yyyymmdd,1);
# �����µ�:yyyymmdd
my $vMonthLastDay = dss_common::getLastDateOfMonth($yyyymmdd);

#7������
my $nowDay = dss_common::addDays($yyyymmdd,1);
# ��һ�µ�:yyyymmdd
my $vPreLastDay = dss_common::addMonths($yyyymmdd,-1);
   $vPreLastDay = dss_common::getLastDateOfMonth($vPreLastDay);
my $preMonth = substr($vPreLastDay,0,6);

if ($vMonthLastDay eq $yyyymmdd){  
my $sql = <<EOF
	#DEBUG: ���ò��ж�;
	ALTER SESSION ENABLE PARALLEL DML;
	ALTER SESSION FORCE PARALLEL query PARALLEL $de;
	ALTER SESSION FORCE PARALLEL DML PARALLEL $de;


  #DEBUG: �ͻ���ַ��Ϣ;
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
   
  #DEBUG:�ͷ���Ϣ����������ȡ�ͻ���Ϣ;
  #IGNORE_ERROR:drop table tmp.tmp_m159_callorder_zs purge;
  CREATE TABLE tmp.tmp_m159_callorder_zs NOLOGGING AS
  SELECT t.*,a.cust_no,a.con_addr
    FROM ODS.TO_CC_CALL_$yyyymmdd t, ODS.TO_Z_WO_ORDER_TOTAL a
   WHERE t.DIRECT='I'  ---�ų���� 
    AND to_char(t.WAIT_TIME,'yyyymm') IN('$vMonth','$preMonth')
	AND t.ORDER_ID = a.ORDER_ID(+);
  

  #DEBUG:��ȡ��ַ��Ϣ;
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

# �ύ����sql��ִ��;
my $result = $conn->batch_execute_sql($sql);
exit 1 if ($result != 0);

}
exit 0;

