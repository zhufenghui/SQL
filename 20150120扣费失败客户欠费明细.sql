 select distinct  b.MARKNO 客户证号,   
       CUSTNAME 姓名,   
       SERVADDR 安装地址,   
       LINKADDR 联系地址,   
       phone    电话,   
       mobile   手机,   
       substr(acctno,length(acctno)-4,4)  银行账号,   
       bankfees 扣费金额,   
       rtdata   扣费失败原因,     
       feemonth,
       (select name from ods.TO_PRV_AREA_ZS where areaid = b.areaid ) area ,    
       (select mnAME   
          from ods.to_prv_sysparam_zs   
         where gcode = 'SYS_BANK'   
           and mcode = (select BANKCODE   
                          from ods.TO_CUST_BANK_ZS   
                         where BANKID = a.BANKID   
                         )) 银行类型    
from   ods.to_bank_log_zs a   join   eds.TW_CUST_STATE_ZS_20150118 b    on     a.custid = b.custid    
join   ods.TO_ARREAR_TOTAL_ZS c   on     a.custid = c.custid   
where  feemonth = substr('20150118',0,6) and rtcode='N' and c.fees !=0 and billmon  ='201412'
        and b.areaid='719' and rtdata='余额不足'
order by b.MARKNO  
 
