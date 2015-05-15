create or replace procedure dropifexists  (
    p_table in varchar2
) AUTHID CURRENT_USER is
    v_count number(10);
begin
   select count(*)
   into v_count
   from all_tables
   where table_name = upper(p_table) AND owner='RECOMMENDATION';
   if v_count > 0 THEN
     --- dbms_output.put_line( 'drop table recommenation.'||p_table||' purge');
     execute immediate 'drop table recommendation.'||p_table||' purge';
   end if;
end dropifexists;
