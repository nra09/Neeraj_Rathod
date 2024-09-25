CREATE OR REPLACE PROCEDURE `skyuk-uk-vis-cust-res-s1-lab.neeraj_r_sandpit.qa_test`(env STRING, dates DATE)
begin
/*
--------------------------------------------------------------------------------------------------
Filename      :	qa_test
Author        :	Neeraj Rathod
Date Created  :	2024-09-24
Description   : checks for QA test results for specific set of tables based on the agreed granulartiy checks 

Revisions
===============================================================================================
Date          userid(s)       Ver.      Comments                                           
----------------------------------------------------------------------------
2024-09-24	  Neeraj Rathod		v0.1		Inital Version 
===============================================================================================	
*/

declare i numeric(10) default 1;
declare var_code string;
declare var_test_sql string;
declare var_results numeric(10);
declare var_end numeric(10) default 0;
declare v_current_batch_run numeric(10) default 0; 

#Temp table used to identify the test codes and adding sequence from the main table 
create or replace temp table current_run as 
select 
dense_rank() over (order by code asc) as sequence,
code,
parameter,
test_sql,
exp_result
from
`skyuk-uk-vis-cust-res-s1-lab.neeraj_r_sandpit.qa_tests`
where enabled='Y'
order by code;

#sets the id of the last procedure to run using it's sequence number
set var_end=(select max(sequence) from current_run);

#loop through using the sequence until reaching the last id
while i<=var_end do 

#for each sequence set the variables for that specific refesh
set v_current_batch_run=i;

set var_code=(select code from current_run where sequence=v_current_batch_run);

set var_test_sql=(select replace(replace(test_sql,"env",var_env),"= date",'='||dates) from current_run where sequence=v_current_batch_run);


if v_current_batch_run<>0 and var_test_sql is not null 
then 
execute immediate var_test_sql into var_results;

insert into skyuk-uk-vis-cust-res-s1-lab.neeraj_r_sandpit.qa_output values (datetime(current_timestamp(),"Europe/London"),var_code,var_test_sql,var_results);
else select "Null";
end if;
set i=i+1;
end while;

#writes any errors to error log table if script fails
exception when error then
insert into `skyuk-uk-vis-cust-res-s1-lab.neeraj_r_sandpit.qa_test_error_log` 
(dates,error_message,statement_text,stack_trace,formatted_stack_trace)
select current_timestamp() dates, @@error.message error_message, @@error.statement_text statement_text,@@error.stack_trace stack_trace,@@error.formatted_stack_trace formatted_stack_trace;
raise using message =@@error.message;

end;