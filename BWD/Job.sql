with test as (select peo_no, cli_no from peo_career where cli_no <> 0 and emp_to = 0 and emp_from <> 0)

,test2 as (select job_no,
a.cli_no,
iif(b.cli_no = a.cli_no,job_con_cnt_peo_no,concat('-999',a.cli_no))
as 'Cont_externalID',
--b.peo_no as cnt_peo,
--b.cli_no as cnt_cli,
job_title as 'position-title',
cast(replace(replace(job_reg_date,left(job_reg_date,4),concat(left(job_reg_date,4),'-')),left(replace(job_reg_date,left(job_reg_date,4),concat(left(job_reg_date,4),'-')),7),
concat(left(replace(job_reg_date,left(job_reg_date,4),concat(left(job_reg_date,4),'-')),7),'-')) as datetime) as 'position-startDate',
iif(job_temp_perm = 'Perm','PERMANENT','TEMPORARY') as 'position-type',
c.con_email as 'position-owner',
job_qty as 'position-headcount',
concat(
'External_ID: ', job_no,(char(13)+char(10)),
'Job Status: ',job_status,(char(13)+char(10)),
'Status_Date: ',replace(replace(job_status_date,left(job_status_date,4),concat(left(job_status_date,4),'-')),left(replace(job_status_date,left(job_status_date,4),concat(left(job_status_date,4),'-')),7),
concat(left(replace(job_status_date,left(job_status_date,4),concat(left(job_status_date,4),'-')),7),'-'))
,(char(13)+char(10)),
'Benefits: ',job_benefits
)as 'position-note',
job_salary as 'position-actualSalary',
job_salary_upper as salary_to,
job_currency as 'position_currency',
job_pay_unit as 'position-contractLength',
job_use_temp_payrate as 'position-payRate'
from jobs a left join test b on a.job_con_cnt_peo_no = b.peo_no
left join consultant c on a.job_con = c.con_initials
)

,test3 as (select ROW_NUMBER() over(partition by [Cont_externalID],[position-title],[position-startdate] order by [Cont_externalID]) as rn ,* from test2)

select iif(rn=1,[position-title],concat(rn,'-',[position-title])) as correct_position,* from test3 order by job_no

