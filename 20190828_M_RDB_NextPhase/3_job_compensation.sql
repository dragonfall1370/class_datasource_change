--CONTRACT JOB COMPENSATION
select concat('NP',j.JobId) as job_ext_id
, j.EmploymentTypeId
, case 
	when j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) then 'CONTRACT'
	when j.EmploymentTypeId in (4, 9, 12) then 'PERMANENT'
	else 'CONTRACT' end as position_type --check in contract jobs
, convert(money,coalesce(j.MinBasic,0)) as salaryFrom
, convert(money,coalesce(j.MaxBasic,0)) as salaryTo
, j.CommissionPerc
, convert(money,coalesce(cj.ChargeRate,0)) as ChargeRate --default 0 if NULL
, convert(money,coalesce(cj.PayRate,0)) as PayRate --default 0 if NULL
, cj.ChargeUnitValueId
, case when lv.ValueName = 'Day' then 3
	when lv.ValueName = 'Hour' then 2
	else NULL end as contract_rate_type
, case when lv.ValueName = 'Day' then 3
	when lv.ValueName = 'Hour' then 2
	else NULL end as contract_length_type
from Jobs j
left join ContractJobs cj on cj.JobId = j.JobId
left join ListValues lv on lv.ListValueId = cj.ChargeUnitValueId
where 1=1
and j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49) --Deleted Clients
and (j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) or j.EmploymentTypeId is NULL) --2692 --CONTRACT JOB
--and j.EmploymentTypeId not in (5, 6, 7, 8, 10, 11, 13) --10913 --PERMANENT JOB


--PERMANENT JOB COMPENSATION
select concat('NP',j.JobId) as job_ext_id
, j.EmploymentTypeId
, case 
	when j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) then 'CONTRACT'
	when j.EmploymentTypeId in (4, 9, 12) then 'PERMANENT'
	else 'CONTRACT' end as position_type --check in contract jobs
, convert(money,j.MaxBasic) as gross_annual_salary
, convert(money,j.MinBasic) as salaryFrom
, convert(money,j.MaxBasic) as salaryTo
, j.CommissionPerc
, cj.ChargeRate
, cj.PayRate
, cj.ChargeUnitValueId
, case when lv.ValueName = 'Day' then '3'
	when lv.ValueName = 'Hour' then '2'
	else NULL end as contract_rate_type
from Jobs j
left join ContractJobs cj on cj.JobId = j.JobId
left join ListValues lv on lv.ListValueId = cj.ChargeUnitValueId
where 1=1
and j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49) --Deleted Clients
--and (j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) or j.EmploymentTypeId is NULL) --2671 --CONTRACT JOB
and j.EmploymentTypeId in (4, 9, 12) --11010 --PERMANENT JOB
