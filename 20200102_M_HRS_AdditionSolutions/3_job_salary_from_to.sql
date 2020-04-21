--Compensation: Salary from / Salary to
select __pk
	, salary_low_exchanged
	, try_parse(trim('£' from salary_low_exchanged) as bigint) as salary_from
	, salary_high_exchanged
	, try_parse(trim('£' from salary_high_exchanged) as bigint) as salary_to
from [20191030_155620_jobs]
where coalesce(nullif(salary_low_exchanged, ''), nullif(salary_high_exchanged, '')) is not NULL


--Pay Rate
select distinct rate_std_exchanged
, try_parse(trim('£' from rate_std_exchanged) as bigint) as pay_rate
from [20191030_155620_jobs]
where rate_std_exchanged is not NULL


-->> Compensation: PERMANENT
with compensation as (select __pk as job_ext_id
	, salary_low_exchanged
	, try_parse(trim('£' from salary_low_exchanged) as bigint) as salary_from
	, salary_high_exchanged
	, try_parse(trim('£' from salary_high_exchanged) as bigint) as salary_to
	, fee_fixed_value_exchanged
	, fee_percentage
	, case when fee_percentage < 1 then fee_percentage * 100
		else fee_percentage end as percentage_annual
	, try_parse(trim('£' from rate_std_exchanged) as bigint) as pay_rate
	, coalesce(nullif(try_parse(trim('£' from salary_high_exchanged) as bigint), '')
		, nullif(try_parse(trim('£' from salary_low_exchanged) as bigint), '')
		, nullif(try_parse(trim('£' from rate_std_exchanged) as bigint), ''), NULL)
	as gross_annual_salary --pay_rate FINAL
from [20191030_155620_jobs]
where contract_type <> 'Temp')

select job_ext_id
, case when percentage_annual is NULL and fee_fixed_value_exchanged is not NULL then 100
	else percentage_annual end as percentage_of_annual_salary
, gross_annual_salary --Fee Fixed Value
, case when percentage_annual is not NULL and gross_annual_salary is not NULL then gross_annual_salary * percentage_annual / 100
		else fee_fixed_value_exchanged end as projected_profit
from compensation
where coalesce(nullif(gross_annual_salary, ''), nullif(fee_fixed_value_exchanged, '')) is not NULL


-->> Compensation: TEMP (CONTRACT)
with compensation as (select __pk as job_ext_id
	, salary_low_exchanged
	, try_parse(trim('£' from salary_low_exchanged) as bigint) as salary_from
	, salary_high_exchanged
	, try_parse(trim('£' from salary_high_exchanged) as bigint) as salary_to
	, fee_fixed_value_exchanged
	, fee_percentage
	, case when fee_percentage < 1 then fee_percentage * 100
		else fee_percentage end as percentage_annual
	, try_parse(trim('£' from rate_std_exchanged) as bigint) as pay_rate
	, coalesce(nullif(try_parse(trim('£' from salary_high_exchanged) as bigint), '')
		, nullif(try_parse(trim('£' from salary_low_exchanged) as bigint), '')
		, nullif(try_parse(trim('£' from rate_std_exchanged) as bigint), ''), NULL)
	as gross_annual_salary --pay_rate FINAL
from [20191030_155620_jobs]
where contract_type = 'Temp')

select job_ext_id
, gross_annual_salary --Fee Fixed Value
, pay_rate
, salary_from
, salary_to
--Default working hours for contract
, 8 as working_hours_per_day
, 5 as working_days_per_week
, 40 working_hours_per_week
, 22 working_days_per_month
, 4 working_weeks_per_month
from compensation
where coalesce(nullif(pay_rate, ''), nullif(salary_from, ''), nullif(salary_to, '')) is not NULL
