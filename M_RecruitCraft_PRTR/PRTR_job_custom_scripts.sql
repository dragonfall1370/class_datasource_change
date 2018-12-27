select concat('PRTR',vac_id) as JobExtID
, 2 as SalaryType --1 annual | 2 monthly
/* OLD FUNCTION */
--, case when isnumeric(vac_salary) = 1 then replace(left(vac_salary,len(vac_salary)-charindex('.',reverse(vac_salary))),',','')
--	else NULL end as SalaryFrom
--, case when isnumeric(vac_salary) = 1 then replace(left(vac_salary,len(vac_salary)-charindex('.',reverse(vac_salary))),',','')
--	else NULL end as PresentSalaryRate
--, case when isnumeric(vac_salary_max) = 1 then replace(left(vac_salary_max,len(vac_salary_max)-charindex('.',reverse(vac_salary_max))),',','')
--	else NULL end as SalaryTo
/* TRY NEW FUNCTION */
, TRY_PARSE(vac_salary AS int) as SalaryFrom
, TRY_PARSE(vac_salary AS int) as PresentSalaryRate --VC Salary / rate per month
, vac_salary_max
, TRY_PARSE(vac_salary_max AS int) as SalaryTo
, 12 as months_per_year
, TRY_PARSE(vac_salary_max AS int) * 12 as pay_rate --VC Annual Salary
from vacancies.Vacancies
where (isnumeric(vac_salary) = 1 or isnumeric(vac_salary_max) = 1) --36365
--and not exists (select vac_id from placements.Placements p where p.vac_id = vacancies.Vacancies.vac_id) --Mapping required only for [Jobs that haven't placed yet]
order by vac_id