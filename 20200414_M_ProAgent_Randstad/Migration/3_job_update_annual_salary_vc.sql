--Update pay_rate from gross_annual_salary
select id, position_id, annual_salary_from, annual_salary_to, gross_annual_salary, pay_rate
from compensation
where position_id in (select id from position_description where position_type = 1)
and pay_rate is NUll

update compensation
set pay_rate = gross_annual_salary
where position_id in (select id from position_description where position_type = 1)
and pay_rate is NULL
and gross_annual_salary > 0 --99723 rows