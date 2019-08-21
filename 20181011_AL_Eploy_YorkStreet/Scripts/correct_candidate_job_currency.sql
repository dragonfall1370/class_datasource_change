select * from contact where email = 'iain.franklin@source8.com'

select id, annual_salary_to, annual_salary_from, currency_type from position_description
where id = 32382

select id, annual_salary_to, annual_salary_from, currency_type, pay_rate from compensation

select * from position_fee_fact

-- update position_description
-- set currency_type = 'pound'

-- update compensation
-- set currency_type = 'pound'

select id, current_salary, desire_salary , currency_of_salary, currency_type from candidate

-- update candidate
-- set currency_type = 'pound'