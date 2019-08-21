--CANDIDATE PRESENT SALARY
-->> From Vincere
select id
, salary_type --1: annual | 2: monthly
, months_per_year
, present_salary_rate --monthly salary
, current_salary --Form: present_salary_rate*months_per_year (default: 12 months)
from candidate

-->> From PRTR DB
select concat('PRTR',cn_id) as CandidateExtID
, 2 as salary_type
, 12 as months_per_year
, cn_present_salary as present_salary_rate
--, cn_present_salary*12 as current_salary --salary is over 8 digits | update in Vincere
from candidate.Candidates
where can_type = 1
order by cn_id