
-- CANDIDATE
select distinct
select DISTINCT 
        c.status
        , ct.description
from candidates c
left join (select code,description from codetables where TabName = 'Candidate Status') ct on ct.code = c.status



-- VACANCIES
select 
        DISTINCT v.status
        , ct.description
from vacancies v
left join (select code,description from codetables where TabName = 'Vac Status Code') ct on ct.code = v.status