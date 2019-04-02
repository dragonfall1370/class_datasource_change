----skill = 1015, industry = 1005
------Organisation
----------Industry
with test as (select distinct code from search_code where code_type = 1005)

select a.code,b.description from test a left join lookup b on a.code = b.code where b.code_type = 1005
---------------

with test as (select * 
from lookup where code_type = 1005)
,test2 as (select  code_type,iif(right(description,3) = 'OLD','',description) as description, ROW_NUMBER() over ( partition by description order by code_type) as name_num from test)
,test5 as (select getdate() as insert_timestamp,*,ROW_NUMBER() over ( partition by code_type order by code_type ) as rn from test2 where description <> '' and name_num = 1)


,test3 as (select * from search_code where code_type = 1005 and organisation_ref <> '')

,test4 as (select a.organisation_ref, b.description from test3 a left join lookup b on a.code = b.code where b.code_type = 1005)

,test6 as (select a.organisation_ref,b.rn from test4 a left join test5 b on a.description = b.description)

,test7 as (select organisation_ref as company_id, rn as industry_code from test6 where rn is not null)

,test8 as (select *,ROW_NUMBER() over (partition by company_id, industry_code order by company_id) as num from test7)

select * from test8 where num = 1




---------- skills-

with test as (select * from search_code where code_type = 1015 and organisation_ref <> '')

,test2 as (select a.organisation_ref, b.description from test a left join lookup b on a.code = b.code where b.code_type = 1015)

SELECT organisation_ref as 'company_id', string_agg(description,',') as 'skills' from test2 group by organisation_ref




