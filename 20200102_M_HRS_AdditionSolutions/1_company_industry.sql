--Industry List
select [companies_industry]
, trim(value) as industry
, current_timestamp as insert_timestamp
from [20191030_160039_value_lists]
cross apply string_split([companies_industry], char(11))
order by value;


--Company industry
with com_industry as (select __pk as com_ext_id
	, industry
	, trim(value) as comp_industry
	from [20191030_153350_companies]
	cross apply string_split(industry, char(11))
)

select distinct com_ext_id
, comp_industry
, current_timestamp as insert_timestamp
from com_industry
where nullif(comp_industry, '') is not NULL