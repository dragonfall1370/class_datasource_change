---
with com_industry as (select c.company_id
	, c.industry_id
	, v.parent_id
	, v.id
	, v.name
	, c.insert_timestamp
	from company_industry c
	left join vertical v on v.id = c.industry_id
	where c.insert_timestamp > '2020-04-07' --limit the company industry
	--and company_id = 54609
	)

, job_ind as (select id position_id
	, NULL parent_id
	, vertical_id industry_id
	, pd.external_id
	, pd.company_id, pd.company_id_bkup
	, row_number() over(partition by id order by vertical_id asc) - 1 as seq
	, current_timestamp insert_timestamp
	from position_description pd
	where external_id ilike 'JOB%'
	and deleted_timestamp is NULL --140966
	and vertical_id is not NULL --139576 
	
	UNION ALL
	select pd.id position_id
	, pd.vertical_id parent_id
	, ci.industry_id
	, pd.external_id
	, pd.company_id, pd.company_id_bkup
	, row_number() over(partition by pd.id, pd.vertical_id order by industry_id asc) - 1 as seq
	, current_timestamp insert_timestamp
	from position_description pd
	left join com_industry ci on ci.company_id = pd.company_id_bkup and ci.parent_id = pd.vertical_id
	where 1=1
	and pd.external_id ilike 'JOB%'
	and pd.deleted_timestamp is NULL --140966
	and pd.vertical_id is not NULL --139576
	and ci.parent_id is not NULL
	) 

insert into position_description_industry (position_id, industry_id, parent_id, seq, insert_timestamp)
select position_id
, industry_id
, parent_id
, seq
, insert_timestamp
from job_ind
order by position_id, seq