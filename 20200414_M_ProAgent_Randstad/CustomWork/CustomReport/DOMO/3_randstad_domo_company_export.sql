with com_brand as (select tgc.company_id
	--, tgc.team_group_id
	, string_agg(tg.name, ',') as brand
	from team_group_company tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	--where team_group_id in (1125, 1123)
	group by tgc.company_id
	)
	
, job_entry as (select company_id
	, max(insert_timestamp) as job_entry_date
	from position_description
	group by company_id
	)
	
, com_owner as (select id
	, unnest(string_to_array(company_owners, ','))::int as owner_id
	from company
	)
	
, com_owners as (select c.id
	, string_agg(ua.name,',') as com_owners
	from com_owner c
	left join user_account ua on ua.id = c.owner_id
	group by c.id
	) --select * from com_owners
	
, com_priority as (select company_id
	, case 
		when max(board) = 2 then 'Potential Clients'
		when max(board) = 3 then 'Primary Clients'
		when max(board) = 4 then 'Strategic Clients'
		else 'Companies' end as com_priority
	, min(insert_timestamp) as com_client_date --get the oldest date for contact
	from contact
	group by company_id
	) --select * from com_priority
	
, com_activity as (select company_id
	, max(insert_timestamp) as last_activity_comment
	from activity_company
	group by company_id
	)
	
, placement_profit as (select pd.company_id
	, sum(coalesce(o.projected_profit, 0))
	, round(sum(coalesce(o.projected_profit, 0)::int), 1) as placement_profit --sum up based on job
	from offer o
	left join position_candidate pc on pc.id = o.position_candidate_id
	left join position_description pd on pd.id = pc.position_description_id
	where 1=1
	and pc.status > 300 --placements
	group by pd.company_id
	)

	
select 
	c.id as "Company ID"
	, cb.brand as "Brand"
	, c.insert_timestamp as "Company entry date"
	, cp.com_client_date as "Became client date"
	, j.job_entry_date as "Time to first job entry"
	, coalesce(DATE_PART('day', current_timestamp - ce.last_activity_date), 0) as "Rotting time (time without contact)"
	, co.com_owners as "Client owner"
	, cp.com_priority as "Client relationship strength (primary, strategic, etc...)"
	, ca.last_activity_comment as "Last comment"
	, ce.last_activity_date as "Last activity date"
	, coalesce(pp.placement_profit, 0) as "Total client fee amount (GP)"
from company c
left join com_brand cb on cb.company_id = c.id
left join job_entry j on j.company_id = c.id
left join company_extension ce on ce.company_id = c.id
left join com_owners co on co.id = c.id
left join com_priority cp on cp.company_id = c.id
left join com_activity ca on ca.company_id = c.id
left join placement_profit pp on pp.company_id = c.id
where 1=1
and c.deleted_timestamp is NULL
--and c.insert_timestamp > '2020-08-01'
order by c.id