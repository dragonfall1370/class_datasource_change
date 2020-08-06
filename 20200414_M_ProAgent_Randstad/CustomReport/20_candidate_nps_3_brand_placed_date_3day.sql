with cand_brand as (select tgc.candidate_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1123)
	)

--PLACEMENT ACTIVE
, placements as (select id
	, position_candidate_id
	, insert_timestamp
	, valid
	, row_number() over(partition by position_candidate_id order by valid desc, id desc) as rn
	from offer)
	
--PLACED candidates
, cand_placed as (select pc.id as job_app_id
		, p.id placement_id
		, pc.candidate_id
		, pc.placed_date
		, row_number() over(partition by pc.candidate_id order by pc.placed_date desc) as rn --get the latest placed_date
		, pd.name as job_title
		, pd.company_id
		, c.name as company_name
		, pd.contact_id
		, con.first_name
		, con.last_name
		, con.email as contact_primary_email
		from position_candidate pc
		left join position_description pd on pd.id = pc.position_description_id
		left join company c on c.id = pd.company_id
		left join contact con on con.id = pd.contact_id
		left join (select * from placements where rn=1) p on p.position_candidate_id = pc.id
		where 1=1
		and pc.status >= 300 --higher than PLACED
		) --select * from cand_placed

--Placed date within the date of 3 days ago
, cand_placed_3day as (select *
		from cand_placed
		where 1=1
		and rn=1
		and (placed_date + interval '3 days')::date = now()::date
		) --select * from cand_placed_3day

--MAIN SCRIPT
select id
, c.first_name
, c.last_name
, c.first_name_kana
, c.last_name_kana
, c.email as primary_email
, cand_brand.brand
, case active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
, case status
		when 1 then 'MET'
		when 2 then 'NOT MET'
		else NULL end as met_notmet
, ca.job_title
, ca.company_id
, ca.company_name
, ca.contact_id
, ca.first_name contact_first_name
, ca.last_name contact_last_name
, contact_primary_email
, ca.job_app_id
, ca.placement_id
, ca.placed_date
from candidate c
join cand_brand on cand_brand.candidate_id = c.id
join cand_placed_3day ca on ca.candidate_id = c.id
where 1=1
and c.deleted_timestamp is NULL
and c.active in (0, 1) --status active or passive
--and c.status in (1) --only MET candidates