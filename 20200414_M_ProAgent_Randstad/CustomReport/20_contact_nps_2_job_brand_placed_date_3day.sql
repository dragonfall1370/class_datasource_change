with job_brand as (select tgc.position_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_position tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125) --1125-Professionals | 1123-CA
	)

--Job with >1 job app	
, job_job_app as (select position_description_id
		, count(id) as job_app_count
		from position_candidate
		where position_description_id in (select position_id from job_brand)
		group by position_description_id
		having count(*) > 1)
		
--PLACEMENT ACTIVE
, placements as (select id
	, position_candidate_id
	, insert_timestamp
	, valid
	, row_number() over(partition by position_candidate_id order by valid desc, id desc) as rn
	from offer)
		
--PLACED jobs
, job_placed as (select pc.id as job_app_id
		, p.id placement_id
		, pc.candidate_id
		, pc.position_description_id
		, pc.placed_date
		, row_number() over(partition by pc.position_description_id order by pc.placed_date asc) as rn --oldest
		, pd.name as job_title
		, pd.company_id
		, c.name as company_name
		, pd.contact_id
		, con.first_name
		, con.last_name
		, con.email as contact_primary_email
		from position_candidate pc
		--join (select * from position_description where id in (select position_description_id from job_job_app)) pd on pd.id = pc.position_description_id --remove the filter
		join position_description pd on pd.id = pc.position_description_id
		left join company c on c.id = pd.company_id
		left join contact con on con.id = pd.contact_id
		left join placements p on p.position_candidate_id = pc.id
		where 1=1
		and pc.status >= 300 --higher than PLACED
		) --select * from job_placed where placed_date::date = '2020-08-02'::date

--Oldest placed date within 3 days
, job_placed_3day as (select *
		from job_placed
		where 1=1
		and rn=1
		and (placed_date + interval '3 days')::date = now()::date
		) --select * from job_placed_3day

--MAIN SCRIPT
select c.id as contact_id
, c.first_name
, c.last_name
, c.first_name_kana
, c.last_name_kana
, c.email
, case active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
, j.position_description_id
, j.job_title
, 'Professionals' as job_brand
, j.company_id
, j.company_name
, j.job_app_id
, j.placement_id
, j.placed_date
from job_placed_3day j
join contact c on j.contact_id = c.id
where 1=1
and nullif(c.email, '') is not NULL --email is not blank
and c.deleted_timestamp is NULL
and c.active in (0, 1) --status active or passive