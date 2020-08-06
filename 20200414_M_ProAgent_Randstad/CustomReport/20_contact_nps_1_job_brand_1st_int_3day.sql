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
		
--Job with >1 job app and setting 1st interview date
, job_1st_int_date as (select pc.id as job_app_id
		, pc.candidate_id
		, pc.position_description_id
		, pc.interview1_date
		, row_number() over(partition by pc.position_description_id order by pc.interview1_date asc) as rn --oldest interview date
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
		where 1=1
		--and pc.position_description_id in (select position_description_id from job_job_app) --remove the filter
		and pc.interview1_date is not NULL
		) --select * from job_1st_int_date where interview1_date::date = '2020-08-02'::date
		
--Job with >1 job app and oldest 1st int date within 3 days
, job_3_day_int as (select *
		from job_1st_int_date
		where 1=1
		and rn = 1 --get the oldest 1st interview date
		and (interview1_date + interval '3 days')::date = now()::date 
		) --select * from job_3_day_int


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
, j.interview1_date
from job_3_day_int j
join contact c on j.contact_id = c.id
where 1=1
and nullif(c.email, '') is not NULL --email is not blank
and c.deleted_timestamp is NULL
and c.active in (0, 1) --status active or passive