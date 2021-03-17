with job_brand as (select tgc.position_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_position tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1124) --1125-Professionals | 1124-障がい者 (Challenged)
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
		, pd.id as job_id
		, pc.candidate_id
		, pc.position_description_id
		, pc.interview1_date
		, row_number() over(partition by pc.position_description_id order by pc.interview1_date asc) as rn --oldest interview date | changed req on 2020-10-30
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
		and (interview1_date + interval '3 days')::date = now()::date --main conditions
		--and (interview1_date + interval '12 days')::date = now()::date --test export date
		) --select * from job_3_day_int
		
--GET ONLY TEAM WITH NPS EVENT
, team as (select id
	, name as team_name
	, description
	, substring(description, position('[' in description) + 1, position(']' in description) - 2) as nps_events
	from team_group
	where 1=1
	--and id = 1055 --test team
	and group_type = 'TEAM'
	and (position('[' in description) > 0 and position(']' in description) > 0) --condition to check NPS events
	) --select * from team_group_user where user_id = 29707 | select * from team_group where id = 1125

--ORDER USERS BY ALPHABETICAL TEAM WITH EXISTING NPS CODES
, user_team as (select tgu.id
	, tgu.user_id
	, tgu.team_group_id
	, tg.team_name
	, tg.nps_events
	, row_number() over(partition by tgu.user_id order by tg.team_name) as team_rn --order by alphabetical order group name
	from team_group_user tgu
	join (select * from team) tg on tg.id = tgu.team_group_id --get only team with NPS code
	) --select * from user_team where user_id = 29707

--SPLIT AND GET NPS CODES FOR FIRST ALPHABETICAL TEAM
, user_team_nps as (select ut.user_id
	, ua.name
	, ua.email
	, ut.team_group_id
	, ut.team_name
	, ut.nps_events
	, ut.team_rn
	, a.code_split
	, a.rn
	from user_team ut
	join (select id, email, name from user_account) ua on ua.id = ut.user_id
	, unnest(string_to_array(nps_events, ',')) WITH ORDINALITY AS a(code_split, rn)
	where 1=1
	and ut.team_rn = 1 --get the first alphabetical team if multiple
	) --select * from user_team_nps where user_id in (29707)


--Custom field
, custom_field as (select cffv.form_id as join_form_id
	, cffv.field_id as join_field_id
	, cfl.translate as join_field_translate
	, cffv.field_value as join_field_value
	from configurable_form_language cfl
	left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
	where 1=1
	and cfl.language = 'en' --input language
	and cffv.field_id = 11382 --input field
	order by cffv.field_value::int)

--[NEW CF] Contact NPS
, con_nps as (select a.additional_id as contact_id
	, a.form_id
	, a.field_id
	, a.field_value
	, cf.join_field_translate as cand_nps
	from additional_form_values a
	left join (select * from custom_field where join_field_id = 11382) cf on cf.join_field_value = a.field_value
	where field_id = 11382
	--and field_value <> '' --removed conditions if any
	and field_value = '1' --No NPS should be excluded
	) --select * from con_nps

--CHANGE FROM CONTACT OWNER TO JOB OWNERS (multiple owners)
, con_owner as (select c.id as contact_id
	--, json_array_elements(contact_owners::json) as ownerId
	--, unnest(contact_owner_ids::varchar[])::int as ownerId
	, c.contact_owner_ids
	, c.first_name
	, c.last_name
	, c.first_name_kana
	, c.last_name_kana
	, c.email
	, case c.active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
	, ji.job_id
	, pac.user_id as job_owner --Job owners
	from job_3_day_int ji --using filter table instead
	join contact c on c.id = ji.contact_id
	join position_agency_consultant pac on pac.position_id = ji.job_id
	where 1=1
	and nullif(c.email, '') is not NULL --email is not blank
	and c.deleted_timestamp is NULL
	and c.active in (0, 1) --status: active or passive
	--and status in (1) --only MET candidates
	and c.id not in (select contact_id from con_nps) --exclude contact if NO NPS is marked
	) --select * from con_owner
	
	
--JOIN JOB OWNERS WITH EXISTING USERS W/ NPS CODE AND IN ALPHABETICAL ORDER IF MULTIPLE OWNERS
, con_owner_rn as (select co.*
	--, ua.email
	, ua.name
	, ua.nps_events
	, ua.user_id
	--, ua2.email
	--, ua2.name
	, row_number() over(partition by contact_id order by case when nullif(nps_events, '') is not NULL then 1 else 0 end desc, ua.name) as rn
	from con_owner co
	left join (select distinct user_id, email, name, nps_events, team_name from user_team_nps) ua on ua.user_id = co.job_owner --get distinct users with existing nps codes | using join only
	--left join (select distinct id, email, name from user_account) ua2 on ua2.id = co.ownerId
	) --select * from con_owner_rn where contact_id = 15849 --select * from user_account where id = 29707
	
--MAIN SCRIPT
select distinct c.contact_id::text as "Contact_ID"
--, c.first_name as "Contact First Name"
--, c.last_name as "Contact Last Name"
--, c.first_name_kana as "Contact First name (kana / romaji)"
--, c.last_name_kana as "Contact Last name (kana / romaji)"
, c.email as "email"
--, c.active as "Contact Status"
--, j.position_description_id as "Job ID"
--, j.job_title as "Job title"
--, 'Professionals' as "Job Brand"
--, j.company_id as "Job Company ID"
--, j.company_name as "Job Company Name"
--, j.job_app_id as "Job Application ID"
--, j.interview1_date as "Job Application 1st int. date"
--, c.ownerId
, nps.code_split as "NPS_Branch_Code"
--, nps.team_name --test team name
--, nps.name --user_name
--, nps.rn
from con_owner_rn c
--join job_3_day_int j on j.contact_id = c.contact_id
join (select * from user_team_nps where rn=4) nps on nps.user_id = c.user_id --NPS Event 4 with rn=4
where 1=1
and c.rn=1 --get the owner in the alphabetical order
--order by c.contact_id