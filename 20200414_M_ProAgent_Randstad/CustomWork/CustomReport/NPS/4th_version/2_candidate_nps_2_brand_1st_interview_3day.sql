with cand_brand as (select tgc.candidate_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1123)
	)

/* 
ACTIVE VALUE: active=0-passive 1-active 2-donotcontact 3-blacklist 
MET-NOT MET: status=1-met 2-notmet
*/
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
	)

--ORDER USERS BY ALPHABETICAL TEAM WITH EXISTING NPS CODES
, user_team as (select tgu.id
	, tgu.user_id
	, tgu.team_group_id
	, tg.team_name
	, tg.nps_events
	, row_number() over(partition by tgu.user_id order by tg.team_name) as team_rn --order by alphabetical order group name
	from team_group_user tgu
	join (select * from team) tg on tg.id = tgu.team_group_id --get only team with NPS code
	) --select * from user_team where user_id = 28964

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
	) --select * from user_team_nps where user_id in (29093)
	
	
--Custom field
, custom_field as (select cffv.form_id as join_form_id
	, cffv.field_id as join_field_id
	, cfl.translate as join_field_translate
	, cffv.field_value as join_field_value
	from configurable_form_language cfl
	left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
	where 1=1
	and cfl.language = 'en' --input language
	--and cffv.field_id = 11312 --input field
	order by cffv.field_value::int)

--[NEW CF] Candidate NPS
, cand_nps as (select a.additional_id as candidate_id
	, a.form_id
	, a.field_id
	, a.field_value
	, cf.join_field_translate as cand_nps
	from additional_form_values a
	left join (select * from custom_field where join_field_id = 11381) cf on cf.join_field_value = a.field_value
	where field_id = 11381
	--and field_value <> '' --removed conditions if any
	and field_value = '1' --No NPS should be excluded
	) --select * from cand_nps


--Candidate with >1 job app	
, cand_job_app as (select candidate_id
		, count(id) as job_app_count
		from position_candidate
		group by candidate_id
		having count(*) > 1)
		
--Candidate with >1 job app and setting 1st interview date
, cand_1st_int_date as (select pc.id as job_app_id
		, pc.candidate_id
		, pc.interview1_date
		, row_number() over(partition by pc.candidate_id order by pc.interview1_date asc) as rn --get oldest 1st int date --changed req on 2020-10-30
		, pd.id as job_id
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
		--and pc.candidate_id in (select candidate_id from cand_job_app) --the filter will not be applied
		and pc.interview1_date is not NULL
		) --select * from cand_1st_int_date where candidate_id = 319809

--Job app info with interview1_date within 3 days ago
, cand_3_day_int as (select *
		from cand_1st_int_date
		where 1=1
		and rn = 1 --get the latest 1st interview date
		and (interview1_date + interval '3 days')::date = (now())::date --main condition
		--and (interview1_date + interval '13 days')::date = (now())::date --test on export date
		) --select * from cand_3_day_int where candidate_id = 319809


--CHANGE FROM CANDIDATE OWNERS TO JOB OWNERS (multiple owners)
, cand_owner as (select c.id cand_id
	, c.candidate_owner_json
	, c.first_name
	, c.last_name
	, c.first_name_kana
	, c.last_name_kana
	, c.email as primary_email
	, case c.status
		when 1 then 'MET'
		when 2 then 'NOT MET'
		else NULL end as met_notmet
	, case c.active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
	, pc.id as job_app_id
	, pc.position_description_id
	, pac.user_id as job_owner--Job owners
	from cand_3_day_int ci
	join candidate c on c.id = ci.candidate_id
	--left join position_candidate pc on pc.candidate_id = c.id
	join position_candidate pc on pc.id = ci.job_app_id --using job app id instead
	join position_agency_consultant pac on pac.position_id = ci.job_id
	where 1=1
	and c.deleted_timestamp is NULL
	and c.active in (0, 1) --status: active or passive
	and c.status in (1) --only MET candidates
	and c.id not in (select candidate_id from cand_nps) --exclude candidate if NO NPS is marked
	) --select * from cand_owner where cand_id = 319809

	
--JOIN JOB OWNERS WITH EXISTING USERS W/ NPS CODE AND IN ALPHABETICAL ORDER IF MULTIPLE OWNERS
, cand_owner_rn as (select co.*
	, ua.email
	, ua.name
	, ua.nps_events
	, ua.user_id
	--, ua2.email
	--, ua2.name
	, row_number() over(partition by cand_id order by case when nullif(nps_events, '') is not NULL then 1 else 0 end desc, ua.name) as rn
	from cand_owner co
	left join (select distinct user_id, email, name, nps_events, team_name from user_team_nps) ua on ua.user_id = co.job_owner --get distinct users with existing nps codes
	--left join (select distinct id, email, name from user_account) ua2 on ua2.id = co.ownerId
	) --select * from cand_owner_rn where cand_id = 319809
	
--MAIN SCRIPT
select distinct cand_id::text as "Candidate_ID"
--, c.first_name as "Candidate First Name"
--, c.last_name as "Candidate Last Name"
--, first_name_kana as "Candidate First name (kana / romaji)"
--, last_name_kana as "Candidate Last name (kana / romaji)"
, primary_email as "email"
--, cand_brand.brand as "Candidate Brand"
--, ca.interview1_date as "Candidate Activity Date"
--, met_notmet as "Candidate Met / Not Met"
--, active as "Candidate Status"
--, ca.job_id as "Job ID"
--, ca.job_title as "Job Title"
--, ca.company_id as "Job Company ID"
--, ca.company_name as "Job Company Name"
--, ca.contact_id as "Job Contact ID"
--, ca.first_name "Job Contact First Name"
--, ca.last_name "Job Contact Last Name"
--, contact_primary_email as "Job Contact Primary Email"
--, ca.job_app_id as "Job Application ID"
--, ca.interview1_date as "Job Application1st int. date"
--, c.ownerId
, nps.code_split as "NPS_Branch_Code"
--, nps.team_name --testing team name
--, nps.name --user_name
--, nps.rn
from cand_owner_rn c
join cand_brand on cand_brand.candidate_id = c.cand_id
--join cand_3_day_int ca on ca.candidate_id = c.cand_id
join (select * from user_team_nps where rn=2) nps on nps.user_id = c.user_id --NPS Event 2 with rn=2
where 1=1
and c.rn=1 --get only 1 candidate with valid nps codes
--and c.cand_id = 251205