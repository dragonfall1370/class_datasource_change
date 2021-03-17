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

select kc.id
, kc.kpi_category_id
, kct.title
, kc.kpi_library_id
, kl.kpi_name
from kpi_criteria kc
left join kpi_library kl on kl.id = kc.kpi_library_id
left join kpi_category kct on kct.id = kc.kpi_category_id
where kc.kpi_library_id = 34

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
	) --select * from user_team_nps where user_id in (29018, 29235, 28961)


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


--KPI library in 'CA - NCAD' or 'NCAD' | updated 20210303
, kpi_lib_name as (select *
	from kpi_library_alt_name
	where 1=1
	--and alt_name ilike '%CAD%'
	and alt_name in ('CA - NCAD', 'NCAD')
	and language = 'en')

, kpi_action as (select *
	from kpi_library
	where id in (select kpi_library_id from kpi_lib_name)
	) --select * from kpi_action

, cand_activity as (select id as activity_id
		, candidate_id
		, insert_timestamp as lastest_activity_date
		, row_number() over(partition by candidate_id order by insert_timestamp asc) rn --changed req on 2020-10-30
		, kpi_action
		from activity
		where 1=1
		and candidate_id > 0
		and kpi_action in (select id::text from kpi_action)
		) --select * from cand_activity

--ONLY CANDIDATES WITH OWNERS (multiple owners)
, cand_owner as (select id cand_id
	, candidate_owner_json
	, (json_array_elements(candidate_owner_json::json)->>'ownerId')::int as ownerId
	, first_name
	, last_name
	, first_name_kana
	, last_name_kana
	, email as primary_email
	, case status
		when 1 then 'MET'
		when 2 then 'NOT MET'
		else NULL end as met_notmet
	, case active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
	from candidate
	where candidate_owner_json is not NULL and candidate_owner_json <> '[]'
	and deleted_timestamp is NULL
	and active in (0, 1) --status: active or passive
	and status in (1) --only MET candidates
	and id not in (select candidate_id from cand_nps) --exclude candidate if NO NPS is marked
	)	--select * from cand_owner

--JOIN WITH USERS WITH EXISTING NPS CODE AND IN ALPHABETICAL ORDER IF MULTIPLE OWNERS
, cand_owner_rn as (select co.*
	, ua.email
	, ua.name
	, ua.nps_events
	, ua.user_id
	--, ua2.email
	--, ua2.name
	, row_number() over(partition by cand_id order by case when nullif(nps_events, '') is not NULL then 1 else 0 end desc, ua.name) as rn
	from cand_owner co
	left join (select distinct user_id, email, name, nps_events, team_name from user_team_nps) ua on ua.user_id = co.ownerId --get distinct users with existing nps codes
	--left join (select distinct id, email, name from user_account) ua2 on ua2.id = co.ownerId
	) --select * from cand_owner_rn where cand_id = 116647


--MAIN SCRIPT
select distinct cand_id::text as "Candidate_ID"
--, first_name as "Candidate First Name"
--, last_name as "Candidate Last Name"
--, first_name_kana as "Candidate First name (kana / romaji)"
--, last_name_kana as "Candidate Last name (kana / romaji)"
, primary_email as "email"
--, cand_brand.brand as "Candidate Brand"
--, ca.lastest_activity_date as "Candidate Activity Date"
--, met_notmet as "Candidate Met / Not Met"
--, active as "Candidate Status"
--, c.ownerId
, nps.code_split as "NPS_Branch_Code"
--, nps.team_name --testing team name
--, nps.name --user_name
--, nps.rn
from cand_owner_rn c
join cand_brand on cand_brand.candidate_id = c.cand_id
join (select * from cand_activity where rn=1) ca on ca.candidate_id = c.cand_id --Latest activity date
join (select * from user_team_nps where rn=1) nps on nps.user_id = c.user_id --NPS Event 1 with rn=1
where 1=1
and c.rn=1 --get only 1 candidate with valid nps codes
and (ca.lastest_activity_date + interval '1 day')::date = now()::date
--order by id --distinct and order should not be in the same query