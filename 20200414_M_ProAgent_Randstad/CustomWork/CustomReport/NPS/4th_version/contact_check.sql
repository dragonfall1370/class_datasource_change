select *
from position_agency_consultant
where position_id in (250972, 251621, 251213, 250409, 250347) --251621

select id, candidate_id, position_description_id, interview1_date
from position_candidate
where candidate_id = 319809

select id, candidate_id, position_description_id, interview1_date
from position_candidate
where id in (978557, 977817)


--CHECK CONTACT > JOB OWNER
with team as (select id
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
	)
	
--[NEW CF] Contact NPS
, con_nps as (select a.additional_id as contact_id
	, a.form_id
	, a.field_id
	, a.field_value
	--, cf.join_field_translate as cand_nps
	from additional_form_values a
	--left join (select * from custom_field where join_field_id = 11382) cf on cf.join_field_value = a.field_value
	where field_id = 11382
	--and field_value <> '' --removed conditions if any
	and field_value = '1' --No NPS should be excluded
	)

select pc.id
, pc.position_description_id
, pc.candidate_id
, pc.interview1_date
, pc.placed_date
, pd.contact_id
, c.first_name
, c.last_name
, c.email
, pac.user_id
, ua.name as user_name
, ut.team_group_id
, ut.team_name
, row_number() over(partition by contact_id order by case when nullif(ut.nps_events, '') is not NULL then 1 else 0 end desc, ua.name) as rn
, ut.nps_events
from position_candidate pc
left join position_description pd on pd.id = pc.position_description_id
left join contact c on c.id = pd.contact_id
left join position_agency_consultant pac on pac.position_id = pd.id
left join user_team ut on ut.user_id = pac.user_id
left join user_account ua on ua.id = ut.user_id
where 1=1
and pd.id in (select position_id from team_group_position where team_group_id in (1125, 1124))
and nullif(c.email, '') is not NULL --email is not blank
and c.deleted_timestamp is NULL
and c.active in (0, 1) --status: active or passive
--and status in (1) --only MET candidates
and c.id not in (select contact_id from con_nps)
--and c.id = 29970 --check contact IDs
--and (interview1_date + interval '12 days')::date = now()::date --Event 4 | NPS4
and (placed_date + interval '12 days')::date = now()::date --Event 5 | NPS5