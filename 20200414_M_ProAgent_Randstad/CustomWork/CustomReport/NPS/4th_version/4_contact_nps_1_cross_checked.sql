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

select pd.id, pd.contact_id, pac.user_id, ua.name, ua.email
, ut.*
from position_description pd
left join position_agency_consultant pac on pac.position_id = pd.id
left join user_account ua on ua.id = pac.user_id
left join user_team_nps ut on ut.user_id = pac.user_id
where contact_id = 70266