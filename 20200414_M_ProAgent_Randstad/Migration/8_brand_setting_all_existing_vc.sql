--| COMPANY BRAND |--
insert into team_group_company (company_id, team_group_id, insert_timestamp)
select id as company_id
, 1125 as team_group_id --1125 | Professionals
, current_timestamp as insert_timestamp
from company
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CPY%')
on conflict on constraint team_group_company_team_group_id_company_id_key
	do nothing


--| CONTACT BRAND |--
insert into team_group_contact (contact_id, team_group_id, insert_timestamp)
select id as contact_id
, 1125 as team_group_id --1125 | Professionals
, current_timestamp as insert_timestamp
from contact
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'REC%')
on conflict on constraint team_group_contact_team_group_id_contact_id_key
	do nothing


--| JOB BRAND |--
insert into team_group_position (position_id, team_group_id, insert_timestamp)
select id as position_id
, 1125 as team_group_id --1125 | Professionals
, current_timestamp as insert_timestamp
from position_description
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'JOB%')
on conflict on constraint team_group_position_team_group_id_position_id_key
	do nothing


--| CANDIDATE BRAND |--
insert into team_group_candidate (candidate_id, team_group_id, insert_timestamp)
select id as candidate_id
, 1125 as team_group_id --1125 | Professionals
, current_timestamp as insert_timestamp
from candidate
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CDT%')
on conflict on constraint team_group_candidate_team_group_id_candidate_id_key
	do nothing