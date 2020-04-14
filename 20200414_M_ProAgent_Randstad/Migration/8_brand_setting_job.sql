--EXISTING JOBS
select id
from position_description
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'JOB%') --22903

--
select max(id), count(id)
from team_group_position --max=281362 | count=140977

select *
from team_group_position

--| MAIN SCRIPT <BRAND> |--
insert into team_group_position (position_id, team_group_id, insert_timestamp)
select id as position_id
, 1105 as team_group_id --BRAND
, current_timestamp as insert_timestamp
from position_description
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'JOB%')
on conflict on constraint team_group_position_team_group_id_position_id_key
	do nothing
	

--| MAIN SCRIPT <BRANCH> |--
insert into team_group_position (position_id, team_group_id, insert_timestamp)
select id as position_id
, 1149 as team_group_id --BRANCH
, current_timestamp as insert_timestamp
from position_description
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'JOB%')
on conflict on constraint team_group_position_team_group_id_position_id_key
	do nothing