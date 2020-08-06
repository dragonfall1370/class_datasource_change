--EXISTING CANDIDATES
select id
from candidate
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CDT%') --69880

--
select max(id), count(id)
from team_group_candidate --max=343244 | count=171839

select *
from team_group_candidate


--| MAIN SCRIPT <BRAND> |--
insert into team_group_candidate (candidate_id, team_group_id, insert_timestamp)
select id as candidate_id
, 1105 as team_group_id --BRAND
, current_timestamp as insert_timestamp
from candidate
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CDT%')
on conflict on constraint team_group_candidate_team_group_id_candidate_id_key
	do nothing
	

-->NOT USING FOR CANDIDATE BRANCH
--| MAIN SCRIPT <BRANCH> |--
insert into team_group_candidate (candidate_id, team_group_id, insert_timestamp)
select id as candidate_id
, 1149 as team_group_id --BRANCH
, current_timestamp as insert_timestamp
from candidate
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CDT%')
on conflict on constraint team_group_candidate_team_group_id_candidate_id_key
	do nothing