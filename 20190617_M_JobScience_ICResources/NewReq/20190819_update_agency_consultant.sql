--BACKUP POSITION EXTENSION
select *
into mike_position_extension_bkup_20190819
from position_extension --125916 rows

--FUNCTION: public.fn_update_agency_consultant()

--MAIN SCRIPT
with consultant as (select position_id
	, array_agg(user_id) as job_agency_consultant_ids
	, string_agg(name, ', ') as job_agency_consultant_names
	FROM (
		SELECT pac.position_id, pac.user_id, ua.name 
		FROM position_agency_consultant pac
		LEFT JOIN user_account ua ON user_id = ua.id 
		) temp
	GROUP BY position_id --62633 rows
)

update position_extension
set agency_consultant_ids = job_agency_consultant_ids, agency_consultant_names = job_agency_consultant_names
from consultant c
where c.position_id = position_extension.position_id

--BACKUP CANDIDATE EXTENSION
select *
into mike_candidate_extension_bkup_20190819
from candidate_extension --107940 rows

--FUNCTION: public.fn_refresh_candidate_owner()

--MAIN SCRIPT: CANDIDATE
with consultant as (SELECT candidate_id
	, array_agg(temp.owner_id::int) as cand_owner_ids
	, string_agg(ua.name, ', ') as cand_owner_names
	FROM (
		(SELECT id as candidate_id
		, json_array_elements(candidate_owner_json::json)->>'ownerId' AS owner_id
		FROM candidate) temp
		LEFT JOIN user_account ua ON temp.owner_id::integer = ua.id
		)
	group by candidate_id
)

UPDATE candidate_extension
SET candidate_owner_ids = cand_owner_ids, candidate_owner_names = cand_owner_names
from consultant c
WHERE c.candidate_id = candidate_extension.candidate_id