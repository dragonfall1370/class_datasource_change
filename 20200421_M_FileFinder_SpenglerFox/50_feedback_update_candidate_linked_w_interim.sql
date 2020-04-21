--Un-archive all candidates links with Interim jobs --regardless of time filter
with job as (select id, position_description_id, candidate_id
			 from position_candidate
			 where position_description_id in (
				select id--, name
				from position_description
				where name ilike '%interim%')
			 ) --204 candidates 
--select distinct candidate_id from job --201 unique candidates

/* AUDIT CHECK
select id, insert_timestamp, deleted_timestamp
from candidate
where id in (select distinct candidate_id from job) --201 candidates
and deleted_timestamp is not NULL --63 candidates
*/

update candidate
set deleted_timestamp = NULL
where id in (select distinct candidate_id from job)
and deleted_timestamp is not NULL