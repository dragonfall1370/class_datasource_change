select desired_work_location_list, *
from candidate
where external_id in ('PR4', 'PR16332', 'PR6359', 'PR12523')

select *
from common_location
where desired_location_candidate_id is not NULL

--MAIN SCRIPT
update candidate
set desired_work_location_list = common_location.id
from common_location
where common_location.desired_location_candidate_id = candidate.id
and common_location.desired_location_candidate_id is not NULL
and candidate.deleted_timestamp is NULL