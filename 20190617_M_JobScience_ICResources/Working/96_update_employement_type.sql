--UPDATE JOB_TYPE FOR BOTH AS CONTRACT
/*
select id as candidate_id
, employment_type_c
, '{2}' as job_type
from contact
where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')
and employment_type_c = 'Both' --15454
*/

--Update field: desired_job_type_json in [Candidate] table
select id as candidate_id
, employment_type_c
, '[{"desiredJobTypeId":"2"}]' as job_type
from contact
where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')
and employment_type_c = 'Both' --15454