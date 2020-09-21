
with t as (
select
         pg.REFERENCE as 'candidate-externalId', pg.person_id
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
	, cp.P_PERM, cp.P_CONTR, cp.P_TEMP
	, case
	      when (cp.P_PERM  = 'Y' and cp.P_CONTR  = 'Y' and cp.P_TEMP  = 'Y') then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"},{"desiredJobTypeId":"4"}]'
	      when (cp.P_PERM  = 'Y' and cp.P_CONTR  = 'Y') then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
	      when (cp.P_PERM  = 'Y' and cp.P_TEMP  = 'Y') then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"4"}]'
	      when (cp.P_CONTR  = 'Y' and cp.P_TEMP  = 'Y') then '[{"desiredJobTypeId":"2"},{"desiredJobTypeId":"4"}]'
	      
	      when cp.P_PERM  = 'Y' then '[{"desiredJobTypeId":"1"}]'
	      when cp.P_CONTR  = 'Y' then '[{"desiredJobTypeId":"2"}]'
	      when cp.P_TEMP  = 'Y' then '[{"desiredJobTypeId":"4"}]'
	      end as 'desired_job_type_json'
from PROP_PERSON_GEN pg
left join PROP_CAND_PREF cp on pg.REFERENCE = cp.REFERENCE	
)

--select distinct desired_job_type_json from t where desired_job_type_json is not null
select * from t where desired_job_type_json is not null
--where pg.person_id = 1136371