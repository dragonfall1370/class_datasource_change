--CONTACT/ CANDIDATE 'Hires Contractor (IC)' as Industry
select ts2_contact_c as candidate_id
, 'Hires Contractor (IC)' as industry
, now() as insert_timestamp
from ts2_skill_c
where lower(ts2_skill_name_c) = lower('Hires Contractor (IC)')