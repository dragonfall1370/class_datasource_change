
DROP TABLE IF EXISTS truong_jopapp;
CREATE TABLE IF NOT EXISTS truong_jopapp as
       select 
                aj.req_id as 'positionExternalId'
              , concat('cand',aj.candidate_id) as 'candidateExternalId'
              ,case aj.status 
                     when 'applied' then 0
                     when 'submitted' then 1
                     when 'placed' then 5
                     else null end  as 'stage'
              from candidate_appliedjobs aj #where aj.candidate_id in (93377)
       union all
              select reqid, concat('cand',candid), 0 as 'stage' from short_lists
       union all
              select posid	, concat('cand',candid), 1 as 'stage' from entity_submission_roledetails
       union all
              select posid, candidate, 5 as 'stage' from placement_jobs where candidate <> ''
;


--------------
        
DROP TABLE IF EXISTS truong_jopapp_ordered;
CREATE TABLE IF NOT EXISTS truong_jopapp_ordered as
       SELECT
                         @rn := IF(@positionExternalId = positionExternalId and @candidateExternalId = candidateExternalId, @rn + 1, 1) AS rn,
                         @positionExternalId := positionExternalId as positionExternalId,
                         @candidateExternalId := candidateExternalId as candidateExternalId,
                         stage
       FROM truong_jopapp
       where candidateExternalId <> ''
       ORDER BY positionExternalId, candidateExternalId,stage DESC;


select 
        positionExternalId as 'application-positionExternalId'
       ,candidateExternalId as 'application-candidateExternalId'
       #,concat('cand',candidateExternalId) as 'application-candidateExternalId'
       ,case stage
       when 0 then 'SHORTLISTED'
       when 1 then 'SENT'
       when 2 then '1ST_INTERVIEW'
       when 3 then '2ND_INTERVIEW'
       when 4 then 'OFFERED'
       when 5 then 'PLACED'
       end as 'application-stage'
from truong_jopapp_ordered where rn =1 ;


