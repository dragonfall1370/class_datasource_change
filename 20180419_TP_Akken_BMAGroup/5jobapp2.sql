
select 
         cg.username as 'externalId' 
        ,cg.fname,cg.mname,cg.lname, cg.profiletitle
        ,cg.sno
        ,j.positionExternalId, j.stage
        ,p.*
# select count(*) #98389#
from candidate_general cg
left join (
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
       ) j on j.candidateExternalId = username
left join posdesc p on p.posid = j.positionExternalId
where 
cg.username like '%cand90591%'
p.posid in (6117) or
cg.fname like '%Othoniel%' and cg.lname like '%Rodriguez%'


select * from candidate_general cg where cg.username in ('cand90591') cg.fname like '%Lida%' and cg.lname like '%Casanova%' or email = 'mailcasanova@netscape.net'
select * from posdesc where postitle = 'IT Support'