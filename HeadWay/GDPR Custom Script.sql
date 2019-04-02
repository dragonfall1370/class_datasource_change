with test as (select case when intLawfulBasisLawfulBasisStatusId in (2,10) then 5 --'Consent Details Sent'
when intLawfulBasisLawfulBasisStatusId = 5 then 5 --'Accepted'
when intLawfulBasisLawfulBasisStatusId = 14 then 1 ----'Legitimate Interest Details Sent'
end as 'consent', intCandidateId, datExpiry, intLawfulBasisLawfulBasisStatusId
from dCandidatePrivacy where intLawfulBasisLawfulBasisStatusId in (2,5,10,14)
)


select *
,getdate() as date 
,-10 as obtained_by
,case when intLawfulBasisLawfulBasisStatusId = 14 then 1
when intLawfulBasisLawfulBasisStatusId in (2,10) then 1
when intLawfulBasisLawfulBasisStatusId = 5 then 1
end as exciplit_consent 

,case when intLawfulBasisLawfulBasisStatusId = 14 then 3
when intLawfulBasisLawfulBasisStatusId in (2,10) then 2
when intLawfulBasisLawfulBasisStatusId = 5 then 3
end as exercise_right

,case when intLawfulBasisLawfulBasisStatusId = 14 then 1
when intLawfulBasisLawfulBasisStatusId in (2,10) then 3
when intLawfulBasisLawfulBasisStatusId = 5 then 5
end as requestThrough

,case when intLawfulBasisLawfulBasisStatusId = 14 then 6
when intLawfulBasisLawfulBasisStatusId in (2,10) then 3
when intLawfulBasisLawfulBasisStatusId = 5 then 5
end as obtainedThrough

,case when intLawfulBasisLawfulBasisStatusId = 14 then 1
end as gdpr_expire
from test