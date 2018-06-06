select concat('MCMalaysia',VacancyID) as 'application-positionExternalId'
, concat('MCMalaysia',CandidateID) as 'application-candidateExternalId'
, Status
, case Status 
when '3rd Interview' then '2ND_INTERVIEW'
when 'Send CV' then 'SENT'
when 'NULL' then 'SHORTLISTED'
when '2nd Interview' then '2ND_INTERVIEW'
when '1st Interview' then '1ST_INTERVIEW'
when 'Other Interview' then '1ST_INTERVIEW'
when 'Offer Made' then 'OFFERED'
else 'SHORTLISTED' end as 'application-stage'
from Interview
where VacancyID is not NULL and CandidateID is not NULL