select concat('MCPhilippines',VacancyID) as 'application-positionExternalId'
, concat('MCPhilippines',CandidateID) as 'application-candidateExternalId'
, IntStatus
, case IntStatus 
when 'Rejected - Client' then 'SENT'
when '3rd Interview' then '2ND_INTERVIEW'
when 'Offering Stage' then 'OFFERED'
when 'Done final interview' then '2ND_INTERVIEW'
when 'Placed' then 'PLACED'
when 'Rejected - Applicant' then 'SHORTLISTED'
when 'Send CV' then 'SENT'
when 'Test Arranged' then 'SHORTLISTED'
when 'final interview' then '2ND_INTERVIEW'
when '2nd Interview' then '2ND_INTERVIEW'
when '1st Interview' then '1ST_INTERVIEW'
when 'No Show' then 'SHORTLISTED'
when 'Other Interview' then '1ST_INTERVIEW'
when 'On Hold' then 'SHORTLISTED'
when 'Rejected - Monroe' then 'SHORTLISTED'
when 'Placed Perm' then 'PLACED'
when 'CV Sent' then 'SENT'
when 'Job Offer' then 'OFFERED'
when 'Placed Contract' then 'PLACED'
when 'Offer Made' then 'OFFERED'
when 'CV Rejected - Client' then 'SENT'
when 'Added to Job' then 'SHORTLISTED'
else 'SHORTLISTED' end as 'application-stage'
from Interview
where VacancyID is not NULL and CandidateID is not NULL




Rejected - Client			SENT
3rd Interview				2ND_INTERVIEW
Offering Stage				OFFERED
Done final interview		2ND_INTERVIEW
Placed						PLACED
Rejected - Applicant		SHORTLISTED
Send CV						SENT
NULL						SHORTLISTED
Test Arranged				SHORTLISTED
final interview				2ND_INTERVIEW
2nd Interview				2ND_INTERVIEW
1st Interview				1ST_INTERVIEW
No Show						SHORTLISTED
Other Interview				1ST_INTERVIEW
On Hold						SHORTLISTED
Rejected - Monroe			SHORTLISTED
Placed Perm					PLACED
CV Sent						SENT
Job Offer					OFFERED
Placed Contract				PLACED
Offer Made					OFFERED
CV Rejected - Client		SENT
Added to Job				SHORTLISTED