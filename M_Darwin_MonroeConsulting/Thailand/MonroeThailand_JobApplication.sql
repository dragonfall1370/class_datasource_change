with CandidateInterview as (SELECT distinct VacancyID, CandidateID, IntStatus
FROM Interview
where VacancyID is not NULL and CandidateID is not NULL
group by VacancyID, CandidateID, IntStatus)

, JobAppMapping as (select concat('MCThailand',VacancyID) as A
, concat('MCThailand',CandidateID) as B
, IntStatus
, case IntStatus 
	when 'Rejected - Client' then 'SENT'
	when 'Did not turn up' then 'SHORTLISTED'
	when '3rd Interview' then '2ND_INTERVIEW'
	when 'Rejected - Applicant' then 'SHORTLISTED'
	when 'Send CV' then 'SENT'
	when 'Availability' then 'SHORTLISTED'
	when '2nd Interview' then '2ND_INTERVIEW'
	when '1st Interview' then '1ST_INTERVIEW'
	when 'Other Interview' then '1ST_INTERVIEW'
	when 'On Hold' then 'SHORTLISTED'
	when 'Rejected - Monroe' then 'SHORTLISTED'
	when 'Applicant on Hold' then 'SHORTLISTED'
	when 'Placed Perm' then 'PLACED'
	when 'rejected ' then 'SHORTLISTED'
	when 'cancelled' then 'SHORTLISTED'
	when 'Offer' then 'OFFERED'
	when 'CV Sent' then 'SENT'
	when '1st Interview - Phone' then '1ST_INTERVIEW'
	when 'Interview Cancelled' then '1ST_INTERVIEW'
	when 'Placed Contract' then 'PLACED'
	when 'Offer Made' then 'OFFERED'
	when 'Added to Job' then 'SHORTLISTED'
	else 'SHORTLISTED' end as 'application-stage'
, case IntStatus 
	when 'Rejected - Client' then 2
	when 'Did not turn up' then 1
	when '3rd Interview' then 4
	when 'Rejected - Applicant' then 1
	when 'Send CV' then 2
	when 'Availability' then 1
	when '2nd Interview' then 4
	when '1st Interview' then 3
	when 'Other Interview' then 3
	when 'On Hold' then 1
	when 'Rejected - Monroe' then 1
	when 'Applicant on Hold' then 1
	when 'Placed Perm' then 6
	when 'rejected ' then 1
	when 'cancelled' then 1
	when 'Offer' then 5
	when 'CV Sent' then 2
	when '1st Interview - Phone' then 3
	when 'Interview Cancelled' then 3
	when 'Placed Contract' then 6
	when 'Offer Made' then 5
	when 'Added to Job' then 1
	else 1 end as C
from CandidateInterview
where VacancyID is not NULL and VacancyID <> ''
and CandidateID is not NULL and CandidateID <> '')

, MaxApplication as 
(select A, B, max(C) as C from JobAppMapping
group by A, B)

select A as 'application-positionExternalId'
	, B as 'application-candidateExternalId'
	, case C
	when 7 then 'INVOICED'
	when 6 then 'PLACED'
	when 5 then 'OFFERED'
	when 4 then '2ND_INTERVIEW'
	when 3 then '1ST_INTERVIEW'
	when 2 then 'SENT'
	when 1 then 'SHORTLISTED'
	else '' end as 'application-stage'
from MaxApplication

/*LIST OF APPLICATION STAGE
1-SHORTLISTED
2-SENT
3-1ST_INTERVIEW
4-2ND_INTERVIEW
5-OFFERED
6-PLACED
7-INVOICED
*/


Rejected - Client
Did not turn up
3rd Interview
Rejected - Applicant
Send CV
NULL
Availability
2nd Interview
1st Interview
Other Interview
On Hold
Rejected - Monroe
Applicant on Hold
Placed Perm
rejected 
cancelled
Offer
CV Sent
1st Interview - Phone
Interview Cancelled
Placed Contract
Offer Made
Added to Job

SENT
SHORTLISTED
2ND_INTERVIEW
SHORTLISTED
SENT
SHORTLISTED
SHORTLISTED
2ND_INTERVIEW
1ST_INTERVIEW
1ST_INTERVIEW
SHORTLISTED
SHORTLISTED
SHORTLISTED
PLACED
SHORTLISTED
SHORTLISTED
OFFERED
SENT
1ST_INTERVIEW
1ST_INTERVIEW
PLACED
OFFERED
SHORTLISTED