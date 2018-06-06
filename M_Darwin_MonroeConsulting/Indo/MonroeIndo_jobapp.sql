with CandidateInterview as (SELECT distinct VacancyID, CandidateID, IntStatus
FROM Interview
where VacancyID is not NULL and CandidateID is not NULL
group by VacancyID, CandidateID, IntStatus)

, JobAppMapping as (select concat('MCIndo',VacancyID) as A
, concat('MCIndo',CandidateID) as B
, IntStatus
, case IntStatus 
	when '1st Interview' then '1ST_INTERVIEW'
	when '2nd Interview' then '2ND_INTERVIEW'
	when '3rd Interview' then '2ND_INTERVIEW'
	when 'Added to Job' then 'SHORTLISTED'
	when 'Applicant On Hold' then 'SHORTLISTED'
	when 'Availability' then 'SHORTLISTED'
	when 'Candidate Unrejected' then 'SHORTLISTED'
	when 'CV Rejected' then 'SHORTLISTED'
	when 'CV Sent' then 'SENT'
	when 'Email conversation' then 'SHORTLISTED'
	when 'Interview' then '1ST_INTERVIEW'
	when 'Interview Cancelled' then '1ST_INTERVIEW'
	when 'Linked In message' then 'SHORTLISTED'
	when 'Linkedin Message' then 'SHORTLISTED'
	when 'Offer' then 'OFFERED'
	when 'Offer Made' then 'OFFERED'
	when 'On Hold' then 'SHORTLISTED'
	when 'Other Interview' then '1ST_INTERVIEW'
	when 'Permanently Placed' then 'PLACED'
	when 'Phone call' then 'SHORTLISTED'
	when 'Placed Contract' then 'PLACED'
	when 'Placed Perm' then 'PLACED'
	when 'Placed Permanent' then 'PLACED'
	when 'Rejected by Applicant' then 'SHORTLISTED'
	when 'rejected by candidate' then 'SHORTLISTED'
	when 'Rejected by Client' then 'SENT'
	when 'Rejected by Company' then 'SHORTLISTED'
	when 'Rejected by Monroe' then 'SHORTLISTED'
	when 'Send CV' then 'SENT'
	when 'Short listed' then 'SHORTLISTED'
	when 'Technical Test' then 'SHORTLISTED'
	when 'Unrejected' then 'SHORTLISTED'
ELSE 'SHORTLISTED' END AS 'application-stage'
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