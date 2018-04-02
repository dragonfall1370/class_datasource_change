with OriginalJobApp as (
select PEOPLECLOUD1__PLACEMENT__C as A
, PEOPLECLOUD1__CANDIDATE__C as B
, case when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Skills Testing' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Possible / Follow Up' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Shortlist' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Culture Fit' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Consider for Other Roles' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'New - Awaiting Approval' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Pre-Employment Screening' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Pre-Employment Screening - Did Not Pass' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Skills Testing - Unsuitable - Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Candidate Not Interested - Length of Role' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Already Applied Directly' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Already Applied Through Agency' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Communication Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Salary Too High' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Salary Too Low' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'Right to Represent' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Unsuitable - Consider for Other Roles' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Awaiting Approval' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Candidate Not Interested' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Client Not Interested - Experience' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Client Not Interested - Industry' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Unsuitable' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Unsuitable - Experience' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'Right to Represent - Offer Withdrawn by Client' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Invoiced' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Cancelled' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Placement Shortened' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Awaiting Invoicing' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Replaced after Placement' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Manager Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Manager' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Approval Invoicing' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Placement Shortened' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Awaiting Invoicing' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Invoiced' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Cancelled' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Replaced after Placement' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Manager Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approval Rejected Manager' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approval Rejected Finance' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Manager' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Temp' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Permanent' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approval Rejected Contracts' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Temp to Perm' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Offer' then '5'
	when STATUS_CANDIDATE_PROGRESS__C = 'Withdrawn - Offer Withdrawn by Client' then '5'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Not Interested' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Not Interested - Found Other Employment' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Not Interested - Salary' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Withdrew from Process' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Communication Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Experience' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (2nd)' then '4'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (3rd+)' then '4'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (2nd) - Candidate Not Interested - Found Other Employment' then '4'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (1st)' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Reference Check' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Interviewed - Client Withdrew' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Interviewed - Candidate Withdrew' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (1st) - Candidate Withdrew from Process' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Left Message - Unsuitable - Culture Fit' then '1'
	else 0 end as C
from CandidateManagement
where PEOPLECLOUD1__PLACEMENT__C is not NULL and PEOPLECLOUD1__CANDIDATE__C is not NULL
and PEOPLECLOUD1__CANDIDATE__C in (select ID from Candidate where PEOPLECLOUD1__STATUS__C not in ('Inactive')))

, maxJobApp as (select A, B, max(C) as maxJobApp
	from OriginalJobApp
	where C > 0
	group by A, B)

select B as 'application-candidateExternalId'
, A as 'application-positionExternalId'
, case maxJobApp
	when 6 then 'PLACED'
	when 5 then 'OFFERED'
	when 4 then 'SECOND_INTERVIEW'
	when 3 then 'FIRST_INTERVIEW'
	when 2 then 'SENT'
	when 1 then 'SHORTLISTED'
	else '' end as 'application-stage'
from maxJobApp