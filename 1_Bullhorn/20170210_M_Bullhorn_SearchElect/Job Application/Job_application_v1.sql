select * from bullhorn1.BH_AggregatedEmailsContact AEC
left join bullhorn1.BH_Client CL on AEC.clientContactID = CL.clientID

select CL.clientID, CL.clientCorporationID, CL.userID, UC.name, * from bullhorn1.BH_Client CL
left join bullhorn1.BH_UserContact UC on CL.userID = UC.userID
where CL.userID = 113

select * from bullhorn1.BH_AggregatedEmailsContact
where clientContactID = 19276

select * from bullhorn1.BH_ClientCorporation

select * from bullhorn1.BH_UserContact

select * from bullhorn1.BH_Client where userID = 19276

select * from bullhorn1.BH_JobResponse where jobPostingID = 894
and status like '%Submission%'
order by userID

select * from bullhorn1.View_JobInterview where jobPostingID = 894

select * from bullhorn1.BH_JobPosting where jobPostingID = 894

select * from bullhorn1.View_JobSubmission where jobPostingID = 894

select * from bullhorn1.BH_ShortListJobPosting where jobPostingID = 894

select * from bullhorn1.BH_JobResponse where jobPostingID = 894
and status like '%Interview%'

select * from bullhorn1.BH_Activity

select * from bullhorn1.BH_JobOpportunity

select * from bullhorn1.BH_JobPosting where jobPostingID = 1095




select * from bullhorn1.BH_Sendout where jobPostingID = 753 --clientsubmission

select * from bullhorn1.BH_JobResponse where jobPostingID = 753 ---submission

select * from bullhorn1.View_JobInterview where jobPostingID = 753 --Interview

select distinct candidateUserID, jobPostingID from bullhorn1.View_Appointment where candidateUserID is not NULL and jobPostingID is not NULL
---appointmentID in (7472,7708,7922,8285)

candidateUserID
17265
34634
34641
34634

select * from bullhorn1.BH_Candidate CA
left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID
where CA.userID in (17265, 34634, 34641)


select a.jobPostingID as 'application-positionExternalId'
	, b.candidateID as 'application-candidateExternalId'
	, 'PLACED' as 'application-stage'
from [bullhorn1].[BH_Placement] a
left join bullhorn1.Candidate b
ON a.userID = b.userID

>>>>>>>>>
select JR.jobPostingID, CA.candidateID, 'SHORTLISTED' as 'Stage' from bullhorn1.BH_JobResponse JR
left join bullhorn1.Candidate CA on JR.userID = CA.userID

select SO.jobPostingID, CA.candidateID, 'SENT' as 'Stage' from bullhorn1.BH_Sendout SO
left join bullhorn1.Candidate CA on SO.candidateUserID = CA.userID
order by SO.jobPostingID

--PLACED
select a.jobPostingID as 'application-positionExternalId'
	, b.candidateID as 'application-candidateExternalId'
	, 'PLACED' as 'application-stage'
from [bullhorn1].[BH_Placement] a
left join bullhorn1.Candidate b
ON a.userID = b.userID

--INTERVIEW
with tmp1 as (select VA.appointmentID, CA.candidateID from bullhorn1.View_Appointment VA
left join bullhorn1.Candidate CA on VA.candidateUserID = CA.userID)

select VJI.jobPostingID, VJI.appointmentID, tmp1.candidateID, 'INTERVIEW' as 'Stage' from bullhorn1.View_JobInterview VJI
left join tmp1 on VJI.appointmentID = tmp1.appointmentID

--SENT
select JP.jobPostingID, CA.candidateID, 'SENT' as 'Stage', * from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_Sendout SO on JP.jobPostingID = SO.jobPostingID
left join bullhorn1.Candidate CA on SO.candidateUserID = CA.userID
where CA.candidateID is NULL
order by JP.jobPostingID

--SHORTLISTED



select * from bullhorn1.BH_Sendout where jobPostingID = 0

select * from bullhorn1.View_JobInterview

select * from bullhorn1.View_Appointment

select * from bullhorn1.Candidate

select * from bullhorn1.BH_JobOpportunity