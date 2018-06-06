--PLACED
select a.jobPostingID as 'application-positionExternalId'
	, b.candidateID as 'application-candidateExternalId'
	, 'PLACED' as 'application-stage'
	, '' as 'application-note'
from bullhorn1.BH_Placement a
left join bullhorn1.Candidate b ON a.userID = b.userID
where b.isPrimaryOwner = 1
order by a.jobPostingID

--INTERVIEW
with tmp1 as (select VA.appointmentID, CA.candidateID from bullhorn1.View_Appointment VA
left join bullhorn1.Candidate CA on VA.candidateUserID = CA.userID)

select VJI.jobPostingID as 'application-positionExternalId'
, tmp1.candidateID as 'application-candidateExternalId'
, '1ST_INTERVIEW' as 'application-stage'
, max(VJI.appointmentID) as 'application-note'
from bullhorn1.View_JobInterview VJI
left join tmp1 on VJI.appointmentID = tmp1.appointmentID
group by VJI.jobPostingID, tmp1.candidateID
order by VJI.jobPostingID


--SENT with distinct
select distinct JP.jobPostingID as 'application-positionExternalId'
, CA.candidateID as 'application-candidateExternalId'
, 'SENT' as 'application-stage'
, '' as 'application-note'
from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_Sendout SO on JP.jobPostingID = SO.jobPostingID
left join bullhorn1.Candidate CA on SO.candidateUserID = CA.userID
where CA.isPrimaryOwner = 1
--and CA.candidateID is NULL
--group by JP.jobPostingID, CA.candidateID
order by JP.jobPostingID

--SHORTLISTED
select JR.jobPostingID as 'application-positionExternalId'
, CA.candidateID as 'application-candidateExternalId'
, replace(JR.status,'Submitted','SHORTLISTED') as 'application-stage'
, '' as 'application-note'
from bullhorn1.BH_JobResponse JR
left join bullhorn1.Candidate CA on JR.userID = CA.userID
where JR.status = 'Submitted'
and CA.isPrimaryOwner = 1
order by JR.jobPostingID


--SENT with more info
select JP.jobPostingID, CA.candidateID, 'SENT' as 'Stage', max(SO.sendoutID) as SendoutID from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_Sendout SO on JP.jobPostingID = SO.jobPostingID
left join bullhorn1.Candidate CA on SO.candidateUserID = CA.userID
where CA.isPrimaryOwner = 1
--and CA.candidateID is NULL
group by JP.jobPostingID, CA.candidateID
order by JP.jobPostingID

