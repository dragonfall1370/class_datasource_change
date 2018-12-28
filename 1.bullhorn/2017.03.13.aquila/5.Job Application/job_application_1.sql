
with 
tmp1 as (
	select VA.appointmentID, CA.candidateID from bullhorn1.View_Appointment VA
	left join bullhorn1.Candidate CA on VA.candidateUserID = CA.userID)

, temp as (
	select JR.jobPostingID as 'application-positionExternalId'
	, CA.candidateID as 'application-candidateExternalId'
	, replace(JR.status,'Submitted',1) as 'application-stage'
	, '' as 'application-note'
	from bullhorn1.BH_JobResponse JR
	left join bullhorn1.Candidate CA on JR.userID = CA.userID
	where JR.status = 'Submitted'
	and CA.isPrimaryOwner = 1
	--order by JR.jobPostingID
UNION
	select distinct JP.jobPostingID as 'application-positionExternalId'
	, CA.candidateID as 'application-candidateExternalId'
	, 2 as 'application-stage'
	, '' as 'application-note'
	from bullhorn1.BH_JobPosting JP
	left join bullhorn1.BH_Sendout SO on JP.jobPostingID = SO.jobPostingID
	left join bullhorn1.Candidate CA on SO.candidateUserID = CA.userID
	where CA.isPrimaryOwner = 1
	--order by JP.jobPostingID
UNION
	select VJI.jobPostingID as 'application-positionExternalId'
	, tmp1.candidateID as 'application-candidateExternalId'
	, 3 as 'application-stage'
	, max(VJI.appointmentID) as 'application-note'
	from bullhorn1.View_JobInterview VJI
	left join tmp1 on VJI.appointmentID = tmp1.appointmentID
	where tmp1.candidateID is not NULL
	group by VJI.jobPostingID, tmp1.candidateID
	--order by VJI.jobPostingID
UNION
	select a.jobPostingID as 'application-positionExternalId'
	, b.candidateID as 'application-candidateExternalId'
	, 4 as 'application-stage'
	, '' as 'application-note'
	from bullhorn1.BH_Placement a
	left join bullhorn1.Candidate b ON a.userID = b.userID
	where b.isPrimaryOwner = 1)

-----
select [application-positionExternalId]
	,[application-candidateExternalId]
	, case when max([application-stage])=1 then 'SHORTLISTED'
	when max([application-stage])=2 then 'SENT'
	when max([application-stage])=3 then '1ST_INTERVIEW'
	when max([application-stage])=4 then 'PLACED' 
	end as Stage 
from temp
group by [application-positionExternalId],[application-candidateExternalId]
order by temp.[application-candidateExternalId]

/*
select count(stage) from temp2
where Stage = 'PLACED'
*/
