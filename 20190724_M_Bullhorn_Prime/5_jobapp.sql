with ja as (
	select jr.jobPostingID
	, ca.candidateID
	, case when jp.employmentType is null then 301
		when JP.employmentType in ('Permanent') then 301
		when JP.employmentType in ('Contract','Fixed Contract','Part-time','Temporary','Temp to Perm') then 302
		--when JP.employmentType in ('Temporary','Temp to Perm') then 303 --no more temp type
		else null end as JobType
	, case when jr.status = 'Candidate Not Interested' then 1
		when jr.status = 'Client Rejected' then 1
		when jr.status = 'CV Sent' then 2
		when jr.status = 'Interview Scheduled' then 3
		when jr.status = 'Placed' then 6
		when jr.status = 'Shortlisted' then 1
		else 0 end as stage
	, case when jr.status in ('Candidate Not Interested', 'Client Rejected') then 'REJECTED'
		else NULL end as rejected
	, jr.status as substatus
	, jr.dateAdded
	, concat('JR', jr.jobResponseID) as referenceId --for updating placement purpose
	from bullhorn1.BH_JobResponse jr
	left join (select c.candidateID, c.userID as CandidateUserID, uc.userID, uc.name, uc.email as candidate_email
						from bullhorn1.Candidate c
						left join bullhorn1.BH_UserContact uc on c.userID = uc.userID) ca on ca.CandidateUserID = jr.userID
	left join bullhorn1.BH_JobPosting jp on jp.jobPostingID = jr.jobPostingID

	UNION ALL
--PLACEMENT
	select pl.jobPostingID
	, ca.candidateID
	, case when jp.employmentType is null then 301
		when JP.employmentType in ('Permanent') then 301
		when JP.employmentType in ('Contract','Fixed Contract','Part-time','Temporary','Temp to Perm') then 302
		--when JP.employmentType in ('Temporary','Temp to Perm') then 303 --no more temp type
		else null end as JobType
	, 6 as stage
	, NULL as rejected
	, 'PLACED' as substatus
	, pl.dateAdded --can be used as placed date / offered date
	, concat('PL', pl.placementID) --for updating placement purpose
	from bullhorn1.BH_Placement pl
	left join (select c.candidateID, c.userID as CandidateUserID, uc.userID, uc.name, uc.email as candidate_email
						from bullhorn1.Candidate c
						left join bullhorn1.BH_UserContact uc on c.userID = uc.userID) ca on ca.CandidateUserID = pl.userID
	left join bullhorn1.BH_JobPosting jp on jp.jobPostingID = pl.jobPostingID
	)

, higheststage as (select jobPostingID
		, candidateID
		, JobType
		, stage
		, rejected
		, substatus
		, dateAdded
		, row_number() over (partition by jobPostingID, candidateID order by stage desc, dateadded desc) as rn --highest stage first then latest date
		, referenceId
		from ja
		where stage > 0)

select concat('PR', jobPostingID) as [application-positionExternalId]
, concat('PR', candidateID) as [application-candidateExternalId]
, case when JobType = 301 and stage = 6 then 'PLACED_PERMANENT'
	when JobType = 302 and stage = 6 then 'PLACED_CONTRACT'
	else NULL end as PlacedStage
, case 
		when Stage = 6 then 'OFFERED' --to be updated as PLACED afterward
		when Stage = 5 then 'OFFERED'
		when Stage = 4 then 'SECOND_INTERVIEW'
		when Stage = 3 then 'FIRST_INTERVIEW'
		when Stage = 2 then 'SENT'
		when Stage = 1 then 'SHORTLISTED'
		end as [application-stage]
, dateAdded as [application-actionedDate]
, case when rejected is not NULL then dateAdded
	else NULL end as rejected_date
from higheststage
where rn = 1