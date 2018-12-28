drop table if exists VCApplications
	        
;with
  JPInfo as (
  	select JP.jobPostingID as JobID
  		, JP.title as JobTitle
		, Cl.clientID as ContactID
		, Cl.userID as ClientUserID
		, UC.name as ContactName
		, UC.email as ContactEmail
		, CC.clientCorporationID as CompanyID
		, CC.name as CompanyName
		, JP.employmentType
	from bullhorn1.BH_JobPosting JP
	left join bullhorn1.BH_Client Cl on JP.clientUserID = Cl.userID
	left join bullhorn1.BH_UserContact UC on JP.clientUserID = UC.userID
	left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
	where 1=1 and JP.title <> '' and Cl.isPrimaryOwner = 1 )
        --select * from JPInfo order by JobID

, ja0 as (
       select
                JR.jobPostingID as [application-positionExternalId]
               --, JPI.ClientUserID
               --, JPI.ContactID, JPI.ContactName, JPI.ContactEmail
               --, JPI.CompanyID, JPI.CompanyName, JPI.JobID,  JPI.JobTitle
               --, JR.userID
               --, CAI.CandidateName as '#Candidate Name'
               , CAI.candidateID as [application-candidateExternalId]
               --, JR.status as '#application-stage' --This field only accepts: SHORTLISTED,SENT,FIRST_INTERVIEW,SECOND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
				--SHORTLISTED
				--SENT
				--FIRST_INTERVIEW
				--SECOND_INTERVIEW
				--OFFERED
				--PLACEMENT_PERMANENT
				--PLACEMENT_CONTRACT
				--PLACEMENT_TEMP
				--ONBOARDING
               , isnull(
					case lower(trim(isnull(JR.status, '')))
						when lower('Candidate Interested') then 'SHORTLISTED'
						when lower('Candidate Not Interested') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Submission') then 'SENT'
						when lower('CV Sent') then 'SENT'
						when lower('Final Interview') then 'SECOND_INTERVIEW'
						when lower('Internally Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Interview Scheduled') then 'FIRST_INTERVIEW'
						when lower('New Lead') then 'SHORTLISTED'
						when lower('Offer Extended') then 'OFFERED'
						when lower('Offer Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Placed') then 'OFFERED'
						when lower('Shortlisted') then 'SHORTLISTED'
						when lower('Submitted') then 'SHORTLISTED'
						--when lower(trim(isnull(JR.status, ''))) = lower('Candidate Interested') then 'SHORTLISTED'
						--when lower(trim(isnull(JR.status, ''))) = lower('2nd Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('Final Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('CV Sent') then 'SENT'
						--when lower(trim(isnull(JR.status, ''))) = lower('Placed') then 'PLACED'
						--when lower(trim(isnull(JR.status, ''))) = lower('Shortlisted') then 'SHORTLISTED'
						else ''
					end
					, 'SHORTLISTED'
				) as [application-stage]
				, isnull(
					case lower(trim(isnull(JR.status, '')))
						when lower('Candidate Interested') then 'SHORTLISTED'
						when lower('Candidate Not Interested') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Submission') then 'SENT'
						when lower('CV Sent') then 'SENT'
						when lower('Final Interview') then 'SECOND_INTERVIEW'
						when lower('Internally Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Interview Scheduled') then 'FIRST_INTERVIEW'
						when lower('New Lead') then 'SHORTLISTED'
						when lower('Offer Extended') then 'OFFERED'
						when lower('Offer Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Placed') then
							case lower(trim(isnull(JPI.employmentType, '')))
								when lower('Contract') then 'PLACEMENT_CONTRACT'
								when lower('Fixed Contract') then 'PLACEMENT_CONTRACT'
								when lower('Temporary') then 'PLACEMENT_CONTRACT'
								when lower('Permanent') then 'PLACEMENT_PERMANENT'
								when lower('Project') then 'PLACEMENT_TEMP'
								when lower('Temp to Perm') then 'PLACEMENT_TEMP'
								when lower('Perm & Contract') then 'PLACEMENT_PERMANENT'
								else 'PLACEMENT_PERMANENT'
							end
						when lower('Shortlisted') then 'SHORTLISTED'
						when lower('Submitted') then 'SHORTLISTED'
						--when lower(trim(isnull(JR.status, ''))) = lower('Candidate Interested') then 'SHORTLISTED'
						--when lower(trim(isnull(JR.status, ''))) = lower('2nd Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('Final Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('CV Sent') then 'SENT'
						--when lower(trim(isnull(JR.status, ''))) = lower('Placed') then 'PLACED'
						--when lower(trim(isnull(JR.status, ''))) = lower('Shortlisted') then 'SHORTLISTED'
						else ''
					end
					, 'SHORTLISTED'
				) as FinalStage
				, isnull(JR.dateAdded, getdate()) as ActionedDate
				, case lower(trim(isnull(JR.status, '')))
						when lower('Candidate Not Interested') then 1
						when lower('Client Rejected') then 1
						when lower('Internally Rejected') then 1
						when lower('Offer Rejected') then 1
						else 0
					end as Rejected
				, iif(
					case lower(trim(isnull(JR.status, '')))
						when lower('Candidate Not Interested') then 1
						when lower('Client Rejected') then 1
						when lower('Internally Rejected') then 1
						when lower('Offer Rejected') then 1
						else 0
					end = 1, isnull(JR.dateAdded, getdate()), null) as RejectedDate
                --, JR.status as '#Original-status'
        --select count(*) --5995-- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
UNION ALL
	select 
	PL.jobPostingID as [application-positionExternalId]
	, CAI.candidateID as [application-candidateExternalId] --, CAI.CandidateName as '#Candidate Name'
	, 'OFFERED' as [application-stage]
	, case lower(trim(isnull(PL.employmentType, '')))
		when lower('Contract') then 'PLACEMENT_CONTRACT'
		when lower('Fixed Contract') then 'PLACEMENT_CONTRACT'
		when lower('Temporary') then 'PLACEMENT_CONTRACT'
		when lower('Permanent') then 'PLACEMENT_PERMANENT'
		when lower('Project') then 'PLACEMENT_TEMP'
		when lower('Temp to Perm') then 'PLACEMENT_TEMP'
		when lower('Perm & Contract') then 'PLACEMENT_PERMANENT'
		else 'PLACEMENT_PERMANENT'
	end as FinalStage
	, isnull(PL.dateAdded, getdate()) as ActionedDate
	, iif(lower(trim(isnull(PL.status, ''))) = lower('Rejected'), 1, 0) as Rejected
	, iif(iif(lower(trim(isnull(PL.status, ''))) = lower('Rejected'), 1, 0) = 1, isnull(PL.dateAdded, getdate()), null) as RejectedDate
	--, '' as ownerEmail
	from bullhorn1.BH_Placement PL
	left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on PL.userID = CAI.CandidateUserID
)


--, ja1 as (
--	SELECT 
--	[application-positionExternalId]
--	, [application-candidateExternalId]
--	, [application-Stage]
--	, FinalStage
--	, ActionedDate
--	, Rejected
--	, RejectedDate
--	, ROW_NUMBER() OVER (PARTITION BY [application-positionExternalId], [application-candidateExternalId], [application-Stage] order by ActionedDate desc, Rejected desc, RejectedDate desc) rn
--	FROM ja0
--)


--select
--[application-positionExternalId]
--, [application-candidateExternalId]
--, [application-Stage]
--, FinalStage
--, ActionedDate
--, Rejected
--, RejectedDate

--into VCApplications

--from ja1
--where rn = 1 and [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
--order by [application-positionExternalId]  asc,
--    CASE [application-stage]
--        --WHEN 'PLACEMENT_PERMANENT' THEN 1
--        --WHEN 'PLACEMENT_CONTRACT' THEN 1
--        --WHEN 'PLACEMENT_TEMP' THEN 1
--        WHEN 'OFFERED' THEN 2
--        WHEN 'SECOND_INTERVIEW' THEN 3
--        WHEN 'FIRST_INTERVIEW' THEN 4
--        WHEN 'SENT' THEN 5
--        WHEN 'SHORTLISTED' THEN 6
--    END asc

----select [application-stage], count(*) from jobapp group by [application-stage]

--select
----distinct
--[application-positionExternalId]
--, [application-candidateExternalId]
--, [application-stage]
--from VCApplications
----where [application-positionExternalId] not in (select [position-externalID] from VCJobs)
----where [application-candidateExternalId] not in (select [candidate-externalId] from VCCans)-- where isDeleted = 0)
--where [application-candidateExternalId] in (select [candidate-externalId] from VCCans where isDeleted = 0)
----and [application-stage] = 'OFFERED'
----where [application-positionExternalId] = 180
----and [application-candidateExternalId] = 7

, AppTmp2 as (
select
	[application-positionExternalId]
	, [application-candidateExternalId]
	, [application-stage]
	, FinalStage
	, ActionedDate
	, Rejected
	, RejectedDate
	, row_number() over (
		partition by [application-positionExternalId], [application-candidateExternalId], [application-stage]
		order by ActionedDate desc, Rejected desc, RejectedDate desc
	) as rn
	--, ownerEmail
	--, PlacementNote
from ja0
)

--select * from AppTmp2

, AppTmp3 as (
	select
		[application-positionExternalId]
		, [application-candidateExternalId]
		, [application-stage]
		, case([application-stage])
			--when 'PLACEMENT_PERMANENT' THEN 1
			--when 'PLACEMENT_CONTRACT' THEN 1
			--when 'PLACEMENT_TEMP' THEN 1
			when 'OFFERED' then 2
			when 'SECOND_INTERVIEW' then 3
			when 'FIRST_INTERVIEW' then 4
			when 'SENT' then 5
			when 'SHORTLISTED' then 6
		end as AppStage
		, FinalStage
		, ActionedDate
		, Rejected
		, RejectedDate
		--, ownerEmail
		--, PlacementNote
	from AppTmp2
	where rn = 1
)

--select * from AppTmp3

, AppTmp4 as (
select
	[application-positionExternalId]
	, [application-candidateExternalId]
	, [application-stage]
	, row_number() over (
		partition by [application-positionExternalId], [application-candidateExternalId]
		order by AppStage asc
	) as rn
	, FinalStage
	, ActionedDate
	, Rejected
	, RejectedDate
	--, ownerEmail
	--, PlacementNote
from AppTmp3
)

select
	cast([application-positionExternalId] as varchar(10)) as [application-positionExternalId]
	, cast([application-candidateExternalId] as varchar(10)) as [application-candidateExternalId]
	, [application-stage]
	, FinalStage
	, ActionedDate
	, Rejected
	, RejectedDate
	--, ownerEmail
	--, PlacementNote

into VC_App

from AppTmp4
where rn = 1

select * from VC_App
--where [application-positionExternalId] not in (select [position-externalID] from VC_Job)
--where [application-candidateExternalId] not in (select [candidate-externalId] from VC_Can)
--where Rejected = 1
where [application-candidateExternalId] in (select [candidate-externalId] from VCCans where isDeleted = 0)
and [application-positionExternalId] = 773 and [application-candidateExternalId] = 11892
order by
ActionedDate
--[application-positionExternalId]
--, [application-candidateExternalId]


--select distinct [application-positionExternalId]
--, [application-candidateExternalId] from VC_App