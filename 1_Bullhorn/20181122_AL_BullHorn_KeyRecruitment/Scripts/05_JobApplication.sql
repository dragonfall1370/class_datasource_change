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
               , JR.dateAdded as dateAdded
			   , JR.dateWebResponse as dateWebResponse
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
						when lower('1st Interview') then 'FIRST_INTERVIEW'
						when lower('2nd Interview') then 'SECOND_INTERVIEW'
						when lower('Candidate Reject opportunity') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Candidate Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Reject candidate') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('CV Sent') then 'SENT'
						when lower('Final Interview') then 'SECOND_INTERVIEW'
						when lower('Longlisted') then 'SHORTLISTED'
						when lower('Offer Extended') then 'OFFERED'
						when lower('Offer Rejected') then 'OFFERED' -- 'OFFERED > REJECTED'
						when lower('Placed') then 'OFFERED'
						when lower('Rejected by recruiter') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Shortlisted') then 'SHORTLISTED'
						when lower('Submitted') then 'SENT'
						--when lower(trim(isnull(JR.status, ''))) = lower('Candidate Interested') then 'SHORTLISTED'
						--when lower(trim(isnull(JR.status, ''))) = lower('2nd Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('Final Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('CV Sent') then 'SENT'
						--when lower(trim(isnull(JR.status, ''))) = lower('Placed') then 'PLACED'
						--when lower(trim(isnull(JR.status, ''))) = lower('Shortlisted') then 'SHORTLISTED'
						else ''
					end
					, ''
				) as [application-stage]

				, isnull(
					case lower(trim(isnull(JR.status, '')))
						when lower('1st Interview') then 'FIRST_INTERVIEW'
						when lower('2nd Interview') then 'SECOND_INTERVIEW'
						when lower('Candidate Reject opportunity') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Candidate Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Reject candidate') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Client Rejected') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('CV Sent') then 'SENT'
						when lower('Final Interview') then 'SECOND_INTERVIEW'
						when lower('Longlisted') then 'SHORTLISTED'
						when lower('Offer Extended') then 'OFFERED'
						when lower('Offer Rejected') then 'OFFERED' -- 'OFFERED > REJECTED'
						when lower('Placed') then
							case lower(trim(isnull(JPI.employmentType, '')))
								when lower('Contract') then 'PLACEMENT_CONTRACT'
								when lower('Fixed Term') then 'PLACEMENT_CONTRACT'
								when lower('Temporary') then 'PLACEMENT_TEMP'
								when lower('Permanent') then 'PLACEMENT_PERMANENT'
								else 'PLACEMENT_PERMANENT'
							end
						when lower('Rejected by recruiter') then 'SHORTLISTED' -- 'SHORTLISTED > REJECTED'
						when lower('Shortlisted') then 'SHORTLISTED'
						when lower('Submitted') then 'SENT'
						--when lower(trim(isnull(JR.status, ''))) = lower('Candidate Interested') then 'SHORTLISTED'
						--when lower(trim(isnull(JR.status, ''))) = lower('2nd Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('Final Interview') then 'SECOND_INTERVIEW'
						--when lower(trim(isnull(JR.status, ''))) = lower('CV Sent') then 'SENT'
						--when lower(trim(isnull(JR.status, ''))) = lower('Placed') then 'PLACED'
						--when lower(trim(isnull(JR.status, ''))) = lower('Shortlisted') then 'SHORTLISTED'
						else ''
					end
					, ''
				) as ApplicationFinalStage
				
			   , JPI.employmentType as employmentType
			   , case lower(trim(isnull(JR.status, '')))
						when lower('Candidate Reject opportunity') then 1
						when lower('Candidate Rejected') then 1
						when lower('Client Reject candidate') then 1
						when lower('Client Rejected') then 1
						when lower('Offer Rejected') then 1
						when lower('Rejected by recruiter') then 1
						else 0
					end as Rejected
                --, JR.status as '#Original-status'
        --select count(*) --5995-- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1 and CA.isDeleted = 0) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
UNION
       select 
                PL.jobPostingID as [application-positionExternalId]
              , CAI.candidateID as [application-candidateExternalId] --, CAI.CandidateName as '#Candidate Name'
              , PL.dateAdded as dateAdded
			  , (SELECT Max(v) 
					FROM (
						VALUES
							(isnull(dateAdded, cast(0 as datetime)))
							, (isnull(dateClientEffective, cast(0 as datetime)))
							, (isnull(dateEffective, cast(0 as datetime)))
					) AS value(v)
				) as dateWebResponse
              , 'OFFERED'as [application-stage]
			  , case lower(trim(isnull(PL.employmentType, '')))
					when lower('Contract') then 'PLACEMENT_CONTRACT'
					when lower('Fixed Term') then 'PLACEMENT_CONTRACT'
					when lower('Temporary') then 'PLACEMENT_TEMP'
					when lower('Permanent') then 'PLACEMENT_PERMANENT'
					else 'PLACEMENT_PERMANENT'
				end as ApplicationFinalStage
			  , PL.employmentType as employmentType
			  , case lower(trim(isnull(PL.status, '')))
						when lower('Candidate Reject opportunity') then 1
						when lower('Candidate Rejected') then 1
						when lower('Client Reject candidate') then 1
						when lower('Client Rejected') then 1
						when lower('Offer Rejected') then 1
						when lower('Rejected by recruiter') then 1
						else 0
					end as Rejected
        from bullhorn1.BH_Placement PL
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1 and CA.isDeleted = 0) CAI on PL.userID = CAI.CandidateUserID
)


, ja1 ([application-positionExternalId], [application-candidateExternalId], [application-Stage], ApplicationFinalStage, dateAdded, employmentType, Rejected, RejectedDate, rn) as (
	SELECT 
		[application-positionExternalId]
		, [application-candidateExternalId]
		, [application-Stage]
		, ApplicationFinalStage
		, dateAdded
		, employmentType
		, Rejected
		, iif(Rejected = 0, null, dateWebResponse) as RejectedDate
		, rn = ROW_NUMBER() OVER (PARTITION BY [application-positionExternalId], [application-candidateExternalId], [application-Stage] ORDER BY [application-positionExternalId] desc)
	FROM ja0
)


select [application-positionExternalId], [application-candidateExternalId], [application-Stage], ApplicationFinalStage, dateAdded, employmentType, Rejected, RejectedDate

into VCApplications

from ja1
where rn = 1 and [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
order by [application-positionExternalId]  asc,
    CASE [application-stage]
        --WHEN 'PLACEMENT_PERMANENT' THEN 1
        --WHEN 'PLACEMENT_CONTRACT' THEN 1
        --WHEN 'PLACEMENT_TEMP' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc

--select [application-stage], count(*) from jobapp group by [application-stage]

select
--distinct
[application-positionExternalId]
, [application-candidateExternalId]
, [application-stage]
from VCApplications
--where [application-positionExternalId] = 180
--and [application-candidateExternalId] = 7

--select * from bullhorn1.BH_JobResponse

--SELECT
--	placementID
--	, dateAdded
--	, dateClientEffective
--	, dateEffective
--    , (SELECT Max(v) 
--   FROM (
--	VALUES (
--		isnull(dateAdded, cast(0 as datetime)))
--		, (isnull(dateClientEffective, cast(0 as datetime)))
--		, (isnull(dateEffective, cast(0 as datetime)))
--	) AS value(v)) as [MaxDate]
--from bullhorn1.BH_Placement PL