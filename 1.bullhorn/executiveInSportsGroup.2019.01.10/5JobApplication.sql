	        
with
  JPInfo as (
  	select JP.jobPostingID as JobID
  		, JP.title as JobTitle
		, Cl.clientID as ContactID
		, Cl.userID as ClientUserID
		, UC.name as ContactName
		, UC.email as ContactEmail
		, CC.clientCorporationID as CompanyID
		, CC.name as CompanyName
	from bullhorn1.BH_JobPosting JP
	left join bullhorn1.BH_Client Cl on JP.clientUserID = Cl.userID
	left join bullhorn1.BH_UserContact UC on JP.clientUserID = UC.userID
	left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
	where 1=1 and JP.title <> '' and Cl.isPrimaryOwner = 1 )
 --select * from JPInfo order by JobID
        

, ja0 as (
       select
                JR.jobPostingID as 'application-positionExternalId'
               , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle,  JR.status, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
               , CAI.candidateID as 'application-candidateExternalId'
               , convert(varchar(10),JR.dateAdded,120) as 'dateAdded'
               , coalesce(nullif(case
                            when JR.status = 'Candidate Interested' then 'SHORTLISTED'
                            when JR.status = 'Contacted' then 'SHORTLISTED'
                            when JR.status = 'CV Sent' then 'SENT'
                            when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
                            when JR.status = 'LaunchPad' then 'CANDIDATES'
                            when JR.status = 'Offer Extended' then 'OFFERED'
                            when JR.status = 'Placed' then 'OFFERED' --PLACEMENT_PERMANENT'
                            when JR.status = 'Shortlisted' then 'SHORTLISTED'
                            when JR.status = 'Submitted' then 'SHORTLISTED'
                            else '' end, ''), '') as 'application-stage'  --, JR.status as '#Original-status' -- SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT, PLACEMENT_CONTRACT, PLACEMENT_TEMP, ONBOARDING.
        -- select count(*) --5995 -- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail  from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
UNION
       select 
                PL.jobPostingID as 'application-positionExternalId'
              , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle,  'PLACEMENT_PERMANENT' as status, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
              , CAI.candidateID as 'application-candidateExternalId'
              , convert(varchar(10),PL.dateAdded,120) as 'dateAdded'
              , 'OFFERED' as 'application-stage'
        -- select count(*)
        from bullhorn1.BH_Placement PL
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on PL.userID = CAI.CandidateUserID 
        left join JPInfo JPI on PL.jobPostingID = JPI.JobID
)
--select * from ja0 
--where ( [application-positionExternalId] = 6744 and [application-candidateExternalId] = 64995) or ([application-positionExternalId] = 5343 and [application-candidateExternalId] = 193508)
--where CompanyName like 'Nuance%' or CompanyID = 6

, ja1 ("application-positionExternalId","application-candidateExternalId","application-Stage","dateAdded", rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              ,"application-Stage"
              ,"dateAdded"
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId"/*,"application-Stage" */ORDER BY "application-positionExternalId" desc,
    CASE [application-stage]
        WHEN 'PLACEMENT_PERMANENT' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc ) 
       FROM ja0 
       where "application-Stage" not like 'CANDIDATE%' and "application-Stage" <> '' )


select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
from ja1
where rn = 1 --and [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
--and [application-stage] = 'PLACEMENT_PERMANENT'
and "application-positionExternalId" = 190
order by [application-positionExternalId]  asc,
    CASE [application-stage]
        WHEN 'PLACEMENT_PERMANENT' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc

--select [application-stage], count(*) from jobapp group by [application-stage]

