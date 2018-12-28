	        
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
               --, JR.userID, JPI.ClientUserID, JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name'
               , CAI.candidateID as 'application-candidateExternalId'
               , convert(varchar(10),JR.dateAdded,120) as 'dateAdded'
               , coalesce(nullif(case
                            when JR.status = 'Client Rejected' then 'SHORTLISTED'
                            when JR.status = 'Client Submission' then 'SENT'
                            when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
                            when JR.status = 'New Lead' then 'SHORTLISTED'
                            when JR.status = 'Placed' then 'PLACEMENT_PERMANENT'
                            when JR.status = 'Sales Rep Rejected' then 'SHORTLISTED'
                            when JR.status = 'Submission' then 'SHORTLISTED'
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
              , CAI.candidateID as 'application-candidateExternalId' --, CAI.CandidateName as '#Candidate Name'
              , convert(varchar(10),PL.dateAdded,120) as 'dateAdded'
              , 'PLACEMENT_PERMANENT' as 'application-stage'
        from bullhorn1.BH_Placement PL
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on PL.userID = CAI.CandidateUserID
)
--select * from ja0 where CompanyID = 179

, ja1 ("application-positionExternalId","application-candidateExternalId","application-Stage","dateAdded", rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              ,"application-Stage"
              ,"dateAdded"
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId","application-Stage" ORDER BY "application-positionExternalId" desc) 
       FROM ja0 )


select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
from ja1
where rn = 1 and [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
and [application-stage] = 'PLACEMENT_PERMANENT'
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

