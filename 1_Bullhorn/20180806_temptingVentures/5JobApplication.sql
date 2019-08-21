	        
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
               , JPI.ClientUserID
               , JPI.ContactID, JPI.ContactName, JPI.ContactEmail
               , JPI.CompanyID, JPI.CompanyName, JPI.JobID,  JPI.JobTitle
               --, JR.userID
               , CAI.CandidateName as '#Candidate Name'
               , CAI.candidateID as 'application-candidateExternalId'
               , CONVERT(VARCHAR(10),JR.dateAdded,120) as 'dateAdded'
               --, JR.status as '#application-stage' --This field only accepts: SHORTLISTED,SENT,FIRST_INTERVIEW,SECOND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
               , Coalesce(NULLIF(case
              when JR.status = '1st Interview' then 'FIRST_INTERVIEW'
              when JR.status = '2nd Interview' then 'SECOND_INTERVIEW'
              when JR.status = '3rd Interview' then 'SECOND_INTERVIEW'
              when JR.status = 'Candidate Rejected' then 'SHORTLISTED' -- > REJECTED
              when JR.status = 'Client Rejected' then 'SHORTLISTED' -- > REJECTED
              when JR.status = 'CV Sent' then 'SENT'
              when JR.status = 'Final Interview' then 'SECOND_INTERVIEW'
              when JR.status = 'Offer Extended' then 'OFFERED'
              when JR.status = 'Placed' then 'PLACED'
              when JR.status = 'Shortlisted' then 'SHORTLISTED'
              when JR.status = 'Submitted' then 'SENT'
                        else '' end, ''), '') as 'application-stage'
                --, JR.status as '#Original-status'
        --select count(*) --5995-- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
/*UNION
       select 
                PL.jobPostingID as 'application-positionExternalId'
              , CAI.candidateID as 'application-candidateExternalId' --, CAI.CandidateName as '#Candidate Name'
              , CONVERT(VARCHAR(10),PL.dateAdded,120) as 'dateAdded'
              , 'PLACED' as 'application-stage'
        from bullhorn1.BH_Placement PL
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on PL.userID = CAI.CandidateUserID
*/)
select * from ja0 where CompanyID = 179

, ja1 ("application-positionExternalId","application-candidateExternalId","application-Stage","dateAdded", rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              ,"application-Stage"
              ,"dateAdded"
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId","application-Stage" ORDER BY "application-positionExternalId" desc) FROM ja0 )


select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
from ja1
where rn = 1 and [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
order by [application-positionExternalId]  asc,
    CASE [application-stage]
        WHEN 'PLACED' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc

--select [application-stage], count(*) from jobapp group by [application-stage]

