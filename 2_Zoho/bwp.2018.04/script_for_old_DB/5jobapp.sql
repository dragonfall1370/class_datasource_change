/*
-- CLIENTS
select clientid from clients where clientId = 'Zrecruit_274609000000051003'

-- contact
select ContactId from Contacts where ContactId = 'Zrecruit_274609000000051003'

-- candidate
select CandidateId from Candidates where candidateid = 'Zrecruit_274609000000051003'

-- users
select userid from users where userid = 'Zrecruit_274609000000051003' <<<<<

-- JOB
select top 100 * from JobOpenings j
select top 100
        j.JobOpeningID, j.AccountManagerId, j.ClientId, j.AssignedRecruiter_s
from JobOpenings j
left join Clients c on c.ClientId = j.ClientId where c.ClientId is not null
*/

/*
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
*/

with jobapp as (
        select
                  --JPI.ClientUserID
                --, JPI.ContactID, JPI.ContactName, JPI.ContactEmail
                --, JPI.CompanyID, JPI.CompanyName, JPI.JobTitle
                --, JR.userID
                  a.JobOpeningID as 'application-positionExternalId'
                --, CAI.CandidateName as '#Candidate Name'
                , a.CandidateID as 'application-candidateExternalId'	
                --, JR.status as '#application-stage' --This field only accepts: SHORTLISTED,SENT,FIRST_INTERVIEW,SECOND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
                , Coalesce(NULLIF(case
                            when a.CandidateStatus = 'Approved by client' then 'SENT'
                            when a.CandidateStatus = 'Associated' then 'SHORTLISTED'
                            when a.CandidateStatus = 'Contacted' then 'SHORTLISTED'
                            when a.CandidateStatus = 'CV rejected by client - seen previously' then 'SENT'
                            when a.CandidateStatus = 'CV submitted' then 'SENT'
                            when a.CandidateStatus = 'Hired' then 'PLACED'
                            when a.CandidateStatus = 'Interview' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'interview 1' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'Interview rejected by client' then 'SENT'
                            when a.CandidateStatus = 'Interview rejected by client - character/personality' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'Interview rejected by client - wrong experience' then 'SENT'
                            when a.CandidateStatus = 'Interview-in-Progress' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'Interview-Scheduled' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'Interview-to-be-Scheduled' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'New' then 'SHORTLISTED'
                            when a.CandidateStatus = 'No-Show' then 'PLACED'
                            when a.CandidateStatus = 'Offer' then 'OFFERED'
                            when a.CandidateStatus = 'Offer accepted' then 'OFFERED'
                            when a.CandidateStatus = 'Offer rejected' then 'OFFERED'
                            when a.CandidateStatus = 'Offer rejected - salary' then 'OFFERED'
                            when a.CandidateStatus = 'Offer-Accepted' then 'OFFERED'
                            when a.CandidateStatus = 'Offer-Declined' then 'OFFERED'
                            when a.CandidateStatus = 'Offer-Made' then 'OFFERED'
                            when a.CandidateStatus = 'Offer-Withdrawn' then 'OFFERED'
                            when a.CandidateStatus = 'Rejected' then 'OFFERED'
                            when a.CandidateStatus = 'Rejected by client' then 'SENT'
                            when a.CandidateStatus = 'Rejected-for-Interview' then 'FIRST_INTERVIEW'
                            when a.CandidateStatus = 'Started' then 'PLACED'
                            when a.CandidateStatus = 'Submitted-to-client' then 'SENT'
                            when a.CandidateStatus = 'To-be-Offered' then 'OFFERED'
                            when a.CandidateStatus = 'Waiting-for-Evaluation' then 'SHORTLISTED'
                     else '' end, ''), '') as 'application-stage'             
        -- select count(*) --1972-- select distinct a.CandidateStatus
        from Associated a
        --left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        --left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        --where CAI.candidateID is not null 
        )

--select * from jobapp where [application-stage] <> '' order by [application-positionExternalId] desc
select * 
from jobapp 
where [application-stage] <> '' --and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
order by [application-positionExternalId] desc,
    CASE [application-stage]
        WHEN 'PLACED' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN '2ND_INTERVIEW' THEN 3
        WHEN '1ST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END desc
--select [application-stage], count(*) from jobapp group by [application-stage]

