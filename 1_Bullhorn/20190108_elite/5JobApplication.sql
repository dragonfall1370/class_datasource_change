	        
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
               , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
               , CAI.candidateID as 'application-candidateExternalId'
               , convert(varchar(10),JR.dateAdded,120) as 'dateAdded'
               , coalesce(nullif(case
when JR.status = '1:Referred' then 'SHORTLISTED'
when JR.status = 'Applied > not suitable' then 'SHORTLISTED' -- > REJECTED'
when JR.status = 'Cand. not interested' then 'SHORTLISTED' -- > REJECTED'
when JR.status = 'Candidate emailed job' then 'SHORTLISTED'
when JR.status = 'Candidate Interested' then 'SHORTLISTED'
when JR.status = 'Conversation held' then 'SHORTLISTED'
when JR.status = 'Done References' then 'SHORTLISTED'
when JR.status = 'External Application' then 'SHORTLISTED'
when JR.status = 'Handoff' then 'SHORTLISTED'
when JR.status = 'Internal Submit' then 'SHORTLISTED'
when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
when JR.status = 'Interviewed>NoRetained' then 'FIRST_INTERVIEW' -- > REJECTED'
when JR.status = 'Interviewing' then 'FIRST_INTERVIEW'
when JR.status = 'Not able to contact' then 'SHORTLISTED' -- > REJECTED'
when JR.status = 'Offer Made' then 'OFFERED'
when JR.status = 'Placed' then 'OFFERED' -->PLACED'
when JR.status = 'Placed Contract' then 'OFFERED' -->PLACED'
when JR.status = 'Placed Perm' then 'OFFERED' -->PLACED'
when JR.status = 'Previously submitted' then 'SENT'
--when JR.status = 'Questionnaire Reminder' then 'N/A'
--when JR.status = 'Questionnaire TO send' then 'N/A'
--when JR.status = 'Questionnaire1 filled' then 'N/A'
--when JR.status = 'Questionnaire1 Sent' then 'N/A'
--when JR.status = 'Questionnaire2 Filled' then 'N/A'
--when JR.status = 'Questionnaire2 Sent' then 'N/A'
--when JR.status = 'Reference Check' then 'N/A'
--when JR.status = 'References 2 Follow-up' then 'N/A'
when JR.status = 'Rejected by client' then 'SENT' -- > REJECTED'
when JR.status = 'Rejected offer' then 'OFFERED' -- > REJECTED'
when JR.status = 'Reviewed' then 'SHORTLISTED'
when JR.status = 'Shortlisted' then 'SHORTLISTED'
when JR.status = 'Shortlisted-Hot' then 'SHORTLISTED'
--when JR.status = 'Shortlisted-No' then 'SHORTLISTED > REJECTED'
--when JR.status = 'Submitted' then 'SENT'
--when JR.status = 'Submitted to AM' then 'SENT'
--when JR.status = 'Submitted to Client' then 'SENT'
--when JR.status = 'Submitted>Not-Retained' then 'SENT > REJECTED'
--when JR.status = 'To Ask 4 References' then 'N/A'
--when JR.status = 'To Call' then 'SHORTLISTED'
--when JR.status = 'To Send' then 'SHORTLISTED'
                            else '' end, ''), '') as 'application-stage'  --, JR.status as '#Original-status' -- SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT, PLACEMENT_CONTRACT, PLACEMENT_TEMP, ONBOARDING.
        -- select count(*) --5995 -- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail  from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
UNION
       select 
                PL.jobPostingID as 'application-positionExternalId'
              , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
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
              , rn = ROW_NUMBER() OVER (PARTITION BY "application-positionExternalId","application-candidateExternalId"/*,"application-Stage" */
                     ORDER BY "application-positionExternalId" desc,
                            CASE [application-stage]
                            WHEN 'PLACEMENT_PERMANENT' THEN 1
                            WHEN 'OFFERED' THEN 2
                            WHEN 'SECOND_INTERVIEW' THEN 3
                            WHEN 'FIRST_INTERVIEW' THEN 4
                            WHEN 'SENT' THEN 5
                            WHEN 'SHORTLISTED' THEN 6
                            END asc )
       FROM ja0 
       where "application-Stage" not like 'CANDIDATE%' and "application-Stage" <> '')



--select [application-stage], count(*) from ja1 where rn = 1 group by [application-stage] 
select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
from ja1
where rn = 1 --and [application-stage] = 'PLACED' --and [#Candidate Name] like '%Freeman%'
/*order by [application-positionExternalId] asc,
    CASE [application-stage]
        WHEN 'PLACED' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END asc*/

