/*
'SHORTLISTED'then 102
'SENT'then 103
'FIRST_INTERVIEW'then 104
'SECOND_INTERVIEW'then 105
'THIRD_INTERVIEW'then 106
'FOURTH_INTERVIEW'then 107
'FIFTH_INTERVIEW'then 108
'LAST_INTERVIEW_ROUND' then 123
'OFFERED'then 200
'PLACED'then 300
'PLACEMENT_PERMANENT'then 301
'PLACEMENT_CONTRACT'then 302
'PLACEMENT_TEMP'then 303
'ONBOARDING'then 310
*/

with
  JPInfo as (
  	select JP.jobPostingID as JobID
  		, JP.title as JobTitle
              , case when JP.employmentType is null then 301
                     --when JP.employmentType in ('Permanent','Opportunity') then 301
                     --when JP.employmentType in ('Contract','Fixed Contract','Part-time','Temporary') then 302
	              when jp.employmentType in ('Full-time','General Posting','INTERN FEE','Internal Recruitment','Perm','Permanent','Strategic Opportunity') then 301
	              when jp.employmentType in ('Contract','Contract Flat Fee','Temp','Temp to Perm','Temporary - Contractor','Temporary - Employee','Transactional Opportunity') then 302
                     --when JP.employmentType in ('Temporary','Temp to Perm') then 303
                     else null end as JobType
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
	where 1=1 and JP.title <> '' and (JP.isdeleted <> 1 and JP.status <> 'Archive')
)
--select * from JPInfo order by JobID


, ja0 as (
       select
                JR.jobPostingID as 'application-positionExternalId', JPI.JobType
               , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
               , CAI.candidateID as 'application-candidateExternalId'
               , convert(varchar(10),JR.dateAdded,120) as 'dateAdded'
               , coalesce(nullif(case
when JR.status = '1st Interview' then 'FIRST_INTERVIEW'
when JR.status = '2nd Interview' then 'SECOND_INTERVIEW'
when JR.status = '3rd Interview' then 'SECOND_INTERVIEW'
when JR.status = 'Candidate Interested' then 'SHORTLISTED'
when JR.status = 'Candidate Not Interested' then 'SHORTLISTED'--> Rejected'
when JR.status = 'Candidate Presentation' then 'SHORTLISTED'
when JR.status = 'Client Interview Booked' then 'FIRST_INTERVIEW'
when JR.status = 'Client Rejected' then 'SENT'-- > Rejected'
when JR.status = 'Coordinator Rejected' then 'SHORTLISTED'-- > Rejected'
when JR.status = 'Final Interview' then 'SECOND_INTERVIEW'
when JR.status = 'Internal Interview' then 'FIRST_INTERVIEW'
when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
--when JR.status = 'New Lead' then 'N/A'
--when JR.status = 'New Lead"' then 'N/A'
when JR.status = 'New Submission' then 'SHORTLISTED'
when JR.status = 'Offer Pending' then 'OFFERED'
when JR.status = 'Offer Rejected' then 'OFFERED'-- > Rejected'
when JR.status = 'Placed' then 'OFFERED' --PLACED
when JR.status = 'Sales Rep Rejected' then 'SHORTLISTED'-- > Rejected'
when JR.status = 'Submitted' then 'SENT'
                            else '' end, ''), '') as 'application-stage'  --, JR.status as '#Original-status' -- SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT, PLACEMENT_CONTRACT, PLACEMENT_TEMP, ONBOARDING.
        -- select count(*) --5995 -- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID ) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
UNION
       select 
                PL.jobPostingID as 'application-positionExternalId', JPI.JobType
              , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
              , CAI.candidateID as 'application-candidateExternalId'
              , convert(varchar(10),PL.dateAdded,120) as 'dateAdded'
              , 'OFFERED' as 'application-stage'
        -- select count(*)
        from bullhorn1.BH_Placement PL
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID ) CAI on PL.userID = CAI.CandidateUserID 
        left join JPInfo JPI on PL.jobPostingID = JPI.JobID
)
--select top 2000 * from ja0
--where ( [application-positionExternalId] = 6744 and [application-candidateExternalId] = 64995) or ([application-positionExternalId] = 5343 and [application-candidateExternalId] = 193508)
--where CompanyName like 'Nuance%' or CompanyID = 6

, ja1 ("application-positionExternalId","application-candidateExternalId", JobType, "application-Stage","dateAdded", rn) as (
       SELECT 
              "application-positionExternalId"
              ,"application-candidateExternalId"
              , JobType
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
       left join (select jobPostingID from bullhorn1.BH_JobPosting where isdeleted <> 1 and status <> 'Archive') job on job.jobPostingID = ja0.[application-positionExternalId]
       left join (select candidateid from bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') candidate on candidate.candidateid = ja0.[application-candidateExternalId]
       where "application-Stage" not like 'CANDIDATE%' and "application-Stage" <> '' and (job.jobPostingID is not null and candidate.candidateid is not null)
       )



--select [application-stage], count(*) from ja1 where rn = 1 group by [application-stage] 
select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
    , JobType as POSITIONCANDIDATE_status
    , 3 as OFFER_draft_offer --used to move OFFERED to PLACED in VC [offer]
    , 2 as INVOICE_status --used to update invoice status in VC [invoice] as 'active'
    , 1 as INVOICE_renewal_index --default value in VC [invoice]
    , 1 as INVOICE_renewal_flow_status --used to update flow status in VC [invoice] as 'placement_active'
    , 1 as INVOICE_valid
from ja1
where rn = 1 --and [application-stage] = 'PLACED' --and [#Candidate Name] like '%Freeman%'

