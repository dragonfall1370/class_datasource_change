	        
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
	where 1=1 and JP.title <> '' )
 --select * from JPInfo order by JobID


, ja0 as (
       select
                JR.jobPostingID as 'application-positionExternalId'
               , JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
               , CAI.candidateID as 'application-candidateExternalId'
               , convert(varchar(10),JR.dateAdded,120) as 'dateAdded'
               , coalesce(nullif(case
when JR.status = 'Acc/Reject' then 'OFFERED'
when JR.status = 'Advert' then 'SHORTLISTED'
when JR.status = 'Candidate Interested' then 'SHORTLISTED'
when JR.status = 'Candidate Not Interested' then 'SHORTLISTED'-- > REJECTED'
--when JR.status = 'Canvass CC' then 'N/A'
when JR.status = 'Client Rejected' then 'SENT'-- > REJECTED'
when JR.status = 'Client Submission' then 'SENT'
--when JR.status = 'Clone Job' then 'N/A'
--when JR.status = 'CV F/Up' then 'N/A'
when JR.status = 'CV Sent' then 'SENT'
--when JR.status = 'DT Action' then 'N/A'
when JR.status = 'Email Send' then 'SHORTLISTED'
--when JR.status = 'F/Up PP' then 'N/A'
when JR.status = 'First Day' then 'OFFERED'--'PLACED'
--when JR.status = 'G''tee Rem' then 'N/A'
--when JR.status = 'GetInTouch' then 'N/A'
when JR.status = 'Int Canx/R' then 'FIRST_INTERVIEW'
when JR.status = 'Int F/Up' then 'FIRST_INTERVIEW'
when JR.status = 'Int Feedbk' then 'FIRST_INTERVIEW'
when JR.status = 'Interview' then 'FIRST_INTERVIEW'
when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
when JR.status = 'Longlist' then 'SHORTLISTED'
when JR.status = 'Offer Extended' then 'OFFERED'
when JR.status = 'Perm Place' then 'OFFERED' --'PERMANENT PLACED'
--when JR.status = 'Phone Call' then 'N/A'
when JR.status = 'Placed' then 'OFFERED' --'PERMANENT PLACED'
when JR.status = 'Placement' then 'OFFERED' --'PERMANENT PLACED'
--when JR.status = 'PleaseCall' then 'N/A'
when JR.status = 'Reject' then 'SHORTLISTED'-- > REJECTED'
--when JR.status = 'Reopen Job' then 'N/A'
--when JR.status = 'Sales Rep Rejected' then 'N/A'
when JR.status = 'Shortlist' then 'SHORTLISTED'
when JR.status = 'Shortlisted' then 'SHORTLISTED'
--when JR.status = 'SMS' then 'N/A'
when JR.status = 'Submitted' then 'SENT'

                            else '' end, ''), '') as 'application-stage'  --, JR.status as '#Original-status' -- SHORTLISTED, SENT, FIRST_INTERVIEW, SECOND_INTERVIEW, OFFERED, PLACEMENT_PERMANENT, PLACEMENT_CONTRACT, PLACEMENT_TEMP, ONBOARDING.
        -- select count(*) --5995 -- select distinct JR.status -- select top 100 *
        from bullhorn1.BH_JobResponse JR
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail  from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID ) CAI on JR.userID = CAI.CandidateUserID
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
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID ) CAI on PL.userID = CAI.CandidateUserID 
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
       left join (select jobPostingID from bullhorn1.BH_JobPosting where isdeleted <> 1 and status <> 'Archive') job on job.jobPostingID = ja0.[application-positionExternalId]
       left join (select candidateid from bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') candidate on candidate.candidateid = ja0.[application-candidateExternalId]
       where "application-Stage" not like 'CANDIDATE%' and "application-Stage" <> '' and (job.jobPostingID is not null and candidate.candidateid is not null)
       )



--select [application-stage], count(*) from ja1 where rn = 1 group by [application-stage] 
select "application-positionExternalId","application-candidateExternalId","application-Stage", "dateAdded", current_timestamp as 'TIME_REJECTED'
    , 301 as POSITIONCANDIDATE_status
    , 3 as OFFER_draft_offer --used to move OFFERED to PLACED in VC [offer]
    , 2 as INVOICE_status --used to update invoice status in VC [invoice] as 'active'
    , 1 as INVOICE_renewal_index --default value in VC [invoice]
    , 1 as INVOICE_renewal_flow_status --used to update flow status in VC [invoice] as 'placement_active'
    , 1 as INVOICE_valid
from ja1
where rn = 1 --and [application-stage] = 'PLACED' --and [#Candidate Name] like '%Freeman%'

