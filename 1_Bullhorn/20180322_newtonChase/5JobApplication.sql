
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

, jobapp as (
        select
                  JPI.ClientUserID
                --, JPI.ContactID, JPI.ContactName, JPI.ContactEmail
                --, JPI.CompanyID, JPI.CompanyName, JPI.JobTitle
                --, JR.userID
                , JR.jobPostingID as 'application-positionExternalId'
                , CAI.CandidateName as '#Candidate Name'
                , CAI.candidateID as 'application-candidateExternalId'	
                --, JR.status as '#application-stage' --This field only accepts: SHORTLISTED,SENT,FIRST_INTERVIEW,SECOND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
                , Coalesce(NULLIF(case
when JR.status = 'Acc/Reject' then 'OFFERED'
when JR.status = 'Advert' then 'SHORTLISTED'
when JR.status = 'Candidate Interested' then 'SHORTLISTED'
when JR.status = 'Candidate Not Interested' then 'SHORTLISTED'
when JR.status = 'Canvass CC' then ''
when JR.status = 'Client Rejected' then 'SENT'
when JR.status = 'Client Submission' then 'SENT'
when JR.status = 'Clone Job' then ''
when JR.status = 'CV F/Up' then ''
when JR.status = 'CV Sent' then 'SENT'
when JR.status = 'DT Action' then ''
when JR.status = 'Email Send' then 'SHORTLISTED'
when JR.status = 'F/Up PP' then ''
when JR.status = 'First Day' then 'PLACED'
when JR.status = 'G''tee Rem' then ''
when JR.status = 'GetInTouch' then ''
when JR.status = 'Int Canx/R' then 'FIRST_INTERVIEW'
when JR.status = 'Int F/Up' then 'FIRST_INTERVIEW'
when JR.status = 'Int Feedbk' then 'FIRST_INTERVIEW'
when JR.status = 'Interview' then 'FIRST_INTERVIEW'
when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
when JR.status = 'Longlist' then 'SHORTLISTED'
when JR.status = 'Offer Extended' then 'OFFERED'
when JR.status = 'Perm Place' then 'PLACED'
when JR.status = 'Phone Call' then ''
when JR.status = 'Placed' then 'PLACED'
when JR.status = 'Placement' then 'PLACED'
when JR.status = 'PleaseCall' then ''
when JR.status = 'Reject' then 'SENT'
when JR.status = 'Reopen Job' then ''
when JR.status = 'Sales Rep Rejected' then ''
when JR.status = 'Shortlist' then 'SHORTLISTED'
when JR.status = 'Shortlisted' then 'SHORTLISTED'
when JR.status = 'SMS' then ''
when JR.status = 'Submitted' then 'SENT'
                        else '' end, ''), '') as 'application-stage'
                , JR.status as 'original-status'             
        --select count(*) --5995-- select distinct JR.status -- select *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null )


select * 
from jobapp 
where [application-stage] <> '' and [application-stage] not like 'CANDIDATE%' --and [#Candidate Name] like '%Freeman%'
order by [application-positionExternalId] desc,
    CASE [application-stage]
        WHEN 'PLACED' THEN 1
        WHEN 'OFFERED' THEN 2
        WHEN 'SECOND_INTERVIEW' THEN 3
        WHEN 'FIRST_INTERVIEW' THEN 4
        WHEN 'SENT' THEN 5
        WHEN 'SHORTLISTED' THEN 6
    END desc
--order by [application-positionExternalId], FIELD([application-stage],'PLACED','OFFERED','2ND_INTERVIEW','1ST_INTERVIEW','SENT','SHORTLISTED') desc

--select [application-stage], count(*) from jobapp group by [application-stage]