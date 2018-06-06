
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
                        when JR.status = 'Placed' then 'PLACED'
                        when JR.status = 'Accepted' then 'OFFERED'
                        when JR.status = 'Close to Offer' then 'OFFERED'
                        when JR.status = 'Offer Accepted' then 'OFFERED'
                        when JR.status = 'Offer Extended' then 'OFFERED'
                        when JR.status = 'Offer Rejected' then 'OFFERED'
                        when JR.status = 'Offer Withdrawn' then 'OFFERED'
                        when JR.status = 'Offer Wthdrawn' then 'OFFERED'
                        when JR.status = 'Offered' then 'OFFERED'
                        when JR.status = 'Candidate Counter Offered' then 'OFFERED'
                        when JR.status = '10th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '2nd Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '3rd Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '4th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '5th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '6th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '7th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '8th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = '9th Interview' then 'SECOND_INTERVIEW'
                        when JR.status = 'Final Interview' then 'SECOND_INTERVIEW'
                        when JR.status = 'Assessment Centre' then 'SECOND_INTERVIEW'
                        when JR.status = '1st Interview' then 'FIRST_INTERVIEW'
                        when JR.status = 'Interview' then 'FIRST_INTERVIEW'
                        when JR.status = 'Interview Requested' then 'FIRST_INTERVIEW'
                        when JR.status = 'Online Testing' then 'FIRST_INTERVIEW'
                        when JR.status = 'Rescheduled Interview' then 'FIRST_INTERVIEW'
                        when JR.status = 'Awaiting Feedback' then 'SENT'
                        when JR.status = 'Candidate Feedback' then 'SENT'
                        when JR.status = 'Client Feedback' then 'SENT'
                        when JR.status = 'CV Send' then 'SENT'
                        when JR.status = 'Submitted' then 'SENT'
                        when JR.status = 'On Hold' then 'SHORTLISTED'
                        else '' end, ''), '') as 'application-stage'
                , JR.status as 'original-status'             
        --select count(*) --5995-- select distinct JR.status
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null )

--select [original-status], count(*) from jobapp group by [original-status]
--select [application-stage], count(*) from jobapp group by [application-stage]

select * from jobapp where [application-stage] <> '' --and [#Candidate Name] like '%Freeman%'
order by [application-positionExternalId] desc

