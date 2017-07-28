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
	where 1=1
	and Cl.isPrimaryOwner = 1)
--select * from JPInfo order by JobID

, CandidateInfo as (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1)

select
	  JPI.ClientUserID
	--, JPI.ContactID, JPI.ContactName, JPI.ContactEmail
	--, JPI.CompanyID, JPI.CompanyName, JPI.JobTitle
	--, JR.userID
	--, JR.jobPostingID as 'application-positionExternalId'
	, CAI.CandidateName
	, CAI.candidateID as 'application-candidateExternalId'	
	--, JR.status as 'application-stage' --This field only accepts: SHORTLISTED,SENT,1ST_INTERVIEW,2ND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
	, case when JR.status = 'Candidate Interested' then 'SHORTLISTED'
                when JR.status = 'AM Rep Rejected' then 'SHORTLISTED'
                when JR.status = 'Attach Candidate to Job' then 'SHORTLISTED'
                when JR.status = 'Candidate Not Interested' then 'SHORTLISTED'
                when JR.status = 'Candidate Not Selected' then 'SHORTLISTED'
                when JR.status = 'Shortlist' then 'SHORTLISTED'
                when JR.status = 'Client Rejected' then 'SHORTLISTED'
                when JR.status = 'Client Submission' then 'SENT'
                when JR.status = 'SendOut' then 'SENT'
                when JR.status = 'Interview Scheduled' then '1ST_INTERVIEW'
                when JR.status = 'Offer Extended' then 'OFFERED'
                when JR.status = 'Offer Rejected' then 'OFFERED'
                when JR.status = 'Placed' then 'PLACED'
                when JR.status = 'Sales Rep Rejected' then 'SHORTLISTED'
                when JR.status = 'Submitted' then 'SHORTLISTED'
                when JR.status = 'CV Sent' then 'SENT'
                when JR.status = '1 - CV Sent' then 'SENT'
                when JR.status = '2 - SHORTLISTED' then 'SHORTLISTED'
                when JR.status = '3 - Shortlisted' then 'SHORTLISTED'
                when JR.status = '4 - SHORTLISTED' then 'SHORTLISTED'
                when JR.status = '5 - Recruiter Rejected' then 'SHORTLISTED'
                when JR.status = '6 - SHORTLISTED' then 'SHORTLISTED'
                when JR.status = '7 - OFFERED' then 'OFFERED'
                when JR.status = 'New Lead' then 'SHORTLISTED'
                else '' end as 'application-stage'
from bullhorn1.BH_JobResponse JR
left join CandidateInfo CAI on JR.userID = CAI.CandidateUserID
left join JPInfo JPI on JR.jobPostingID = JPI.JobID