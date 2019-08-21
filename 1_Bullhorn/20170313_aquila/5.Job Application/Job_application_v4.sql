with JPInfo as (select JP.jobPostingID as JobID, JP.title as JobTitle
, Cl.clientID as ContactID, Cl.userID as ClientUserID
, UC.name as ContactName, UC.email as ContactEmail
, CC.clientCorporationID as CompanyID, CC.name as CompanyName
from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_Client Cl on JP.clientUserID = Cl.userID
left join bullhorn1.BH_UserContact UC on JP.clientUserID = UC.userID
left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
where 1=1
and Cl.isPrimaryOwner = 1)

--Search information in JPInfo table:
--select * from JPInfo order by JobID

, CandidateInfo as (select CA.userID as CandidateUserID, CA.candidateID
, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail
from bullhorn1.BH_Candidate CA
left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID
where CA.isPrimaryOwner = 1)

/* Search information in CandidateInfo table
select * from CandidateInfo
order by CandidateUserID
*/

select JPI.ClientUserID, JPI.ContactID, JPI.ContactName, JPI.ContactEmail
, JPI.CompanyID, JPI.CompanyName, JPI.JobTitle
, JR.jobPostingID as 'application-positionExternalId', JR.userID, CAI.candidateID as 'application-candidateExternalId'
, CAI.CandidateName

--, JR.status as 'application-stage'
, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	JR.status,'Candidate Interested','SHORTLISTED')
	,'Candidate Not Interested','SHORTLISTED')
	,'Client Rejected','SHORTLISTED')
	,'Client Submission','SENT')
	,'Interview Scheduled','1ST_INTERVIEW')
	,'New Lead','SHORTLISTED')
	,'Offer Extended','OFFERED')
	,'Offer Rejected','OFFERED')
	,'Placed','PLACED')
	,'Sales Rep Rejected','SHORTLISTED')
	,'Submitted','SHORTLISTED')
	as 'application-stage'
from bullhorn1.BH_JobResponse JR
left join CandidateInfo CAI on JR.userID = CAI.CandidateUserID
left join JPInfo JPI on JR.jobPostingID = JPI.JobID