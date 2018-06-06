
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
                --, JR.status as '#application-stage' --This field only accepts: SHORTLISTED,SENT,1ST_INTERVIEW,2ND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
                , Coalesce(NULLIF(case when JR.status = 'Placed' then 'PLACED'
                        when JR.status = 'CV Sent' then 'OFFERED'
                        when JR.status = 'Final Interview' then 'OFFERED'
                        when JR.status = 'New Lead' then 'OFFERED'
                        when JR.status = 'Offer Accepted' then 'OFFERED'
                        when JR.status = 'Offer Extended' then 'OFFERED'
                        when JR.status = 'Offer Rejected' then 'OFFERED'
                        when JR.status = 'Under Offer' then 'OFFERED'
                        when JR.status = '2nd Interview' then '2ND+_INTERVIEW'
                        when JR.status = '3rd Interview' then '2ND+_INTERVIEW'
                        when JR.status = '4th Interview' then '2ND+_INTERVIEW'
                        when JR.status = '1st Interview' then '1ST_INTERVIEW'
                        when JR.status = 'Shortlisted' then 'SHORTLISTED'
                        when JR.status = 'Submitted' then 'SHORTLISTED'
                        when JR.status = 'Subsequent' then 'SHORTLISTED'
                        when JR.status = 'Shortlisted' then 'SHORTLISTED'
                        else '' end, ''), '') as 'application-stage'             
        --select count (*) --39194
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null )

select * from jobapp where [application-stage] <> ''

--select distinct JR.status from bullhorn1.BH_JobResponse JR