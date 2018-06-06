
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
                                        when JR.status = 'Offer' then 'OFFERED'
                                        when JR.status = 'Offer Extended' then 'OFFERED'
                                        when JR.status = 'Offer Rejected' then 'OFFERED'
                                        when JR.status = '2nd Interview' then 'SECOND_INTERVIEW'
                                        when JR.status = '3rd Interview' then 'SECOND_INTERVIEW'
                                        when JR.status = 'Final Interview' then 'SECOND_INTERVIEW'
                                        when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
                                        when JR.status = 'CV Sent' then 'SENT'
                                        when JR.status = 'Submitted' then 'SENT'
                                        when JR.status = 'Candidate Interested' then 'SHORTLISTED'
                                        when JR.status = 'Shortlisted' then 'SHORTLISTED'
                        else '' end, ''), '') as 'application-stage'
                , JR.status as ORIGIN           
        --select count(*) --5995-- select distinct JR.status -- select *
        from bullhorn1.BH_JobResponse JR  --JR.jobPostingID in (76938, 100453, 120112)
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null 
            
        )
--select * from jobapp where [application-positionExternalId] in (185,164,178,36)

select * from jobapp where [application-stage] <> '' order by [application-positionExternalId] desc 
--select [application-stage], count(*) from jobapp group by [application-stage]



----------------
with t as (SELECT userid, payRate,dateAdded, rn = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_JobResponse)
--select * from t where rn =1
select       t.userID as '#userID'
		, c.candidateID as 'candidate-externalId'
		, t.payrate
from t
left join bullhorn1.Candidate C on c.userid = t.userid
where t.payrate is not null and  c.isPrimaryOwner = 1