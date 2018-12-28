
select 
        concat(PL.jobPostingID,'_',PL0.ID) as 'application-positionExternalId'
       ,C.candidateid as 'application-candidateExternalId' , PL0.candidateid
       , 'PLACED' as 'application-stage'
       -- select * 
       from BullhornPlacements PL0 left join bullhorn1.BH_Placement PL on PL0.id = PL.placementID
       left join bullhorn1.Candidate C on c.userid = PL0.candidateid
       where C.candidateid is not null

/*with
 jobapp as (
        select
                  --JPI.ClientUserID
                --, JPI.ContactID, JPI.ContactName, JPI.ContactEmail
                --, JPI.CompanyID, JPI.CompanyName, JPI.JobTitle
                --, JR.userID
                  concat(JPI.jobPostingID,'_',JPI.ID) as 'application-positionExternalId' --, JR.jobPostingID 
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
        -- select count(*) --5995-- select distinct JR.status -- select *
        from bullhorn1.BH_JobResponse JR  --JR.jobPostingID in (76938, 100453, 120112)
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join ( 
              select PL.jobPostingID, PL.ID
              -- select count(*)
              from bullhorn1.BH_JobPosting a
              left join (select PL.jobPostingID,PL0.*  from BullhornPlacements PL0 left join bullhorn1.BH_Placement PL on PL0.id = PL.placementID ) PL  on PL.jobPostingID = a.jobPostingID
              where PL.jobPostingID is not null
              ) JPI on JPI.jobPostingID = JR.jobPostingID
        where JPI.jobPostingID is not null --and CAI.candidateID is not null 
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

*/