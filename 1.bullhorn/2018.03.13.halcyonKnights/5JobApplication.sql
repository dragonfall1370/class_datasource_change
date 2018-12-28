
select * from bullhorn1.BH_Placement


select PL.jobPostingID as jobPostingID
       , a.clientUserID as '#UserID', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName', CC.clientCorporationID as '#CompanyID', cc.name as '#CompanyName'
from bullhorn1.BH_JobPosting a --where a.jobPostingID in (544,843,725,964,1109,1225,1323,1409,1444,1471,1540) --(76938, 100453, 120112)
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join bullhorn1.BH_Placement PL on PL.jobPostingID = a.jobPostingID
--left join mail5 ON a.userID = mail5.ID
--where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
where cc.name like '%AppDynamics%'
and PL.jobPostingID is not null --and a.jobPostingID in (1,164,178,36)
--order by a.jobPostingID asc



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
                , JPI.ContactID, JPI.ContactName, JPI.ContactEmail
                , JPI.CompanyID, JPI.CompanyName, JPI.JobTitle
                --, JR.userID
                , JR.jobPostingID as 'application-positionExternalId'
                , CAI.CandidateName as '#Candidate Name'
                , CAI.candidateID as 'application-candidateExternalId'	
                --, JR.status as '#application-stage' --This field only accepts: SHORTLISTED,SENT,FIRST_INTERVIEW,SECOND_INTERVIEW,OFFERED,PLACED, INVOICED, Other values will not be recognized.
                , Coalesce(NULLIF(case
                            when JR.status = ' Final Interview' then 'SECOND_INTERVIEW'
                            when JR.status = ' Interview 2' then 'SECOND_INTERVIEW'
                            when JR.status = ' Interview 3' then 'SECOND_INTERVIEW'
                            when JR.status = 'Candidate Interested' then 'SHORTLISTED'
                            when JR.status = 'Client Rejected' then 'SENT'
                            when JR.status = 'Client Submission' then 'SENT'
                            when JR.status = 'Interview 1' then 'FIRST_INTERVIEW'
                            when JR.status = 'Interview Scheduled' then 'FIRST_INTERVIEW'
                            when JR.status = 'Offer Extended' then 'OFFERED'
                            when JR.status = 'Offer Rejected' then 'OFFERED'
                            when JR.status = 'Placed' then 'PLACED'
                            when JR.status = 'Submission' then 'SHORTLISTED'
                            when JR.status = 'Submitted' then 'SHORTLISTED'
                        else '' end, ''), '') as 'application-stage'
                , JR.status as 'original-status'             
        --select count(*) --5995-- select distinct JR.status -- select *
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        --where CAI.candidateID is not null 
        )


select * from jobapp where CompanyID = 3114 and [application-stage] = 'PLACED'
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