

with

-- JOB APPLICATION
  JPInfo as (
  	select JP.jobPostingID as JobID
  		, JP.title as JobTitle
              , case when JP.employmentType is null then 301
                     when JP.employmentType like '%Perm%' then 301
                     when JP.employmentType like '%Opportunity%' then 301
			when JP.employmentType in ('Full-time','General Posting','INTERN FEE','Internal Recruitment') then 301
                     when JP.employmentType like '%Contract%' then 302
                     when JP.employmentType like '%Fixed%' then 302
                     when JP.employmentType like '%Part%' then 302
                     when JP.employmentType like '%Temp%' then 302
		       when JP.employmentType in ('Transactional Opportunity') then 302
                     --when JP.employmentType in ('Temporary','Temp to Perm') then 303
                     else null end as JobType
		, Cl.clientID as ContactID
		, Cl.userID as ClientUserID
		, UC.name as ContactName
		, UC.email as ContactEmail
		, CC.clientCorporationID as CompanyID
		, CC.name as CompanyName
	-- select distinct JP.employmentType
	from bullhorn1.BH_JobPosting JP
	left join bullhorn1.View_ClientContact Cl on JP.clientUserID = Cl.userID
	left join bullhorn1.BH_UserContact UC on JP.clientUserID = UC.userID
	left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
	where 1=1 and JP.title <> '' and (JP.isdeleted <> 1)
)
--select top 100 * from JPInfo order by JobID


, ja0 as (
       select
                JR.jobPostingID as 'application-positionExternalId'
               , JPI.JobType, JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
               , CAI.candidateID as 'application-candidateExternalId'
               , convert(varchar(10),JR.dateAdded,120) as 'dateAdded'
               , JR.status as 'application-stage'
--              , null as 'rejected_date'
        -- select count(*) -- select top 100 * -- select distinct JR.status
        from bullhorn1.BH_JobResponse JR
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID ) CAI on JR.userID = CAI.CandidateUserID
        left join JPInfo JPI on JR.jobPostingID = JPI.JobID
        where CAI.candidateID is not null
--        and JR.status = 'Placed'
UNION
       select 
                PL.jobPostingID as 'application-positionExternalId'
              , JPI.JobType, JPI.CompanyID, JPI.CompanyName,  JPI.ContactID, JPI.ContactName, JPI.ContactEmail, JPI.JobID,  JPI.JobTitle, CAI.CandidateName as '#Candidate Name' --, JR.userID, JPI.ClientUserID, 
              , CAI.candidateID as 'application-candidateExternalId'
              , convert(varchar(10),PL.dateAdded,120) as 'dateAdded'
              , 'PLACED' as 'application-stage'
--              , null as 'rejected_date'
        -- select count(*)
        from bullhorn1.BH_Placement PL
        left join ( select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID ) CAI on PL.userID = CAI.CandidateUserID 
        left join JPInfo JPI on PL.jobPostingID = JPI.JobID
)
, ja1 as (select distinct [application-candidateExternalId], count(*) as total_jobapplication from ja0 group by [application-candidateExternalId])
--select distinct [application-stage], count(*) from ja0 where [rejected_date] is not null group by [application-stage];
--select top 10 * from ja0
--where [application-positionExternalId] = 4997
--where ( [application-positionExternalId] = 6744 and [application-candidateExternalId] = 64995) or ([application-positionExternalId] = 5343 and [application-candidateExternalId] = 193508)
--where CompanyName like 'Nuance%' or CompanyID = 6





-- CANDIDATE
, -- with
  mail1 (ID,userID,email,rn) as (
       select C.candidateID, C.userID
	      , replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --translate special characters
	      , row_number() over(partition by C.candidateID order by C.candidateID) as rn
	from bullhorn1.View_Candidate C left join bullhorn1.BH_UserContact UC on UC.UserID = C.userID
	--cross apply string_split( concat_ws(' ',UC.email, UC.email2, UC.email3),' ')
	cross apply string_split( concat_ws(' ', nullif(convert(nvarchar(max),trim(UC.email)), '') , nullif(convert(nvarchar(max),trim(UC.email2)), ''), nullif(convert(nvarchar(max),trim(UC.email3)), '') ),' ')
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and C.isdeleted <> 1 --and C.status <> 'Archive'
--       and C.userID in (115048)
--       and C.candidateID in (7232,180193)
	)
--select * from mail1 where email <> '' and ID in (7232,180193)

, mail1a (ID,userID,email,rn) as (
       select C.candidateID, C.userID
              , coalesce( nullif( mail1.email,''), concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co')) as email
              , rn
       from bullhorn1.View_Candidate C
       left join mail1 on mail1.ID = C.candidateID --email-deduplication
--       where C.userID in (115048)
--       where candidateID in (7232,180193)
	)
--select * from mail1a where ID in (7232,180193)
	
, mail2 (ID,userID,email,rn,ID_rn) as (
       select ID, userID
              , trim(' ' from email) as email
--              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
--              , row_number() over(partition by ID order by trim(' ' from email)) as ID_rn --distinct if contacts may have more than 1 email
              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
              , row_number() over(partition by ID order by rn asc) as ID_rn --distinct if contacts may have more than 1 email
              --, rn as ID_rn
	from mail1a
	)
--select * from mail2 where ID in (10316, 32224)

, ed (ID,email) as (
       select ID
	      , case when rn > 1 then concat(email,'_',rn) else email end as email
	from mail2
	where email is not NULL and email <> ''
	and ID_rn = 1
	)
--select * from ed where ID in (186063, 188424)
	
, e2 (ID,email) as (select ID, email from mail2 where ID_rn = 2)
, e3 (ID,email) as (select ID, email from mail2 where ID_rn = 3)	
--select * from mail1 where ID in (391, 2447) or email like '%lburlovich@challenger.com.au%'

, can as (
       select
                C.candidateID as 'candidate-externalId', C.userID as 'UserID', C.status
              , coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'firstName'
              , C.middleName as 'middleName'
              , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'lastName'
              , mail2.email as 'email' --UC.email
              , C.dateadded
--              , vclm.dateLastModified
              , C.isdeleted
       -- select count (*)
       from bullhorn1.View_Candidate VC left join bullhorn1.Candidate C on C.candidateID = VC.candidateID
--       left join bullhorn1.BH_UserContact UC ON UC.userID = VC.userID
       left join mail2 on mail2.ID = C.candidateID
--       left join bullhorn1.View_CandidateLastModified vclm on vclm.userID = VC.userID
       where VC.isdeleted <> 1 and mail2.ID_rn = 1 and mail2.email not like '%noemailaddress.co' and mail2.email like '%_@_%.__%'
)
--select * from can where "candidate-externalId" in (1010, 2848, 10316, 18563, 20147, 23114, 30947, 32224, 38642, 48514, 33166, 38643, 45475)
--select * from can where userid in (111039, 161124, 161125)
--and [candidate-lastName] like '%Dempsey%'
--and (C.firstName like '%Jo%' and C.lastName like '%Wallace%')      


, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_UserComment )


select   can.*
       , lc.comments as latest_comment, lc.dateAdded as comment_dateAdded
       , ja1.total_jobapplication
from can
left join (select * from lc where rn = 1) lc on lc.userid = can.userid
left join ja1 on ja1.[application-candidateExternalId] = can.[candidate-externalId]
where can.email in (
       select mail2.email --UC.email
       from bullhorn1.View_Candidate VC left join bullhorn1.Candidate C on C.candidateID = VC.candidateID
       left join bullhorn1.BH_UserContact UC ON UC.userID = VC.userID
       left join mail2 on mail2.ID = C.candidateID
       where VC.isdeleted <> 1 and mail2.ID_rn = 1 and mail2.email not like '%noemailaddress.co' and mail2.email like '%_@_%.__%' --UC.email 
       group by mail2.email having count(*) > 1
       --group by trim(UC.email) having count(*) > 1
       ) -- DUPLICATION NUMBER
--and (can.email like '%jackkelly432@gmail.com%' or can.email like '%buckley95%')
--and "candidate-externalId" in (1010, 2848, 10316, 18563, 20147, 23114, 30947, 32224, 38642, 48514, 33166, 38643, 45475)

--select * from bullhorn1.View_Candidate VC where userid = 14004
--select * from bullhorn1.Candidate where candidateID = 1010