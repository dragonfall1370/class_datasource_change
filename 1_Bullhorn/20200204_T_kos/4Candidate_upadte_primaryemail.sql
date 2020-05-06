/*
       select  C.candidateID, C.userID, C.isdeleted, C.status
              , UC.email, UC.email2, UC.email3
	from bullhorn1.BH_UserContact UC 
	left join bullhorn1.Candidate C on C.userID = UC.UserID
	where C.userID in (115048)
	
	*/
	
with
mail1 (ID,userID,email) as (
       select distinct C.candidateID, C.userID
	      , replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
	from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID
	cross apply string_split( concat(UC.email,' ',UC.email2,' ',UC.email3) ,' ')
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and C.isdeleted <> 1 --and C.status <> 'Archive'
	--and C.userID in (115048)
--	and C.candidateID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)
--       select REFERENCE
--	, replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
--	from PROP_EMAIL
--	cross apply string_split(EMAIL_ADD,' ')
--	where EMAIL_ADD like '%_@_%.__%' and REFERENCE in (61065,43945)
	)
--select * from mail1 where email <> '' and ID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)

, mail1a (ID,email) as (
       select --top 100
              C.candidateID --, C.userID as '#userID'
              , coalesce( nullif( mail1.email,''), concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co')) as email
       from bullhorn1.Candidate C
       left join mail1 on mail1.ID = C.candidateID -- candidate-email-deduplication
       --where C.userID in (115048)
--       where candidateID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)
	)
--select * from mail1a where ID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)
	
, mail2 (ID,email,rn,ID_rn) as (
       select distinct ID --, userID
              , trim(' ' from email) as email
              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
              , row_number() over(partition by ID order by trim(' ' from email)) as ID_rn --distinct if contacts may have more than 1 email
	from mail1a
	--where email like '%_@_%.__%'
	)
--select * from mail2 where ID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)

, ed (ID,email) as (
       select ID
	      , case when rn > 1 then concat(email,'_',rn)
	else email end as email
	from mail2
	where email is not NULL and email <> ''
	and ID_rn = 1
	)
--select * from ed where ID in (186063, 188424)
	
, e2 (ID,email) as (select ID, email from mail2 where ID_rn = 2)
, e3 (ID,email) as (select ID, email from mail2 where ID_rn = 3)	
--select * from mail1 where ID in (391, 2447) or email like '%lburlovich@challenger.com.au%'


select --top 1
         C.candidateID as 'candidate-externalId' , C.userID as '#userID'
	, ed.email as 'candidate-email' 
	, UC.email as email1, UC.email2, UC.email3
from bullhorn1.Candidate C
left join ed on ed.ID = C.candidateID -- candidate-email-deduplication
left join bullhorn1.BH_UserContact UC on UC.UserID  = C.userID
where C.isdeleted <> 1
--and C.userID in (115048)
and ed.email like '%@noemailaddress.co'
--and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
--and concat (C.FirstName,' ',C.LastName) like '%Partha%'