
with
-- EMAIL
--  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
  mail1 (ID,email) as (select C.candidateID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and (C.isdeleted <> 1 and C.status <> 'Archive') /*and C.isPrimaryOwner = 1*/ )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
--, ed2 (ID,email) as (SELECT ID,email FROM ed where rn = 2)
--, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
--, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed where /*ID in (22052,17856,17880,41670,18025)*/ email like '%maria.w.wu@gmail.com%' or email like '%markyeung86@yahoo.com%' or email like '%manlongcheng@hotmail.com%' or email like '%marihirakawa@hotmail.com' or email like '%marvyn_s@hotmail.com%'

select --top 30
         C.candidateID as 'candidate-externalId' , C.userID as '#userID'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	--, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
	, ed.email, ed2.email, ed3.email
from bullhorn1.Candidate C 
--left join ed on C.candidateid = ed.ID -- candidate-email-deduplication
left join (select * from ed where rn = 1) ed on C.candidateid = ed.ID
left join (select * from ed where rn = 2) ed2 on C.candidateid = ed2.ID
left join (select * from ed where rn = 3) ed3 on C.candidateid = ed3.ID
--left join ed2 on C.candidateid = ed2.ID -- candidate-email-deduplication
where C.isdeleted <> 1 and C.status <> 'Archive'	
--and ed2.ID is not null and ed3.ID is not null
--and ed0.ID is not null
--and (select ID ed.rn > 1 )
--where (ed.email like '%maria.w.wu@gmail.com%' or ed.email like '%markyeung86@yahoo.com%' or ed.email like '%manlongcheng@hotmail.com%' or ed.email like '%marihirakawa@hotmail.com' or ed.email like '%marvyn_s@hotmail.com%')
--and c.candidateID in (22054, 22053, 22052, 48122, 46090, 17856, 64457, 63956, 17880, 108072, 108069, 41670, 80108, 18026, 18025)
--and ed.email like 'Joellelee_93@hotmail.com%'
and C.candidateID in (124300, 124119)




select --top 30
         C.candidateID as 'candidate-externalId' , C.userID as '#userID'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
       , C.dateadded
       , UC.email, UC.email2, UC.email3
       ,*
from bullhorn1.Candidate C
left join bullhorn1.BH_UserContact UC on C.userID = UC.UserID 
where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and (C.isdeleted <> 1 and C.status <> 'Archive')
and C.candidateID in (124300, 124119,22054, 22053, 48122, 46090, 17856, 64457, 63956, 17880, 108072, 108069, 41670, 80108, 18026, 18025) --or UC.email like '%manlongcheng@hotmail.com%')
