
--ALTER DATABASE [nlsearch2] SET COMPATIBILITY_LEVEL = 130

with
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
--, oe3 (ID,email) as (select ID, email from mail4 where rn = 3)
--, oe4 as (select ID, email from mail4 where rn = 4)
--select id from e1 where id in (select id from ed where rn = 2)
--select count(*) from ed

--select * from bullhorn1.Candidate where userid in (select id from mail1)

	select --top 200
                  C.userID as '#userID'
		, C.candidateID as 'candidate-externalId'
--		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
--      , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
--		, C.middleName as 'candidate-middleName'
		--, iif(e1.ID in (select ID from ed where rn = 2 ),concat(ed.email,'_',ed.rn), iif(e1.email = '' or e1.email is null, concat('candidate_',cast(C.userID as varchar(200)),'@noemailaddress.co'),e1.email) ) as 'candidate-email'
--		, e1.email
, iif(ed.rn = 2,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
		, e2.email as 'candidate-workEmail' --<<
	from bullhorn1.Candidate C --where C.isPrimaryOwner = 1 --8545
	--left join e1 on C.userID = e1.ID
	left join e2 on C.userID = e2.ID
	left join ed on C.userID = ed.ID -- candidate-email-DUPLICATION
	where C.isPrimaryOwner = 1
	
	

