

with
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(UC.email)),',',ltrim(rtrim(UC.email2)),',',ltrim(rtrim(UC.email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from bullhorn1.BH_UserContact UC left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID where Cl.isPrimaryOwner = 1)
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--select * from ed


-----MAIN SCRIPT------
select  convert(varchar(500),Cl.clientID) as 'contact-externalId'
	, case when (ltrim(replace(UC.firstName,'?','')) = '' or  UC.firstName is null) then 'Firstname' else ltrim(replace(UC.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(UC.lastName,'?','')) = '' or  UC.lastName is null) then concat('Lastname-',Cl.clientID) else ltrim(replace(UC.lastName,'?','')) end as 'contact-Lastname'
	, case UC.namePrefix
                when 'Dr' then 'Dr.'
                when 'Dr.' then 'Dr.'
                when 'Micmr' then ''
                when 'Miss' then 'Miss.'
                when 'Mme' then ''
                when 'Mr' then 'Mr.'
                when 'Mr.' then 'Mr.'
                when 'Mrs' then 'Mrs.'
                when 'Mrs.' then 'Mrs.'
                when 'Ms' then 'Ms.'
                when 'Ms.' then 'Ms.'
                when 'nr' then ''
        end as 'Title'
	, UC.nickName as 'Preferred Name' --VC nick_name
	, UC.Mobile as 'Contact-MobilePhone'
	, UC.phone2 as 'Contact-HomePhone'
       , e2.email as 'contact-PersonalEmail'
	-- select count(*) --7487 -- select distinct UC.namePrefix -- select *
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
--left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
--left join mail5 ON Cl.userID = mail5.ID
left join e1 ON Cl.userID = e1.ID -- candidate-email
left join e2 ON Cl.userID = e2.ID -- candidate-email
--left join e3 ON Cl.userID = e3.ID -- candidate-email
--left join e4 ON Cl.userID = e4.ID -- candidate-email
left join ed ON Cl.userID = ed.ID -- candidate-email-DUPLICATION
where isPrimaryOwner = 1 --and UC.clientCorporationID = 1284