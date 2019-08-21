-- ALTER DATABASE [temptingventures] SET COMPATIBILITY_LEVEL = 130
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

select 
	 Cl.clientID as 'contact-externalId'
       , Stuff(
                                   Coalesce('' + NULLIF(UC.address1, ''), '')
                               + Coalesce(', ' + NULLIF(UC.address2, ''), '')
                               + Coalesce(', ' + NULLIF(UC.city, ''), '')
                               + Coalesce(', ' + NULLIF(UC.state, ''), '')
                               + Coalesce(', ' + NULLIF(tc.country, ''), '')
                       , 1, 1, '') as 'address'
       , UC.city
       , UC.state
       , UC.zip as 'post_code'
       , tc.ABBREVIATION as 'country_code' --UC.countryID
       , ltrim(Stuff(
                         Coalesce(', ' + NULLIF(UC.city, ''), '')
                        + Coalesce(', ' + NULLIF(UC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 1, '') ) as 'locationName'
, Cl.division as 'division->department'
, e2.email as 'personal_email'
, UC.namePrefix as 'namePrefix->gender_title'
, UC.NickName as 'nickname->nickname'

-- select count(*) --7487 -- select top 10 *
from bullhorn1.BH_Client Cl --where isPrimaryOwner = 1
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
left join e2 ON Cl.userID = e2.ID -- candidate-email
left join tmp_country tc ON UC.countryID = tc.code
where isPrimaryOwner = 1 