
with
-- EMAIL
  mail1 (ID,email) as (select ContactUniqueID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(email,',',HomeEmail),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from contacts where email like '%_@_%.__%' or HomeEmail like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID, email1, email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		--left join (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 ) */
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
--, oe3 as (select ID, email from mail4 where rn = 3)
--, oe4 as (select ID, email from mail4 where rn = 4)
--mail (ID,email,rn) as ( SELECT ContactUniqueID, concat(ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))),',',ltrim(rtrim(CONVERT(NVARCHAR(MAX), HomeEmail))) ) , r1 = ROW_NUMBER() OVER (PARTITION BY email ORDER BY email desc) FROM contacts where email like '%_@_%.__%' or HomeEmail like '%_@_%.__%')
--, e1 as (select ID, email from mail where rn = 1)
--, ed (ID,email,rn) as (select ID, email, rn from mail where rn > 1)
--select top 200 * from mail4 where ID in ('2456','995','2154')
/*
select
          c.ContactUniqueID
          ,e1.*
          , coalesce(nullif(e1.email,''),concat(ed.email,'_',ed.rn)) as email
          --,ed.*
          --, iif(e1.ID not in (select ID from ed),concat(ed.email,'_',ed.rn), e1.email) as 'contact-email' --, c.Email as 'contact-email#'
from contacts c
left join e1 ON e1.ID = c.ContactUniqueID -- candidate-email
left join ed ON ed.ID = c.ContactUniqueID -- candidate-email-DUPLICATION
where c.ContactUniqueID in ('2456','995','2154')
*/


select --top 10
          c.ContactUniqueID as 'candidate-externalid'
        , coalesce(nullif(c.Forename,''),'No FirstName') as 'candidate-firstName'
        , coalesce(nullif(c.Surname,''),'No LastName') as 'candidate-lastName'
        --, case when c.SiteUnique <> '0' then c.SiteUnique else 'defaultcompany' end as 'contact-companyId' , cl.AccountName as 'company-name#'
        , am1.email as 'candidate-owners' --c.AccountManager
        
        --, coalesce(nullif(e1.email,''), concat(ed.email,'_',ed.rn) ) as 'contact-email' --, c.Email as 'contact-email#'
        --, coalesce( nullif(e1.email,''), coalesce( concat( nullif(ed.email,''),'_',ed.rn),'') ) as 'contact-email' --, c.Email as 'contact-email#'
        , coalesce( nullif(e1.email,''), coalesce( nullif(ed.email,'') + '(duplicated_' + cast(ed.rn as varchar(10)) + ')','') ) as 'candidate-email' --, c.Email as 'contact-email#'
        --, replace(c.HomeEmail,char(9),'') as 'Personal Email' -- INJECTION
        
        , c.Telephone as 'candidate-phone'
        , ltrim(Stuff( 
                  Coalesce(' ' + NULLIF(cast(c.Mobile as varchar(max)), '') + char(10), '')
                + Coalesce(', ' + NULLIF(cast(c.PersonalMobile as varchar(max)), '') + char(10), '')
                , 1, 1, '') ) as 'candidate-mobile' -- INJECTION
        , ltrim(Stuff( 
                  Coalesce(' ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressHseNo)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddress)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressLine1)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressLine2)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressLine3)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressLine4)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressLine5)) as varchar(max)), ''), '')
                + Coalesce(', ' + NULLIF(cast(ltrim(rtrim(c.HomeAddressLine6)) as varchar(max)), ''), '')
                , 1, 1, '')) as 'candidate-address'
        , c.HomePostcode As 'candidate-zipCode'
                                
        , UPPER(replace(c.Title,'Sir','MR')) as 'candidate-title'
        , case when c.Title in ('Miss','Mrs','Ms') then 'FEMALE' when c.Title in ('Mr','Sir') then 'MALE' else '' end As 'candidate-gender'
                
        , c.RoleDescription as 'candidate-jobTitle'
        , Stuff( 
                  Coalesce('Email 2: ' + NULLIF(cast(e2.email as varchar(max)), '') + char(10), '')
                + Coalesce('Creation Date: ' + NULLIF(cast(c.CreationDate as varchar(max)), '') + char(10), '')
                + Coalesce('Amendment Date: ' + NULLIF(cast(c.AmendmentDate as varchar(max)), '') + char(10), '')
                + Coalesce('Position Code: ' + NULLIF(cast(c.PositionCode as varchar(max)), '') + char(10), '')
                + Coalesce('Company: ' + NULLIF(cast(cl.AccountName as varchar(max)), '') + char(10), '') --c.SiteUnique
                + Coalesce('Extension: ' + NULLIF(cast(c.Extension as varchar(max)), '') + char(10), '')
                + Coalesce('DDI: ' + NULLIF(cast(c.DDI as varchar(max)), '') + char(10), '')
                + Coalesce('Web Address: ' + NULLIF(cast(c.WebAddress as varchar(max)), '') + char(10), '')
                + Coalesce('Next Call Date: ' + NULLIF(cast(c.NextCallDate as varchar(max)), '') + char(10), '')
                + Coalesce('Next Call Time: ' + NULLIF(cast(c.NextCallTime as varchar(max)), '') + char(10), '')
                + Coalesce('Mailshot YN: ' + NULLIF(cast(c.MailshotYN as varchar(max)), '') + char(10), '')
                + Coalesce('Eshot YN: ' + NULLIF(cast(c.EshotYN as varchar(max)), '') + char(10), '')
                + Coalesce('Creating User: ' + NULLIF(cast(am2.fullname as varchar(max)), '') + char(10), '') --c.CreatingUser
                , 1, 0, '') as 'candidate-note'
        --, JOURNALS as 'Activities Comments'
-- select count(*) --12336 -- select top 10 * -- select distinct c.Title
from contacts c --where c.SiteUnique = '0' --9583
left join e1 ON c.ContactUniqueID = e1.ID -- candidate-email
left join e2 ON c.ContactUniqueID = e2.ID
left join ed ON c.ContactUniqueID = ed.ID -- candidate-email-DUPLICATION
left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join contacts c on am.[user] = c.AccountManager) am1 on am1.[user] = c.AccountManager
left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join contacts c on am.[user] = c.CreatingUser) am2 on am2.[user] = c.CreatingUser
left join Clients cl on cl.UniqueID = c.SiteUnique
where c.SiteUnique = '0'
--where c.SiteUnique = '2106'
--where c.ContactUniqueID in ('2456','995','2154')
--where c.AccountManager <> c.CreatingUser
--where SiteUnique = '0'
--and c.Forename like 'Sunisa%' -- Bunsalee

