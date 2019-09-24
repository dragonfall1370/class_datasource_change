
with
  mail (ID,email,rn) as ( SELECT ContactUniqueID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY email ORDER BY email desc) FROM contacts where email <> '')
, e1 as (select ID, email from mail where rn = 1)
, ed (ID,email,rn) as (select ID, email, rn from mail where rn > 1)
--select * from ed where ID in ('2456','995','2154')
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

select
          c.ContactUniqueID as 'contact-externalid'
        , coalesce(nullif(c.Forename,''),'No FirstName') as 'contact-firstName'
        , coalesce(nullif(c.Surname,''),'No LastName') as 'contact-lastName'
        , case when c.SiteUnique <> '0' then c.SiteUnique else 'defaultcompany' end as 'contact-companyId' , cl.AccountName as 'company-name#'
        , am1.email as 'contact-owners' --c.AccountManager
        --, coalesce(nullif(e1.email,''), concat(ed.email,'_',ed.rn) ) as 'contact-email' --, c.Email as 'contact-email#'
        --, coalesce( nullif(e1.email,''), coalesce( concat( nullif(ed.email,''),'_',ed.rn),'') ) as 'contact-email' --, c.Email as 'contact-email#'
        , coalesce( nullif(e1.email,''), coalesce( nullif(ed.email,'') + '(duplicated_' + cast(ed.rn as varchar(10)) + ')','') ) as 'contact-email' --, c.Email as 'contact-email#'
        , c.Telephone as 'contact-phone'
        , ltrim(Stuff( 
                  Coalesce(' ' + NULLIF(cast(c.Mobile as varchar(max)), '') + char(10), '')
                + Coalesce(', ' + NULLIF(cast(c.PersonalMobile as varchar(max)), '') + char(10), '')
                , 1, 1, '') ) as 'contact-mobile' -- INJECTION
                        
        , UPPER(replace(c.Title,'SIR','MR')) as 'contact-title'
        
        , replace(c.HomeEmail,char(9),'') as 'Personal Email' -- INJECTION
        
        , c.RoleDescription as 'contact-jobTitle'
        --, JOURNALS as 'Activities Comments'
        , Stuff( 
                  Coalesce('Creation Date: ' + NULLIF(cast(c.CreationDate as varchar(max)), '') + char(10), '')
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
                , 1, 0, '') as 'contact-note'
-- select count(*) --11210
from contacts c
left join e1 ON c.ContactUniqueID = e1.ID -- candidate-email
left join ed ON c.ContactUniqueID = ed.ID -- candidate-email-DUPLICATION
left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join contacts c on am.[user] = c.AccountManager) am1 on am1.[user] = c.AccountManager
left join (select distinct am.[user],am.fullname,am.email from AccountManager am left join contacts c on am.[user] = c.CreatingUser) am2 on am2.[user] = c.CreatingUser
left join Clients cl on cl.UniqueID = c.SiteUnique
--where c.ContactUniqueID in ('2456','995','2154')
--where c.AccountManager <> c.CreatingUser
where SiteUnique = '0'