
-- ALTER DATABASE [cag4] SET COMPATIBILITY_LEVEL = 130


with
  mail1 (ID,email) as (select ClientContactID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ltrim(rtrim(email)),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from contacts )
--, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
, e3 as (select ID, email from mail4 where rn = 3)
--, e4 as (select ID, email from mail4 where rn = 4)
 --select * from ed where rn > 2 --email like '%@%@%'



--DOCUMENT
--with
, d (id, name) as (SELECT ContactID
                 , STUFF((SELECT DISTINCT ',' + Nm from DocFolder WHERE ContactID <> 0 and JobSpecID = 0  and ContactID = a.ContactID --and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') 
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select ContactID from DocFolder where ContactID <> 0 and JobSpecID = 0) AS a GROUP BY a.ContactID)
-- select top 100 * from DocFolder
-- select count(*) from d
-- select top 100 * from d

select
  o.email as 'contact-owners' , c.UserID
, c.ClientContactID as 'contact-externalId'
, case when (ltrim(replace(c.Firstname,'?','')) = '' or  c.Firstname is null) then 'Firstname' else ltrim(replace(c.Firstname,'?','')) end as 'contact-firstName'
, case when (ltrim(replace(c.Surname,'?','')) = '' or  c.Surname is null) then concat('Lastname-',c.ClientContactID) else ltrim(replace(c.Surname,'?','')) end as 'contact-Lastname'
, iif(com.ClientID is null, 'default', cast(c.ClientID as varchar(max)) ) as 'contact-companyId'
, c.Designation as 'contact-jobTitle'
, c.Tel as 'contact-phone'
, C.Mobile  as 'contact-mobile'
, Stuff(Coalesce('Co Tel: ' + NULLIF(cast(C.CoTel as varchar(max)), '') + char(10), ''), 1, 0, '') as note
/*, Stuff( Coalesce('Mobile: ' + NULLIF(cast(C.Mobile as varchar(max)), '') + char(10), '')
       + Coalesce('Co Tel: ' + NULLIF(cast(C.CoTel as varchar(max)), '') + char(10), '')
                , 1, 0, '') as note*/
--, c.Email as 'contact-email'
, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('contact_',cast(C.ClientContactID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'contact-email'
, Newsletter as 'Newsletter'
, d.name as 'contact-document'
-- select count(*) --13280 -- select *
from Contacts c --where ClientContactID = 2145848688
left join d on d.id = c.ClientContactID
left join owners o on o.id = c.UserID
left join ed on C.ClientContactID = ed.ID -- candidate-email-DUPLICATION
left join e2 on C.ClientContactID = e2.ID
left join companies com on com.ClientID = c.ClientID;





/*
select * from contacts where firstname like '%Greyling%' or Surname like '%Greyling%'
select * from contacts where firstname like '%stumpf%' or Surname like '%stumpf%'
Xstumpf, Xmargo
Jooste, Oxana 
X Greyling, X Sharon
*/

select
       c.ClientContactID as 'additional_id'
       , case when (ltrim(replace(c.Firstname,'?','')) = '' or  c.Firstname is null) then 'Firstname' else ltrim(replace(c.Firstname,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(c.Surname,'?','')) = '' or  c.Surname is null) then concat('Lastname-',c.ClientContactID) else ltrim(replace(c.Surname,'?','')) end as 'contact-Lastname'
        , 'add_con_info' as additional_type
        , 1006 as form_id
        , 1016 as field_id
       , convert(varchar,
              case Newsletter 
                     when 'true' then 1
                     when 'false' then 2
              end
              )
         as 'field_value'
         , Newsletter
-- select count(*) --5996 -- select distinct Newsletter
from Contacts c;
