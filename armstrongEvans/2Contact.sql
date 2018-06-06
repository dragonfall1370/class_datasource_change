/*
with
------------
-- MAIL
------------
  mail1 (ID,email) as (select cast(c.contact_externalId as varchar(100)), replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace( convert(varchar(100),c.contact_email),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from ContactsImportAutomappingTemplate c where convert(varchar(100),c.contact_email) <> '')
--select * from mail1
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--, ed0 (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID ASC) AS rn FROM mail4) --DUPLICATION
--, ed1 (ID,email,rn) as (select distinct ID,email,rn from ed0 where rn > 1)
--, e1 as (select ID, email from mail4 where rn = 1)
--, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
--, e2 as (select ID, email from mail4 where rn = 2)
--, e3 as (select ID, email from mail4 where rn = 3)
--, e4 as (select ID, email from mail4 where rn = 4)
select * from mail4 where rn > 2 email like '%@%@%'
*/


------------
-- DOCUMENT
------------
with doc (Contact,files) as (
        SELECT cast(Contact as varchar(max)), Files = STUFF(( SELECT DISTINCT ', ' + cast(Filename as varchar(max)) FROM Documents b WHERE cast(b.Contact as varchar(max)) <> '' and cast(Contact as varchar(max)) = cast(a.Contact as varchar(max)) FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 1, '') FROM Documents a GROUP BY cast(a.Contact as varchar(max))
        )
--select * from doc
-- select * from Documents where convert(varchar,Contact) <> ''


-----MAIN SCRIPT------
select  c.contact_companyId as 'contact-companyId'
	, c.contact_externalId as 'contact-externalId'
	, c.contact_firstName as 'contact-firstName'
	, c.contact_lastName as 'contact-Lastname'
	, c.contact_email as 'contact-email'
	, o.email as 'contact-owners' --, c.contact_owners 
        , c.contact_phone as 'contact-phone'
	, c.contact_jobTitle as 'contact-jobTitle'
	, c.contact_linkedin as 'contact-linkedin'
	--, UC2.name as '#Owners Name'
        --, iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), e1.email) as 'contact-email'
	, doc.files as 'contact-document'
	--, note.note as 'contact-note'
        --, replace(replace(replace(replace(replace(ltrim(rtrim([dbo].[udf_StripHTML](c.comments))),'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'contact-comment'
	--, len(replace(c.comments,'&#x0D;','')) as '(length-contact-comment)'
-- select count(*) --4389 -- select top 10 * -- select distinct convert(varchar,c.contact_owners)
from ContactsImportAutomappingTemplate c
left join owner o on cast(o.fullname as varchar(max)) =  cast(c.contact_owners as varchar(max))
left join doc on cast(c.contact_externalId as varchar(max)) = cast(doc.contact as varchar(max))
--where doc.files is not null
--left join mail5 ON Cl.userID = mail5.ID
--and Cl.clientID = 7773
--order by Cl.clientID desc
/*
where
    (c.contact_firstName like '%Adam%' and c.contact_lastName like '%Kerley%')
 or (c.contact_firstName like '%Adam%' and c.contact_lastName like '%Scott%')
 or (c.contact_firstName like '%Andrew%' and c.contact_lastName like '%Barclay%')
 or (c.contact_firstName like '%Ashley%' and c.contact_lastName like '%Shannon%')
 or (c.contact_firstName like '%Blake%' and c.contact_lastName like '%Irving%')
 or (c.contact_firstName like '%Chris%' and c.contact_lastName like '%Perry%')
 or (c.contact_firstName like '%Christopher%' and c.contact_lastName like '%Gray%')
 or (c.contact_firstName like '%Jack%' and c.contact_lastName like '%Lindley%')
 or (c.contact_firstName like '%James%' and c.contact_lastName like '%Epton%')
 or (c.contact_firstName like '%Jamie%' and c.contact_lastName like '%Allison%')
 or (c.contact_firstName like '%Lauren%' and c.contact_lastName like '%Schofield%')
 or (c.contact_firstName like '%Louis%' and c.contact_lastName like '%Colley%')
 or (c.contact_firstName like '%Martin%' and c.contact_lastName like '%Bishop%')
 or (c.contact_firstName like '%Matt%' and c.contact_lastName like '%Banks%')
 or (c.contact_firstName like '%Matthew%' and c.contact_lastName like '%Nesbitt%')
 or (c.contact_firstName like '%Mitch%' and c.contact_lastName like '%Oakley%')
 or (c.contact_firstName like '%Peter%' and c.contact_lastName like '%Ryalls%')
 or (c.contact_firstName like '%Ryan%' and c.contact_lastName like '%Ealand%')
-- JOB OWNER
 or (c.contact_firstName like '%Adam%' and c.contact_lastName like '%Kerley%')
 or (c.contact_firstName like '%Adam%' and c.contact_lastName like '%Scott%')
 or (c.contact_firstName like '%Andrew%' and c.contact_lastName like '%Barclay%')
 or (c.contact_firstName like '%Ashley%' and c.contact_lastName like '%Shannon%')
 or (c.contact_firstName like '%Blake%' and c.contact_lastName like '%Irving%')
 or (c.contact_firstName like '%Chris%' and c.contact_lastName like '%Perry%')
 or (c.contact_firstName like '%Christopher%' and c.contact_lastName like '%Gray%')
 or (c.contact_firstName like '%Jack%' and c.contact_lastName like '%Lindley%')
 or (c.contact_firstName like '%James%' and c.contact_lastName like '%Epton%')
 or (c.contact_firstName like '%Jamie%' and c.contact_lastName like '%Allison%')
 or (c.contact_firstName like '%Lauren%' and c.contact_lastName like '%Schofield%')
 or (c.contact_firstName like '%Managing%' and c.contact_lastName like '%Consultant%')
 or (c.contact_firstName like '%Martin%' and c.contact_lastName like '%Bishop%')
 or (c.contact_firstName like '%Matt%' and c.contact_lastName like '%Banks%')
 or (c.contact_firstName like '%Matthew%' and c.contact_lastName like '%Nesbitt%')
 or (c.contact_firstName like '%Mitch%' and c.contact_lastName like '%Oakley%')
 or (c.contact_firstName like '%Peter%' and c.contact_lastName like '%Ryalls%')
 or (c.contact_firstName like '%Ryan%' and c.contact_lastName like '%Ealand%')
*/

/*
------------
-- COMMENT
------------
with comment (Contacts,date,comment) as (
	select
	  j.Contacts
	, j.Date as 'date'
	, Stuff(          Coalesce('Date: ' + NULLIF(convert(varchar(10),j.Date,120), '') + char(10), '')
                        + Coalesce('Subject: ' + NULLIF(cast(j.Subject as varchar(max)), '') + char(10), '')
                        + Coalesce('Body: ' + NULLIF(cast(j.Body as varchar(max)), '') + char(10), '')
                        + Coalesce('Type: ' + NULLIF(cast(j.Type as varchar(max)), '') + char(10), '')
                        + Coalesce('Consultant: ' + NULLIF(cast(Consultant as varchar(max)), '') + char(10), '')
                        + Coalesce('Company Name: ' + NULLIF(cast(c.company_name as varchar(max)), '') + char(10), '')
                        + Coalesce('Contact Name: ' + NULLIF(cast(con.fullname as varchar(max)), '') + char(10), '')
                        + Coalesce('Job Title: ' + NULLIF(cast(con.contact_jobTitle as varchar(max)), '') + char(10), '')
                , 1, 0, '') as comment
        from Journals j
        left join CompanyImportAutomappingTemplate c on cast(c.company_externalid as varchar(max))= cast(j.Clients as varchar(max))
        left join (select contact_externalId, concat(contact_firstName,' ',contact_lastName) as fullname,contact_jobTitle from ContactsImportAutomappingTemplate) con on cast(con.contact_externalId as varchar(max)) = cast(j.Contacts as varchar(max))
        --where cast(Contacts as varchar(max)) <> ''
        where (cast(j.Contacts as varchar(max)) <> '' and cast(j.Contacts as varchar(max)) not LIKE '%,%')
              and (cast(j.date as varchar(max)) LIKE '%/%' or cast(j.date as varchar(max)) LIKE '')        
)
--select count(*) from comment --44193
select top 200
        Contacts as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(datetime, CONVERT(VARCHAR(19),replace(convert(varchar(50),date),'',''),120) , 103) as 'comment_timestamp|insert_timestamp'
        , comment as 'comment_content'
from comment

*/