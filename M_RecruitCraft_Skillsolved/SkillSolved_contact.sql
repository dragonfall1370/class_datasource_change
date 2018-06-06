--CONTACT EMAIL
with ContactEmails as (select c.contact_id, rtrim(ltrim(contact_email)) as ContactEmails
	from tblContacts c
	where contact_show = 1 and contact_email like '%_@_%.__%'

	UNION ALL

	select c.contact_id, rtrim(ltrim(contact_email2))
	from tblContacts c
	where contact_show = 1 and contact_email2 like '%_@_%.__%') --> union all contact emails into 1 column

, ContactConcatEmails as (SELECT
     contact_id,
     STUFF(
         (SELECT ',' + ContactEmails 
			from ContactEmails
			WHERE contact_id = a.contact_id
			FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
			, 1, 1, '')  as ContactEmails
FROM ContactEmails as a
GROUP BY a.contact_id) --> concate all contact emails group by contact_id

, ContactConcatEmails2 as (select contact_id
	, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace
	(replace(replace(replace(replace(replace(replace(ContactEmails,'/',' '),'<',' '),'>',' ')
	,'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' ')
	,'''',' '),';',' '),'•',' '),char(9),' ') as ContactEmails
	from ContactConcatEmails) --> replace invalid characters in contact emails

, ContactSplitEmails as (SELECT contact_id, Split.a.value('.', 'VARCHAR(100)') AS ContactSplitEmails 
	from (SELECT contact_id, CAST ('<M>' + REPLACE(REPLACE(ContactEmails,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data 
	FROM ContactConcatEmails2) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)) --> split contact emails into rows based on empty space

, ContactSplitEmails2 as (SELECT contact_id
	, case when RIGHT(ContactSplitEmails, 1) = '.' then LEFT(ContactSplitEmails, LEN(ContactSplitEmails) - 1) 
	when LEFT(ContactSplitEmails, 1) = '.' then RIGHT(ContactSplitEmails, LEN(ContactSplitEmails) - 1) 
	else ContactSplitEmails end as ContactSplitEmails 
	from ContactSplitEmails 
	WHERE ContactSplitEmails like '%_@_%.__%') --> find valid emails and remove '.' in contact emails

, ContactSplitEmails2Dup as (SELECT contact_id, ltrim(rtrim(CONVERT(NVARCHAR(MAX), ContactSplitEmails))) as ContactEmails
	, ROW_NUMBER() OVER (PARTITION BY ContactSplitEmails ORDER BY contact_id desc) as rn
	FROM ContactSplitEmails2) --> verify if 1 contact may have multiple emails

, ContactFinalEmails as (SELECT
     contact_id,
     STUFF(
         (SELECT ',' + iif(rn > 1,concat(rn,'_',ContactEmails),ContactEmails)
			from ContactSplitEmails2Dup
			WHERE contact_id = a.contact_id
			FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
			, 1, 1, '')  AS ContactEmails
FROM ContactSplitEmails2Dup as a
GROUP BY a.contact_id) --> concate valid contact emails group by contact_id

--CONTACT COMMENTS
, ContactComments as (SELECT
     contact_id,
     STUFF(
         (SELECT '<hr>' + 'Comment date: ' + convert(varchar(20),comment_date,120) + char(10)
		 + 'Consultant: ' + ltrim(rtrim(consultant)) + char(10) 
		 + 'Contact name: ' + ltrim(rtrim(contact_name)) + char(10) + 'Comment: ' + comment
          from tblContactsComments
          WHERE contact_id = a.contact_id
		  order by comment_date desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 4, '')  AS ContactComments
FROM tblContactsComments as a
GROUP BY a.contact_id)

--CONTACT EMAIL FILES
, ContactEmailFiles as (select ec.ContactID, ec.EmailID, concat(ec.EmailID,rtrim(EmailFileExt)) as EmailFile 
	from tblEmailContacts ec
	left join tblEmails e on e.EmailID = ec.EmailID)

, ContactEmailFiles2 as (SELECT
     ContactID,
     STUFF(
         (SELECT ', ' + EmailFile
          from  ContactEmailFiles
          WHERE ContactID = a.ContactID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS ContactEmailFiles
FROM ContactEmailFiles as a
GROUP BY a.ContactID)

--MAIN SCRIPT
select concat('RC',c.contact_company_id) as 'contact-companyId'
, concat('RC',c.contact_id) as 'contact-externalId'
, case 
	when charindex(' ',ltrim(rtrim(c.contact_name))) <> 0 then left(contact_name,charindex(' ',c.contact_name)-1)
	else c.contact_name end as 'contact-firstName'
, case
	when charindex(' ',ltrim(rtrim(c.contact_name))) <> 0 then right(contact_name,len(contact_name)-charindex(' ',c.contact_name))
	else 'Lastname' end as 'contact-lastName'
, ltrim(rtrim(u.usr_email)) as 'contact-owners'
, ltrim(rtrim(c.contact_job_title)) as 'contact-jobTitle'
, ltrim(stuff((coalesce(' Mob:' + nullif(nullif(ltrim(rtrim(c.contact_mob)),''),'-'),'') 
	+ coalesce(', ' + 'Tel:' + nullif(nullif(ltrim(rtrim(c.contact_tel)),''),'-'),'')),1,1,'')) as 'contact-phone'
, nullif(cfe.ContactEmails,'') as 'contact-email'
, concat('RecruitCraft External ID: ',c.contact_id,char(10)
	, coalesce('Contact Full Name: ' + nullif(ltrim(rtrim(c.contact_name)),'') + char(10),'')
	, coalesce('Contact Fax: ' + nullif(ltrim(rtrim(c.contact_fax)),''),'')) as 'contact-note'
, cc.ContactComments as 'contact-comment'
--, cef.ContactEmailFiles as 'contact-document' -> msg files cannot be inserted via data import
from tblContacts c
left join tblUser u on u.usr_id = c.consultant_id
left join ContactFinalEmails cfe on cfe.contact_id = c.contact_id
left join ContactComments cc on cc.contact_id = c.contact_id
left join ContactEmailFiles2 cef on cef.ContactID = c.contact_id
where c.contact_show = 1
order by c.contact_id