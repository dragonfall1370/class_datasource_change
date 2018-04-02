-------------
--PART 1: MAIN SCRIPT
-------------
with 
--MAIL DUPLICATION
UnionEmail as (select ID, replace(replace(replace(EMAIL,'%',''),'..@','@'),'.@','@') as EMAIL from Contact where EMAIL is not NULL
UNION ALL
select ID, replace(replace(replace(PEOPLECLOUD1__HOME_EMAIL__C,'%',''),'..@','@'),'.@','@') from Contact where PEOPLECLOUD1__HOME_EMAIL__C is not NULL
UNION ALL
select ID, replace(replace(replace(PEOPLECLOUD1__WORK_EMAIL__C,'%',''),'..@','@'),'.@','@') from Contact where PEOPLECLOUD1__WORK_EMAIL__C is not NULL
)

, UnionEmailDistinct as (select ID, EMAIL from UnionEmail group by ID, Email)

, dup as (SELECT ID, EMAIL, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY ID ASC) AS rn 
FROM UnionEmailDistinct where EMAIL is not NULL)

/* SQL Server 2016 
, ContactEmail as (SELECT
     ID,
     STUFF(
         (SELECT ', ' 
		 + case when rn > 1 then concat(rn,'_duplicate_',EMAIL)
         else EMAIL end as EMAIL
		 from dup
         WHERE ID = a.ID
		 order by ID desc
         FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
         , 1, 2, '')  AS ContactEmail
FROM dup as a
GROUP BY a.ID) */ 

/* Support from SQL Server 2017 */
, ContactEmail as (select ID
	, STRING_AGG(case when rn > 1 then concat(rn,'_duplicate_',replace(replace(replace(EMAIL,'%',''),'..@','@'),'.@','@'))
	else replace(replace(replace(EMAIL,'%',''),'..@','@'),'.@','@') end, ', ') as ContactEmail
from dup
group by ID)

--CONTACT FILES
, ContactFiles as (select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C
	, STRING_AGG(NAME, ', ') as ContactFiles
	from ResumeCompliance
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PEOPLECLOUD1__DOCUMENT_RELATED_TO__C)

--COMPANY ATTACHMENTS /* Support from SQL Server 2017 */
, ConAttachments as (select PARENTID
	, STRING_AGG(NAME, ', ') as ConAttachments
	from Attachments
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PARENTID)
	
/* Support from SQL Server 2017 */

--CONTACT OWNERS
, MergeContactOwners as (select c.ID
	, CONCAT_WS(';',u.NAME, CONTACT_OWNER_SBS__C, CONTACT_OWNERSSM__C, CONTACT_OWNER_STSSTM__C, CONTACT_OWNER_IND__C
		, CONTACT_OWNER_SAAF__C, TECH_CONTACT_OWNER_CONTRACT__C, TECH_CONTACT_OWNER_PERM__C) as ContactOwners
	from Contact c
	left join SiriusUsers u on c.OWNERID = u.ID)

, SplitContactOwners as (select ID as ContactID, ContactOwners, value as SplitContactOwners
	from MergeContactOwners
	cross apply string_split(ContactOwners,';'))

, ContactOwners as (select ContactID, ltrim(rtrim(SplitContactOwners)) as SplitContactOwners
	from SplitContactOwners
	group by ContactID, SplitContactOwners)

, ContactOwnersFinal as (select co.ContactID, STRING_AGG(su.EMAIL, ',') as ContactOwnersFinal
	from ContactOwners co
	left join SiriusUsers su on su.NAME = co.SplitContactOwners
	group by co.ContactID)

--MAIN SCRIPT
select c.ID as 'contact-externalId'
, case when c.ACCOUNTID is NULL or c.ACCOUNTID = '' or c.ACCOUNTID not in (select ID from Company) then 'SP999999999' 
	else c.ACCOUNTID end as 'contact-companyId'
, case when c.LASTNAME = '' or c.LASTNAME is NULL then 'Lastname'
	else c.LASTNAME end as 'contact-lastName'
, case when c.FIRSTNAME = '' or c.FIRSTNAME is NULL then 'Firstname'
	else c.FIRSTNAME end as 'contact-firstName'
, c.TITLE as 'contact-jobTitle'
, ce.ContactEmail as 'contact-email'
, left(c.LINKEDIN_PROFILE__C,200) as 'contact-linkedIn'
--, stuff((coalesce(',' + nullif(ltrim(su.EMAIL),''),'') + coalesce(',' + nullif(ltrim(su2.EMAIL),''),'')
--	+ coalesce(',' + nullif(ltrim(su3.EMAIL),''),'') + coalesce(',' + nullif(ltrim(su4.EMAIL),''),'')
--	+ coalesce(',' + nullif(ltrim(su5.EMAIL),''),'') + coalesce(',' + nullif(ltrim(su6.EMAIL),''),'')
--	+ coalesce(',' + nullif(ltrim(su7.EMAIL),''),'')
--	), 1, 1,'') as 'contact-owners'
, cof.ContactOwnersFinal as 'contact-owners'
, concat(coalesce(ltrim(c.PHONE),''), coalesce(',' + ltrim(c.MOBILEPHONE),'')) as 'contact-phone'
, concat('Contact External ID: ',c.ID,char(10)
	, coalesce('Salutation: ' + c.SALUTATION + char(10),'')
	, 'Do not call: ',c.DONOTCALL,char(10)
	, 'Contact status: ',c.CONTACT_STATUS__C,char(10)
	, coalesce('No of perm staff: ' + c.NO_PERM_STAFF_IN_TEAM__C + char(10),'')
	, coalesce('No of contractors: ' + c.NO_CONTRACTORS_IN_TEAM__C + char(10),'')
	, coalesce('Contact Industry: ' + c.INDUSTRY_SECTORS__C + char(10),'')
	, coalesce('No of temps: ' + c.NO_TEMPS_IN_TEAM__C + char(10),'')
	, coalesce('Do not Contact Reason: ' + c.DO_NOT_CONTACT_REASON__C + char(10),'')
	, coalesce('Desk: ' + c.DESK__C + char(10),'')
	, coalesce('No of how many people in your team: ' + c.HOW_MANY_PEOPLE_IN_YOUR_TEAM__C + char(10),'')
	, coalesce('Current team size: '+ c.CURRENT_TEAM_SIZE__C + char(10),'')
	, coalesce('Accouting finance: ' + c.ACCOUNTING_FINANCE__C + char(10),'')
	, coalesce('Development qualification: ' + c.DEVELOPMENT_QUALIFICATION__C + char(10),'')
	, coalesce('Infrastructure qualification: '  + c.INFRASTRUCTURE_QUALIFICATION__C + char(10),'')
	, coalesce('BI data CRM qualification: ' + c.BI_DATA_CRM_QUALIFICATION__C + char(10),'')
	, coalesce('Project services qualification: ' + c.PROJECT_SERVICES_QUALIFICATION__C + char(10),'')
	, coalesce('Support: '+ c.SUPPORT__C + char(10),'')
	, coalesce('Industrious: ' + c.INDUSTRIOUS__C + char(10),'')
	, coalesce('SSM: '+ c.SSM__C + char(10),'')
	, coalesce('Companies packages: ' + c.COMPANIES_PACKAGES__C + char(10),'')
	, coalesce('Digital qualification: ' + c.DIGITAL_QUALIFICATION__C + char(10),'')
	, coalesce('Reports To ID: ' + c.REPORTSTOID,'')
) as 'contact-note'
, stuff((coalesce(',' + nullif(ltrim(cf.ContactFiles),''),'') + coalesce(',' + nullif(ltrim(ca.ConAttachments),''),'')
	), 1, 1,'') as 'contact-document'
from Contact c
left join ContactOwnersFinal cof on cof.ContactID = c.ID
--left join SiriusUsers su on su.ID = c.OWNERID
--left join SiriusUsers su2 on su2.ID = c.OWNER__C
--left join SiriusUsers su3 on su3.ID = c.CONTACT_OWNER_SBS__C
--left join SiriusUsers su4 on su4.ID = c.CONTACT_OWNERSSM__C
--left join SiriusUsers su5 on su5.ID = c.CONTACT_OWNER_STSSTM__C
--left join SiriusUsers su6 on su6.ID = c.CONTACT_OWNER_IND__C
--left join SiriusUsers su7 on su7.ID = c.CONTACT_OWNER_SAAF__C
--left join SiriusUsers su8 on su8.ID = c.TECH_CONTACT_OWNER_CONTRACT__C
--left join SiriusUsers su9 on su9.ID = c.TECH_CONTACT_OWNER_PERM__C
left join ContactEmail ce on ce.ID = c.ID
left join ContactFiles cf on cf.PEOPLECLOUD1__DOCUMENT_RELATED_TO__C = c.ID
left join ConAttachments ca on ca.PARENTID = c.ID

UNION ALL

select 'SP999999999','SP999999999','Default','Contact','','','','','','This is default contact from data import',''

-------------
--PART 2: INJECT CONTACT ACTIVITIES
-------------
/* Query from DB */

--CONTACT ACTIVITIES (SMS HISTORY)
select sh.SMAGICINTERACT__CONTACT__C as ContactExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('SMS name: ', sh.NAME),char(10)
		 + concat('Created date: ', convert(varchar(20),sh.CREATEDDATE,120),char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Interact Name: ', sh.SMAGICINTERACT__NAME__C,char(10))
		 + concat('*** SMS Text: ', sh.SMAGICINTERACT__SMSTEXT__C)
		 ) as Sirius_comments
	, CONVERT(varchar(20), sh.CREATEDDATE, 120) as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'contact' as Sirius_type
	from SMSHistory sh
	left join SiriusUsers su on su.ID = sh.OWNERID
	where sh.SMAGICINTERACT__CONTACT__C is not NULL
	and sh.SMAGICINTERACT__CONTACT__C in (select ID from Contact)

--CONTACT TASKS
select t.WHOID as ContactExtID
	, -10 as Sirius_user_account_id
	, left(t.SUBJECT,200) as Sirius_subject
	, Concat(concat('Subject: ', t.SUBJECT),char(10)
		 + concat('Status: ', t.STATUS,char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Short description: ', t.SHORT_DESCRIPTION__C,char(10))
		 + case when t.COMPLETED_DATE__C is NULL then 'No completed date'
		 else concat('*** Completed date: ', convert(varchar(20),t.COMPLETED_DATE__C,103)) end
		 ) as Sirius_contact_tasks
	, t.COMPLETED_DATE__C
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 103)
	else getdate() end as Sirius_insert_timestamp
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 103) 
	else getdate() + 3 end as Sirius_next_contact_date
	, case when t.COMPLETED_DATE__C is not NULL then CONVERT(Datetime, t.COMPLETED_DATE__C, 103) 
	else getdate() + 3 end as Sirius_next_contact_to_date
	, 'Australia/Sydney' as Sirius_time_zone
	, 'task' as Sirius_category
	, 'contact' as Sirius_type
	from Tasks t
	left join SiriusUsers su on su.ID = t.OWNERID
	where t.WHOID is not NULL

/* Running process */

-------------
--PART 3: CUSTOM FIELDS
-------------

/* 1. CUSTOM FIELD > DO NOT CALL | Type:  */
select 'add_contact_info' as Sirius_additional_type
, ID as Sirius_ContactExtID
, 1005 as Sirius_form_id
, 1021 as Sirius_field_id
, case when DONOTCALL = 1 then 'YES' else 'NO' end as Sirius_donotcall_value
, getdate() as Sirius_insert_timestamp
from contact

/* 2. CUSTOM FIELD > NO OF PERM STAFF */

/* 3. CUSTOM FIELD > NO OF CONTRACTORS */

/* 4. CUSTOM FIELD > DIVISION */
select 'add_con_info' as Sirius_additional_type
, ID as Sirius_ContactExtID
, '' as Siriusform_id
, '' as Siriusfield_id
, DIVISION__C as Sirius_Division
, getdate() as Sirius_insert_timestamp
from Contact
where DIVISION__C is not NULL

/* 5. CUSTOM FIELD > NO OF TEMPS */

/* 6. CUSTOM FIELD > NO OF HOW MANY PEOPLE IN YOUR TEAM */

/* 7. CUSTOM FIELD > CURRENT TEAM SIZE */

/* 8. Contact Owners --> Already included */

-------------
--PART 4: FUNCTIONAL EXP - SUB EXP
------------
--TABLE INPUT
with FunctionalExp as (select ID
, concat_ws(';',ACCOUNTING_FINANCE__C
, DEVELOPMENT_QUALIFICATION__C
, INFRASTRUCTURE_QUALIFICATION__C
, BI_DATA_CRM_QUALIFICATION__C
, PROJECT_SERVICES_QUALIFICATION__C
, SUPPORT__C
, INDUSTRIOUS__C
, SSM__C
, COMPANIES_PACKAGES__C
, DIGITAL_QUALIFICATION__C) as FExp
from Contact)

select ID as Sirius_ContactExtID
, value as Sirius_FunExp
, getdate() as Sirius_insert_timestamp
	from FunctionalExp
	CROSS APPLY STRING_SPLIT(FExp, ';')
	where FExp <> ''

--MAPPING
--contact_id
--functional_expertise_id
--insert_timestamp
--sub_functional_expertise_id

--PROCESS
-->> Input > Lookup 

-------------
--PART 4: DEFAULT CONTACT MAIN SCRIPT
-------------
select concat(ID,'-DC') as 'contact-externalId'
, ID as 'contact-companyId'
, 'Default' as 'contact-lastName'
, concat('Contact-',ID) as 'contact-firstName'
, 'This is default contact for company' as 'contact-note'
from Company