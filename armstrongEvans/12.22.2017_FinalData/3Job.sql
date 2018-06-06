/*
with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5 where id in (39188,14248,30223)

-- NOTE
, note as (
        select jobPostingID
	, Stuff(  Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Status: ' + NULLIF(JP.Status, '') + char(10), '')
	        + Coalesce('Position type: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
	        + Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as note
        -- select top 30 *
        from bullhorn1.BH_JobPosting JP
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        )
--select count(*) from note --918 > 1103
*/


--JOB DUPLICATION REGCONITION
with job0 (position_externalId,company_externalId,position_title,position_startDate) as (
	SELECT    a.position_externalId as position_externalId
		, case when b.company_externalId is null then 99999999 else b.company_externalId end as company_externalId
		, a.position_title as position_title
		, CONVERT(VARCHAR(10),a.position_startDate,120) as position_startDate
		--, ROW_NUMBER() OVER(PARTITION BY convert(varchar(max),b.company_externalId), convert(varchar(max),a.position_title), CONVERT(VARCHAR(10),a.position_startDate,120) ORDER BY a.position_externalId) AS rn 
	-- select count(*) --2073 -- select top 20 *
	from VacancieDetails a
	left join CompanyImportAutomappingTemplate b on convert(varchar(max),b.company_externalId) = convert(varchar(max),a.position_contactId)
	--where convert(varchar(max),a.position_title) = ''
	where cast(a.position_externalId as varchar(max)) <> ''
	)
, job (position_externalId,company_externalId,position_title,position_startDate,rn) as (
	SELECT    a.position_externalId
		, a.company_externalId
		, a.position_title
		, a.position_startDate
		, ROW_NUMBER() OVER(PARTITION BY convert(varchar(max),a.company_externalId), convert(varchar(max),a.position_title), CONVERT(VARCHAR(10),a.position_startDate,120) ORDER BY a.position_externalId) AS rn 
	-- select count(*) --2073 -- select top 20 *
	from job0 a
	)
--select * from job --where convert(varchar(max),position_title) = ''


------------
-- DOCUMENT
------------
--dbo.candidateshistorywith
, doc (Vacancy,files) as (
        SELECT cast(Vacancy as varchar(max)), Files = STUFF(( SELECT DISTINCT ', ' + cast(Filename as varchar(max)) FROM Documents b WHERE cast(b.Vacancy as varchar(max)) <> '' and cast(Vacancy as varchar(max)) = cast(a.Vacancy as varchar(max)) FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 1, '') FROM Documents a GROUP BY cast(a.Vacancy as varchar(max))
        )
--select * from doc
-- select * from Documents where convert(varchar,Contact) <> ''

select --top 100
          a.position_externalId as 'position-externalId' 
	, iif(cast(a.position_contactId as varchar(max)) = '', '99999999', a.position_contactId ) as 'position-contactId'
	--, uc.firstname as '#ContactFirstName'
	--, uc.lastname as '#ContactLastName'
	--, cc.name as '#CompanyName'
	--, a.clientUserID as '#UserID'
	--, case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	--, iif(cast(a.position_title as varchar(max)) = '', 'No Title', a.position_title ) as 'position-title'
	, case when job.rn > 1 then concat(job.position_title,' ',rn) else job.position_title end as 'position-title'
	, a.position_headcount as 'position-headcount'
	, o.email as 'position-owners' --a.position_owners
	, a.position_type as 'position-type' /* This field only accepts PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT */
        , replace(replace(convert(varchar(max),a.position_employmentType),'Full-time','FULL_TIME'),'Part-time','PART_TIME') as 'position-employmentType' /* This field only accepts FULL_TIME, PART_TIME, CASUAL */
        , a.position_currency as 'position-currency'
        , a.position_actualSalary as 'position-actualSalary'
        , a.position_internalDescription as 'position-internalDescription'
        --, a.position_startDate as 'position-startDate'
        , CONVERT(varchar(10), CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),a.position_startDate),'Date Added',''),120) , 103), 120) as 'position-startDate'
	--, cast(a.publicDescription as varchar(max)) as 'position-publicDescription'
	--, cast(a.description as varchar(max)) as 'position-internalDescription'
	--, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, doc.files as 'position-document'
	--, concat(note.note,placementnote.note) as 'position-note' --left(,32000)
-- select distinct employmentType --select distinct Type -- select distinct status -- select distinct customtext1 --select count(*) --1380 -- select top 10 * -- select distinct convert(varchar,position_currency)
from VacancieDetails a
left join owner o on cast(o.fullname as varchar(max)) =  cast(a.position_owners as varchar(max))
left join doc on cast(a.position_externalId as varchar(max)) = cast(doc.Vacancy as varchar(max))
left join job on a.position_externalId = job.position_externalId
where cast(a.position_externalId as varchar(max)) not in ('','Record ID')
--where cast(a.position_title as varchar(max)) = ''
--where doc.files is not null
--left join doc on a.jobPostingID = doc.jobPostingID
--where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--and job.title <> ''


/*
------------
-- COMMENT
with comment (Vacancies,date,comment) as (
	select
	  j.Vacancies
	, j.Date as 'date'
	, Stuff(        'JOURNAL HISTORY:' + char(10) +
	                + Coalesce('Date: ' + NULLIF(convert(varchar(10),j.Date,120), '') + char(10), '')
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
        where (cast(j.Vacancies as varchar(max)) <> '' and cast(j.Vacancies as varchar(max)) not LIKE '%,%')
              and (cast(j.date as varchar(max)) LIKE '%/%' or cast(j.date as varchar(max)) LIKE '')        
)
select count(*) from comment --5836
select top 200
        Vacancies as 'externalId'
        , cast('-10' as int) as 'user_account_id'
        , CONVERT(datetime, CONVERT(VARCHAR(19),replace(convert(varchar(50),date),'',''),120) , 103) as 'comment_timestamp|insert_timestamp'
        , comment as 'comment_body'
from comment

*/