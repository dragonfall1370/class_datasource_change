

with
-- select * from Attachments
attachment as ( 
       SELECT 
                ParentID
              , STUFF((
                     --SELECT ',' + replace(filename,',','') 
                     SELECT ',' + replace(filename,',','') as filename
                     from Attachments
                     WHERE ParentID = c.ParentID and filename is not NULL and filename <> ''
                     FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename 
       FROM Attachments as c 
       left join contacts on contacts.contactid = c.ParentID where contacts.contactid is not null --<<
       GROUP BY c.ParentID )
--select * from attachment
--select * from attachment a left join contacts c on c.contactid = a.ParentID where c.contactid is not null

, dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.ClientName)) ORDER BY CC.ClientId ASC) AS rn FROM Clients CC )
--select * from dup


select
       c.ContactId As 'contact-externalId'
--, c.FirstName As 'contact-firstName'
--, c.LastName As 'contact-lastName'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'No FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then 'No LastName' else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
       , case when cl.ClientId is null then 'default' else cl.ClientID end As 'contact-companyId' --, c.ClientId
       , c.Email As 'contact-email'
       , c.JobTitle As 'contact-jobTitle'
       , c.WorkPhone As 'contact-phone'
       --, c.Mobile As 'contact-Mobile'
       --, c.skypeid as 'contact-skype'
       --, c.MailingCountry as 'Country' --<<
       /*, case
		when c.MailingCountry like 'Africa%' then 'ZA'
		when c.MailingCountry like 'Austral%' then 'AU'
		when c.MailingCountry like 'Bahrain%' then 'BH'
		when c.MailingCountry like 'Belgium%' then 'BE'
		when c.MailingCountry like 'China%' then 'CN'
		when c.MailingCountry like 'Egypt%' then 'EG'
		when c.MailingCountry like 'France%' then 'FR'
		when c.MailingCountry like 'Germany%' then 'DE'
		when c.MailingCountry like 'Hong%' then 'HK'
		when c.MailingCountry like 'Hotels%' then ''
		when c.MailingCountry like 'India%' then 'IN'
		when c.MailingCountry like 'Italy%' then 'IT'
		when c.MailingCountry like 'Korea%' then 'KR'
		when c.MailingCountry like 'Kuwait%' then 'KW'
		when c.MailingCountry like 'Malaysi%' then 'MY'
		when c.MailingCountry like 'Norway%' then 'NO'
		when c.MailingCountry like 'Oman%' then 'OM'
		when c.MailingCountry like 'Philipp%' then 'PH'
		when c.MailingCountry like 'Qatar%' then 'QA'
		when c.MailingCountry like 'Saudi%' then 'SA'
		when c.MailingCountry like 'Singapo%' then 'SG'
		when c.MailingCountry like 'Spain%' then 'ES'
		when c.MailingCountry like 'Switzer%' then 'CH'
		when c.MailingCountry like 'UAE%' then ''
		when c.MailingCountry like 'UK%' then 'GB'
		when c.MailingCountry like 'Vietnam%' then 'VN'
		when c.MailingCountry like '%UNITED%ARAB%' then 'AE'
		when c.MailingCountry like '%UAE%' then 'AE'
		when c.MailingCountry like '%U.A.E%' then 'AE'
		when c.MailingCountry like '%UNITED%KINGDOM%' then 'GB'
		when c.MailingCountry like '%UNITED%STATES%' then 'US'
		when c.MailingCountry like '%US%' then 'US'
		end as 'contact-country' */
        /*, ltrim(Stuff(    Coalesce(', ' + NULLIF(c.WorkPhone, '') + char(10), '')
                        + Coalesce(',' + NULLIF(c.Mobile, '') + char(10), '')
                , 1, 1, '') ) as 'contact-phone' */
       , u.email As 'contact-owners' --, c.Consultant
       --, c.ContactTypeofIndustry
        , ltrim(Stuff(    --Coalesce('Full Name: ' + NULLIF(c.FullName, '') + char(10), '')
                            Coalesce('Mobile: ' + NULLIF(c.Mobile, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(c.Source, '') + char(10), '')
                        + Coalesce('Is Primary Contact: ' + NULLIF(C.IsPrimaryContact, '') + char(10), '')
                        + Coalesce('Email Opt Out: ' + NULLIF(c.EmailOptOut, '') + char(10), '')                       
                        /*+ Coalesce('Department: ' + NULLIF(c.Department, '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(concat(u1.FirstName,' ',u1.LastName,' - ',u1.email), '') + char(10), '') --c.CreatedBy
                        + Coalesce('Contact Position: ' + NULLIF(C.ContactPosition, '') + char(10), '')
                        + Coalesce('Client Industry: ' + NULLIF(C.ClientIndustry, '') + char(10), '')*/
                , 1, 0, '') ) as 'contact-note'
       , a.filename as 'contact-document'
-- select count(*) -- select distinct MailingCountry -- select distinct MailingCountry --ContactTypeofIndustry
from Contacts c
--left join (select * from dup where rn = 1) cl on cl.ClientId = c.ClientId
left join (select * from dup) cl on cl.ClientId = c.ClientId
left join users u on u.userid = c.ContactOwnerId
left join (select userid, FirstName, LastName, email from users) u1 on u1.userid = c.CreatedBy
left join attachment a on a.ParentId = c.ClientId


/*

select ContactID as additional_id
        , firstName, lastName
        , 'add_con_info' as additional_type
        , 1008 as form_id
        , 1025 as field_id
        , TypeofClient 
        , case TypeofClient
                when 'Barge' then 1
                when 'Domestic Household' then 2
                when 'Ski Chalet' then 3
                when 'Summer Villa/Chateau' then 4
                when 'UK Hospitality' then 5
                when 'Yacht' then 6
        end as field_value
from contacts where TypeofClient <> ''
select distinct TypeofClient from contacts where TypeofClient <> ''

*/


----
----------

with comment as (
        select
                   j.ParentID
                 --, CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),103) as 'comment_timestamp|insert_timestamp'
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),110) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Note Owner: ' + NULLIF(u1.email, '') + char(10), '')
                                + Coalesce('Note Type: ' + NULLIF(j.NoteType, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u2.email, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select top 100 * 
        from Notes J
        left join (select * from users) u1 on u1.userid = j.NoteOwnerId
        left join (select * from users) u2 on u2.userid = j.CreatedBy
        --left join Contacts c on c.ContactID = j.ParentID where c.ContactID is not null
/*UNION ALL
        select
                   j.EntityId
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Time: ' + NULLIF(j.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Subject: ' + NULLIF(j.Subject, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Emails J
        ---left join Contacts c on c.ContactID = j.EntityId where c.ContactID is not null */
/*UNION ALL select eventid, getdate(), title from events
UNION ALL select taskid, getdate(), subject from tasks */
)
--select count(*) from comment where comment.comment is not null --8157

select
        c.ContactId as 'externalId'
        , cast('-10' as int) as 'user_account_id'
        , [comment_timestamp|insert_timestamp]
        , comment.comment  as 'comment_body'
from Contacts c
left join comment on comment.ParentID = c.ContactId 
where c.ContactID is not null and comment.comment is not null


/*
select
  c.ContactId As 'contact-externalId'
, c.Mobile As 'contact-Mobile'
from Contacts c WHERE C.Mobile IS NOT NULL AND C.Mobile <> ''
*/