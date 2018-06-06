
with 
attachment as ( SELECT ParentID, STUFF((SELECT ',' + replace(filename,',','') from Attachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM Attachments as c GROUP BY c.ParentID )
--select * from attachment a left join contacts c on c.contactid = a.ParentID where c.contactid is not null

, dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.ClientName)) ORDER BY CC.ClientId ASC) AS rn FROM Clients CC )
--select * from dup


select
  c.ContactId As 'contact-externalId'
--, c.FirstName As 'contact-firstName'
--, c.LastName As 'contact-lastName'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',Cl.clientID) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
, cl.ClientId As 'contact-companyId' --c.ClientName As 'contact-companyId'
, c.Email As 'contact-email'
, c.JobTitle As 'contact-jobTitle'
, c.WorkPhone As 'contact-phone' --'contact-WorkPhone'
, c.Mobile As 'contact-Mobile'
        /*, ltrim(Stuff(    Coalesce(', ' + NULLIF(c.WorkPhone, '') + char(10), '')
                        + Coalesce(',' + NULLIF(c.Mobile, '') + char(10), '')
                , 1, 1, '') ) as 'contact-phone' */
, u.email As 'contact-owners' --, 'philippa@silverswanrecruitment.com' As 'contact-owners'

/*
, FullName As 'contact-note'
, Department As 'contact-note'
, Salutation As 'contact-note'
, LastActivityTime As 'contact-note'
, LastMailedTime As 'contact-note'
, IsPrimaryContact As 'contact-note'
, EmailOptOut As 'contact-note'
, IsAttachmentPresent  As 'contact-note'
, TypeofClient As 'contact-note' */
        , ltrim(Stuff(    Coalesce('Full Name: ' + NULLIF(c.FullName, '') + char(10), '')
                        + Coalesce('Department: ' + NULLIF(c.Department, '') + char(10), '')
                        + Coalesce('Salutation: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Last Mailed Time: ' + NULLIF(C.LastMailedTime, '') + char(10), '')
                        + Coalesce('Is Primary Contact: ' + NULLIF(C.IsPrimaryContact, '') + char(10), '')
                        + Coalesce('Email Opt Out: ' + NULLIF(C.EmailOptOut, '') + char(10), '')
                        + Coalesce('Is Attachment Present: ' + NULLIF(C.IsAttachmentPresent, '') + char(10), '')
                        + Coalesce('Type of Client: ' + NULLIF(C.TypeofClient, '') + char(10), '')
                , 1, 0, '') ) as 'contact-note'
-- select count(*)
from Contacts c
left join (select * from dup where rn = 1) cl on cl.ClientId = c.ClientId
left join users u on u.userid = c.ContactOwnerId



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

/*
----
----------

with comment as (
        select
                   j.ParentID
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Note Owner: ' + NULLIF(u1.email, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u2.email, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select top 100 * 
        from Notes J
        left join (select * from users) u1 on u1.userid = j.NoteOwnerId
        left join (select * from users) u2 on u2.userid = j.CreatedBy
        --left join Contacts c on c.ContactID = j.ParentID where c.ContactID is not null
UNION ALL
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
        ---left join Contacts c on c.ContactID = j.EntityId where c.ContactID is not null
)
--select count(*) from comment where comment.comment is not null --8157

select c.ContactId, [comment_timestamp|insert_timestamp], comment.comment 
from Contacts c
left join comment on comment.ParentID = c.ContactId 
where c.ContactID is not null and comment.comment is not null
*/


/*
select
  c.ContactId As 'contact-externalId'
, c.Mobile As 'contact-Mobile'
from Contacts c WHERE C.Mobile IS NOT NULL AND C.Mobile <> ''
*/