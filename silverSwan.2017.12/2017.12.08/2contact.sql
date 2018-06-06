
with 
 dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.ClientName)) ORDER BY CC.ClientId ASC) AS rn FROM Clients CC )

select
  c.ContactId As 'contact-externalId'
--, c.FirstName As 'contact-firstName'
--, c.LastName As 'contact-lastName'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',Cl.clientID) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
, cl.ClientId As 'contact-companyId' --c.ClientName As 'contact-companyId'
, c.Email As 'contact-email'
, c.JobTitle As 'contact-jobTitle'
, c.WorkPhone As 'contact-WorkPhone'
, c.Mobile As 'contact-Mobile'
        , ltrim(Stuff(    Coalesce(', ' + NULLIF(c.WorkPhone, '') + char(10), '')
                        + Coalesce(',' + NULLIF(c.Mobile, '') + char(10), '')
                , 1, 1, '') ) as 'company-phone'
, 'philippa@silverswanrecruitment.com' As 'contact-owners' --,c.ContactOwner 

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
left join (select * from dup where rn = 1) cl on cl.ClientName = c.ClientName


/*

----
----------

with comment as (
        select
                   j.ParentID
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from ClientsNotes J
UNION ALL
        select
                   j.ParentID
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from JobNotes J --left join JobOpenings jo on jo.JobOpeningId = j.ParentID
UNION ALL
        select
                   j.ParentID
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from CandidatesNotes J
)
--select top 1000 * from comment

select c.ContactId, comment.comment from Contacts c
left join comment on comment.ParentID = c.ContactId where comment.comment is not null
*/