
/*
with
------------
-- MAIL
------------
  mail1 (ID,email) as (select UC.ContactId, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
        stuff(Coalesce(NULLIF(UC.Email, ''), '') + Coalesce(',' + NULLIF(UC.PersonalEmail, ''), ''), 1, 0, '')
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from contact UC )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
--select * from e2
*/

select
        c.ContactId	as 'contact-externalId',
        c.FirstName	as 'contact-firstName',
        c.LastName	as 'contact-lastName',
        Coalesce(NULLIF(c.ClientId, ''), 'default') as 'contact-companyId',
        --c.Email	as 'contact-email',
        --c.PersonalEmail	as 'contact-email',
        Stuff(Coalesce(NULLIF(c.Email, ''), '') + Coalesce(',' + NULLIF(c.PersonalEmail, ''), ''), 1, 0, '') as 'contact-email',
        c.JobTitle	as 'contact-jobTitle',
        --c.WorkPhone	as 'contact-phone',
        --c.Mobile	as 'contact-phone',
        Stuff(Coalesce(NULLIF(c.Mobile, ''), '') + Coalesce(',' + NULLIF(c.WorkPhone, ''), ''), 1, 0, '') as 'contact-phone',
        --c.ContactOwnerId	as 'contact-owners',
        u.email	as 'contact-owners',
	Stuff(
	         Coalesce('Full Name: ' + NULLIF(cast(c.FullName as varchar(max)), '') + char(10), '')
	       + Coalesce('Salutation: ' + NULLIF(cast(c.Salutation as varchar(max)), '') + char(10), '')
               + Coalesce('Last Activity Time: ' + NULLIF(cast(c.LastActivityTime as varchar(max)), '') + char(10), '')
               + Coalesce('Last Mailed Time: ' + NULLIF(cast(c.LastMailedTime as varchar(max)), '') + char(10), '')
               + Coalesce('Associated Tags: ' + NULLIF(c.AssociatedTags, '') + char(10), '')
               + Coalesce('Source: ' + NULLIF(c.Source, '') + char(10), '')
               + Coalesce('Preferred Name: ' + NULLIF(c.Preferredname, '') + char(10), '')
               + Coalesce('Meeting Notes: ' + NULLIF(c.MeetingNotes, '') + char(10), '')
                , 1, 0, '') as note,
        doc.docs as 'contact-document' 
from contact c
left join users u on u.userid = c.ContactOwnerId
left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join client c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = c.ContactId
where
c.clientID in ('Zrecruit_139304000000926097')
c.ContactId in ('Zrecruit_139304000004302059','Zrecruit_139304000003959479')


/*
select u.*
from contact c
left join users u on u.userid = c.ContactOwnerId

*/

--INSERT INTO contact_comment (contact_id, user_id, comment_content, insert_timestamp) VALUES ( 64167, -10, 'TESTING', '2019-01-01 00:00:00' )
--NOTE COMMENT - INJECT TO VINCERE
select
        t.ParentId as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(DATETIME, t.CreatedTime, 103) as 'comment_timestamp|insert_timestamp'
	, Stuff( 
	                  Coalesce('Title: ' + NULLIF(cast(t.NoteTitle as varchar(max)), '') + char(10), '')
                        + Coalesce('Content: ' + char(10) + NULLIF(cast(t.NoteContent as varchar(max)), '') + char(10) + char(10), '')
                        --+ Coalesce('Created By: ' + NULLIF(cast(t.CreatedBy as varchar(max)), '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Modified By: ' + NULLIF(t.ModifiedBy, '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(t.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(t.ModifiedTime, '') + char(10), '')
                , 1, 0, '') as 'comment_content'
--select count(*) --7177
from note t
left join contact c on c.ContactId = t.parentid
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.ContactId is not null



------------
--CALL COMMENT - INJECT TO VINCERE
select
        t.RelatedTo as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(DATETIME, t.CreatedTime, 103) as 'comment_timestamp|insert_timestamp'
	, Stuff( 
	                  Coalesce('Subject: ' + NULLIF(cast(t.Subject as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Type: ' + NULLIF(cast(t.CallType as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Purpose: ' + NULLIF(cast(t.CallPurpose as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Contact Name: ' + NULLIF(cast(t.ContactName as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Start Time: ' + NULLIF(cast(t.CallStartTime as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Duration: ' + NULLIF(cast(t.CallDuration as varchar(max)), '') + char(10), '')
                        + Coalesce('Description: ' + NULLIF(cast(t.Description as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Result: ' + NULLIF(cast(t.CallResult as varchar(max)), '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(t.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(t.ModifiedTime, '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(cast(t.Status as varchar(max)), '') + char(10), '')
                        + Coalesce('Reminder: ' + NULLIF(cast(t.Reminder as varchar(max)), '') + char(10), '')
                , 1, 0, '') as 'comment_content'
--select count(*) --2
from call t
left join contact c on c.ContactId = t.RelatedTo
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.ContactId is not null
--and t.ParentId = 'Zrecruit_139304000004093014'

