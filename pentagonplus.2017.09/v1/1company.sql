
with dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY CC.ClientName ORDER BY CC.ClientId ASC) AS rn FROM client CC ) --where name like 'Azurance'
--select * from dup

      
select --top 1000
        c.ClientId as 'company-externalId',
        --c.AccountManagerId as 'company-owners#',
        u.email as 'company-owners',
        --c.ClientName	as 'company-name',
        iif(c.ClientName in (select ClientName from dup where dup.rn > 1),concat(dup.ClientName,' ',dup.rn), iif(c.ClientName = '' or c.ClientName is null,'No CompanyName',c.ClientName)) as 'company-name',
        c.ContactNumber	as 'company-phone',
        c.Fax	as 'company-fax',
        c.Website	as 'company-website',
        c.Contactaddress as 'company-locationName',
	Stuff( 
	                  Coalesce('About: ' + NULLIF(cast(c.About as varchar(max)), '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(cast(c.LastActivityTime as varchar(max)), '') + char(10), '')
                        + Coalesce('Last Mailed Time: ' + NULLIF(cast(c.LastMailedTime as varchar(max)), '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(c.Source, '') + char(10), '')
                        + Coalesce('Associated Tags: ' + NULLIF(c.AssociatedTags, '') + char(10), '')
                        + Coalesce('Industry: ' + NULLIF(c.Industry, '') + char(10), '')
                        + Coalesce('Remarks: ' + NULLIF(c.Remarks, '') + char(10), '')
                , 1, 0, '') as 'company-note',
        doc.docs as 'company-document'
from client c
left join dup on dup.ClientId = c.ClientId
left join users u on u.userid = c.AccountManagerId
left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join client c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = c.clientID

/*
select clientID,ClientName from client c
left join (select AccountManagerId from client) owner on owner.AccountManagerId = c.clientID
where owner.AccountManagerId is not null

select u.*
from client c
left join users u on u.userid = c.AccountManagerId

select u.*
from client c
left join users u on u.userid = c.AccountManagerId

select a.parentid,a.FileName
from client c
left join attachments a on a.parentid = c.ClientId where a.filename like '%,%' or a.filename like '%''%'

with files(parentid, docs) as (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join client c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid)
select top 100 * from files where parentid in ('Zrecruit_139304000000784126','Zrecruit_139304000001597039','Zrecruit_139304000000036001','Zrecruit_139304000000458053','Zrecruit_139304000000784126','Zrecruit_139304000001059092','Zrecruit_139304000001597039')



-- NOTE COMMENT - INJECT TO VINCERE
select top 10
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
--select count(*) --565
from note t
left join client c on c.ClientId = t.parentid
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.ClientId is not null
and t.ParentId = 'Zrecruit_139304000000055051'


------------
-- CALL COMMENT - INJECT TO VINCERE
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
--select count(*) --41
from call t
left join client c on c.ClientId = t.RelatedTo
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.ClientId is not null
--and t.ParentId = 'Zrecruit_139304000000055051'

*/