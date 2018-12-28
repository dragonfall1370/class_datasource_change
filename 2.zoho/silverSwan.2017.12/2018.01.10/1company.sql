
with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + replace(filename,',','') from Attachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM Attachments as c GROUP BY c.ParentID )
--select * from attachment a left join Clients c on c.ClientID = a.ParentID where c.Clientid is not null
--select * from ClientsAttachments


, dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.ClientName)) ORDER BY CC.ClientId ASC) AS rn FROM Clients CC ) --where name like 'Azurance'


select
  c.ClientId as 'company-externalId'
, u.email as 'company-owners' --, 'philippa@silverswanrecruitment.com' as 'company-owners'
, iif(C.ClientId in (select ClientId from dup where dup.rn > 1),concat(dup.ClientName,' ',dup.rn), iif(C.ClientName = '' or C.ClientName is null,'No CompanyName',C.ClientName)) as 'company-name'
--, c.ClientName as 'company-name'
, c.ContactNumber as 'company-phone'
, c.Fax as 'company-fax'
, c.Website as 'company-website'
        , ltrim(Stuff(    Coalesce('About: ' + NULLIF(c.About, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Last Mailed Time: ' + NULLIF(C.LastMailedTime, '') + char(10), '')
                , 1, 0, '') ) as 'company-note'
, c.BusinessType as 'company-business type' -- CUSTOM
, left(c.Contactaddress,400) as 'company-locationName'
, a.filename as 'company-document'
-- select * -- select count(*)
from Clients c
left join attachments a on a.ParentId = c.ClientId
left join dup on C.ClientId = dup.ClientId
left join users u on u.userid = c.AccountManagerId


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


select
          c.ClientID as 'externalId'
        , cast('-10' as int) as userid
        --, CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
        , [comment_timestamp|insert_timestamp]
        , comment.comment  as 'comment_content'
from Clients c
left join comment on comment.ParentID = c.ClientId
where c.ClientID is not null and comment.comment is not null

*/

select
  c.ClientId as 'company-externalId'
, c.ClientName
, c.BusinessType
-- select distinct BusinessType
from Clients c
where BusinessType <> ''


with t0 (ClientId,ClientName,BusinessType) as (
        SELECT    ClientId,ClientName
                , Split.a.value('.', 'VARCHAR(max)') AS String
        FROM ( SELECT     ClientId, ClientName
                        , CAST ('<M>' + REPLACE(BusinessType,';','</M><M>') + '</M>' AS XML) AS Data 
               FROM Clients ) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
        )
--select distinct BusinessType from t
, t1 as (
        select 
                  cast(ClientId as varchar(100)) as 'additional_id' , ClientName, BusinessType
                , 'add_com_info' as additional_type
                , convert(int,1007) as form_id
                , convert(int,1020) as field_id
                , convert(varchar(100),case BusinessType
                        when 'Barge' then '1'
                        when 'Domestic Household' then '2'
                        when 'Operator' then '3'
                        when 'Other' then '4'
                        when 'Ski Operator' then '5'
                        when 'Ski Private Property' then '6'
                        when 'Summer Operator' then '7'
                        when 'Summer Private Property' then '8'
                        when 'Travel Agency' then '9'
                        when 'Yacht' then '10'
                   end) as field_value
        from t0
        )
--select * from t1
--SELECT additional_id, STUFF((SELECT ',' + field_value from t1 WHERE additional_id = c.additional_id and field_value is not NULL and field_value <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS field_value FROM t1 as c GROUP BY c.additional_id
select 
                  distinct t1.additional_id --, t1.ClientName, t1.BusinessType
                , t1.additional_type
                , t1.form_id
                , t1.field_id
                , f.field_value
from t1
left join (SELECT additional_id, STUFF((SELECT ',' + field_value from t1 WHERE additional_id = c.additional_id and field_value is not NULL and field_value <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS field_value FROM t1 as c GROUP BY c.additional_id) f on f.additional_id = t1.additional_id
where t1.additional_id in ('Zrecruit_274609000000415225','Zrecruit_274609000000365310')
