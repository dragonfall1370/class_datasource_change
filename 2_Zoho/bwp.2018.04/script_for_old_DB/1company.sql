

with
-- select * from Attachments where filename like '%Damien%' -- in ('Killa - BWP 2016 Revised signed .pdf')
attachment as ( 
       SELECT 
                ParentID
              , STUFF((
                     SELECT ',' + concat(replace(OldAttachmentId,'Zrecruit_','') , '_' , replace(filename,',','')) as filename
                     from Attachments
                     WHERE ParentID = c.ParentID and filename is not NULL and filename <> ''
                     FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename 
       FROM Attachments as c 
       left join Clients on c.ParentID = Clients.ClientId where Clients.ClientId is not null --<<
       GROUP BY c.ParentID )
--select * from attachment --a left join Clients c on c.ClientID = a.ParentID where a.ParentID = 'Zrecruit_344622000000204105' and  c.Clientid is not null



, dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.ClientName)) ORDER BY CC.ClientId ASC) AS rn FROM Clients CC ) --where name like 'Azurance'
--select * from dup

select
         c.ClientId as 'company-externalId'
       , u.email as 'company-owners' --, c.AccountManagerId, c.Primarycontact
       , iif(dup.rn > 1,concat(dup.ClientName,' ',dup.rn), iif(dup.ClientName = '' or dup.ClientName is null,'No CompanyName',dup.ClientName)) as 'company-name'
       , c.Website as 'company-website'
       , c.Industry --<<
       --, c.CreatedBy --<<
        , ltrim(Stuff(
                            Coalesce(' ' + NULLIF(c.BillingStreet, ''), '')
                        + Coalesce(', ' + NULLIF(c.BillingCode, ''), '')
                        + Coalesce(', ' + NULLIF(C.BillingCity, ''), '')
                        + Coalesce(', ' + NULLIF(C.BillingState, ''), '')
                        + Coalesce(', ' + NULLIF(C.BillingCountry, ''), '')
                , 1, 1, '') ) as 'company-locationName'
       /*, c.BillingStreet
       , c.BillingCity
       , c.BillingState
       , c.BillingCode
       , c.BillingCountry */
       --, c.ContactNumber as 'company-phone', c.OfficeTel
        , ltrim(Stuff(
                            Coalesce(' ' + NULLIF(c.ContactNumber, ''), '')
                        + Coalesce(', ' + NULLIF(c.OfficeTel, ''), '')
                , 1, 1, '') ) as 'company-phone'
       , c.Fax as 'company-fax'
        , ltrim(Stuff(    
                            Coalesce('Created By: ' + NULLIF(concat(u1.FirstName,' ',u1.LastName,' - ',u1.email), '') + char(10), '')
                        + Coalesce('Parent Company: ' + NULLIF(p.ClientName, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Email: ' + NULLIF(C.Email, '') + char(10), '')
                        + Coalesce('Direct Dial: ' + NULLIF(C.DirectDial, '') + char(10), '')
                        + Coalesce('Contact Person: ' + NULLIF(C.ContactPerson, '') + char(10), '')
                , 1, 0, '') ) as 'company-note'
       , a.filename as 'company-document'
-- select * -- select count(*)
from Clients c
left join (select userid, email from users) u on u.userid = c.AccountManagerId
left join (select userid, FirstName, LastName, email from users) u1 on u1.userid = c.CreatedBy
left join (select ClientId, ClientName from Clients) p on p.ClientId = c.ParentClient
left join dup on C.ClientId = dup.ClientId
left join attachment a on a.ParentId = c.ClientId

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


/*
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
*/