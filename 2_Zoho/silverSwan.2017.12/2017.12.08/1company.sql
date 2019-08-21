
with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + filename from ClientsAttachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM ClientsAttachments as c GROUP BY c.ParentID )
--select * from attachment
--select * from ClientsAttachments


, dup as (SELECT ClientId,ClientName,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.ClientName)) ORDER BY CC.ClientId ASC) AS rn FROM Clients CC ) --where name like 'Azurance'


select
  c.ClientId as 'company-externalId'
, 'philippa@silverswanrecruitment.com' as 'company-owners' --c.AccountManager as 'company-owners'
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
-- select *
from Clients c
left join attachment a on a.ParentId = c.ClientId
left join dup on C.ClientId = dup.ClientId


----------
with comment as (
        select
                   j.ParentID
                 , j.CreatedTime
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
                 , j.CreatedTime
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
                 , j.CreatedTime
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

select
          c.ClientID as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
        , comment.comment  as 'comment_content'
from Clients c
left join comment on comment.ParentID = c.ClientId where comment.comment is not null

*/