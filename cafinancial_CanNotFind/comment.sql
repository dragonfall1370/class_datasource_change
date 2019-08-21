--, a.RowID as '[Activities]'

-- select  c.ClientID as 'company-externalId', CompanyName as 'company-name'  from companies c where CompanyName like '%shell%'
-- COMPANY
with comments as (
       select
         a.ClientID as 'externalID'
       , a.InputDate as 'insert_timestamp'
       , STUFF(
                 + Coalesce('Candidate: ' + NULLIF(cast(can.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Contact: ' + NULLIF(cast(con.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Contact Type: ' + NULLIF(cast(a.ContactType as varchar(max)), '') + char(10), '')                 
                 + Coalesce('Company: ' + NULLIF(cast(com.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Comment: ' + NULLIF(cast(a.Comment as varchar(max)), '') + char(10), '')
                 + Coalesce('Owner: ' + NULLIF(cast( concat(o.fullname,' - ',o.email) as varchar(max)), '') + char(10), '')
              , 1, 0, '') AS 'content'
       -- select count(*) -- select top 100 *
       from activities a --where Comment =''
       left join (select ClientID, CompanyName as name from companies) com on com.ClientID = a.ClientID
       left join (select ClientContactID, concat(Firstname,  ' ', Surname) as name  from Contacts ) con on con.ClientContactID = a.ClientContactID
       left join (select CVID, concat(Firstname,  ' ', Surname) as name from candidates ) can on can.CVID = a.CVID
       left join owners o on o.id = a.userID
       where com.ClientID <> 0 --31825
       --and com.ClientID in (2142983081)
)

select
        externalID
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'company' as 'type'
       , insert_timestamp as 'insert_timestamp'
       , content as 'content'
from comments where content <> '' and content like '%Financial Manager%'




-- CONTACT
with comments as (
       select
         a.ClientContactID as 'externalID'
       , a.InputDate as 'insert_timestamp'
       , STUFF(
                 + Coalesce('Candidate: ' + NULLIF(cast(can.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Contact: ' + NULLIF(cast(con.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Contact Type: ' + NULLIF(cast(a.ContactType as varchar(max)), '') + char(10), '')                 
                 + Coalesce('Company: ' + NULLIF(cast(com.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Comment: ' + NULLIF(cast(a.Comment as varchar(max)), '') + char(10), '')
                 + Coalesce('Owner: ' + NULLIF(cast( concat(o.fullname,' - ',o.email) as varchar(max)), '') + char(10), '')
              , 1, 0, '') AS 'content'
       -- select count(*) -- select top 100 *
       from activities a --where Comment =''
       left join (select ClientID, CompanyName as name from companies) com on com.ClientID = a.ClientID
       left join (select ClientContactID, concat(Firstname,  ' ', Surname) as name  from Contacts ) con on con.ClientContactID = a.ClientContactID
       left join (select CVID, concat(Firstname,  ' ', Surname) as name from candidates ) can on can.CVID = a.CVID
       left join owners o on o.id = a.userID
       where a.ClientContactID <> 0 --112708
)

select
        externalID
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'contact' as 'type'
       , insert_timestamp as 'insert_timestamp'
       , content as 'content'
from comments where content <> '' and content like '%Financial Manager%'




-- CANDIDATE
with comments as (
       select
         a.CVID as 'externalID'
       , a.InputDate as 'insert_timestamp'
       , STUFF(
                 + Coalesce('Candidate: ' + NULLIF(cast(can.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Contact: ' + NULLIF(cast(con.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Contact Type: ' + NULLIF(cast(a.ContactType as varchar(max)), '') + char(10), '')                 
                 + Coalesce('Company: ' + NULLIF(cast(com.name as varchar(max)), '') + char(10), '')
                 + Coalesce('Comment: ' + NULLIF(cast(a.Comment as varchar(max)), '') + char(10), '')
                 + Coalesce('Owner: ' + NULLIF(cast( concat(o.fullname,' - ',o.email) as varchar(max)), '') + char(10), '')
              , 1, 0, '') AS 'content'
       -- select count(*) -- select top 100 *
       from activities a --where Comment =''
       left join (select ClientID, CompanyName as name from companies) com on com.ClientID = a.ClientID
       left join (select ClientContactID, concat(Firstname,  ' ', Surname) as name  from Contacts ) con on con.ClientContactID = a.ClientContactID
       left join (select CVID, concat(Firstname,  ' ', Surname) as name from candidates ) can on can.CVID = a.CVID
       left join owners o on o.id = a.userID
       where a.CVID <> 0 --136811
)

select
        externalID
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'candidate' as 'type'
       , insert_timestamp as 'insert_timestamp'
       , content as 'content'
from comments where content <> ''
--and candidateID = 2988 or fullname like '%Philip%'


