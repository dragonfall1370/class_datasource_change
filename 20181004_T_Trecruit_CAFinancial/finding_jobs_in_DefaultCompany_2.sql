--DOCUMENT
with
 d (id, name) as (SELECT JobSpecID
                 , STUFF((SELECT DISTINCT ',' + Nm from DocFolder WHERE JobSpecID <> 0 and JobSpecID = a.JobSpecID --and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') 
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select JobSpecID from DocFolder where JobSpecID <> 0) AS a GROUP BY a.JobSpecID)
-- select top 100 * from DocFolder
-- select count(*) from d
 --select top 100 * from d


, dup0 as (
select  j.JobSpecID, j.Manager, c2.ClientContactID, c2.ClientID, c2.companyname
from JobSpecs j
left join (select 
                       c.ClientContactID
                     , lower(ltrim(rtrim( replace(replace(replace(replace(replace(replace( concat(c.Surname,c.Firstname) ,',',''),'  ',''),' ',''),'?',''),'.',''),'''','') ))) as name
                     , com.ClientID, com.CompanyName
              -- select count(distinct c.ClientContactID)
              from contacts c
              left join (select ClientID, CompanyName from companies ) com on com.ClientID = c.ClientID
              where c.Surname <> '' or c.Firstname <> ''
              ) c2 on c2.name = lower(ltrim(rtrim( replace(replace(replace(replace(replace(replace( j.Manager,',',''),'  ',''),' ',''),'?',''),'.',''),'''','') )))
)
, dup1 as (select JobSpecID from dup0 group by JobSpecID having count(*) < 2)
, dup as (select * from dup0 where JobSpecID in (select JobSpecID from dup1) )
--select * from dup
--select count(*) from dup1      



, job as (
select --top 200
  j.JobSpecID as 'position-externalId'
--, j.DateReceived as 'position-startDate' --, cast(j.DateReceived as varchar(max)) as 'position-startDate' --, iif(j.DateReceived like '%/%',CONVERT(VARCHAR(10),j.DateReceived,120),'') as 'position-startDate'
, CONVERT(VARCHAR(10),j.DateReceived,120) as 'position-startDate'
--, o.email as 'position-owners' --, j.Consultant as '#Consultant' ,-- case when LEFT(j.Consultant, 1) in ('X','x') then ltrim(RIGHT(j.Consultant, LEN(j.Consultant) - 1))   else j.Consultant end as owners
, ltrim(case
       when j.Consultant like 'X %, X %' then replace(j.Consultant, 'X ','')
       when j.Consultant like 'X%, X%' then replace(j.Consultant, 'X','')
       --when j.Consultant like 'X%, X%' then replace(j.Consultant, 'X' collate Latin1_General_CS_AS,'')
       else j.Consultant end) as owner

, iif(convert(varchar(500),d.ClientContactID) is null, 'default', convert(varchar(500),d.ClientContactID) ) as 'position-contactId' , d.ClientID as 'ClientID' --, j.Manager as '#Manager', c2.companyname  as '#CompanyName'-- c2.name, ltrim(rtrim( replace(replace(replace(j.Manager,',',''),'  ',''),' ','') )) as test
, j.Position as 'position-title'
--, Stuff(    Coalesce('Consultant: ' + NULLIF(iif(o.email is null,cast(j.Consultant as varchar(max)),'') , '') + char(10), '')
, Stuff(    --Coalesce('Consultant: ' + NULLIF(iif(o.email is null,cast(j.Consultant as varchar(max)),'') , '') + char(10), '')
              --+ Coalesce('Contact: ' + NULLIF( iif(c2.name is null, cast(j.Manager as varchar(max)),'' ) , '') + char(10), '') 
              + Coalesce('Note: ' + NULLIF(cast(j.JobStatusDesc as varchar(max)), '') + char(10), '')
                , 1, 0, '')as 'position-note'
, doc.name as 'position-document'
, j.DateReceived
-- select count(*) --6854 -- select distinct Consultant -- select *
from JobSpecs j
--left join owners o on o.fullname = j.Consultant
left join dup d on d.JobSpecID = j.JobSpecID
left join d doc on doc.id = j.JobSpecID
--where d.ClientContactID is not null
)
--select * from job --select count(*) from job

, jobowner as (
        SELECT fullname
                , email
                , ROW_NUMBER() OVER(PARTITION BY fullname ORDER BY fullname) AS rn 
        from owners
       )
       
, job1 as (
       --select distinct[#Consultant] from job where [position-owners] is null
       select  jo.email as 'position-owners', j.*
       from job j
       left join (select * from jobowner where rn = 1) jo on jo.fullname = j.owner
)
--select * from job1



--JOB DUPLICATION REGCONITION
, jobdup as (
        SELECT  [position-externalId]
                , [position-startDate]
                , iif([position-owners] is null, 'xsonjaxdavies@no_email.io', [position-owners]) as [position-owners]
                , [position-contactId]
                , iif(ClientID is null, 'default', cast(ClientID as varchar(max)) ) as ClientID
                , [position-title]
                , [position-note]
                , [position-document]
                , DateReceived
                , ROW_NUMBER() OVER(PARTITION BY iif(ClientID is null, 'default', cast(ClientID as varchar(max)) ),[position-title],[position-startDate] ORDER BY [position-externalId]) AS rn 
                --, ROW_NUMBER() OVER(PARTITION BY [position-contactId],[position-title],CONVERT(VARCHAR(10),startDate,120) ORDER BY [position-externalId]) AS rn 
        from job1
       )


, final as (
select [position-externalId]
                , [position-contactId]
                , ClientID
                , case when rn > 1 then concat([position-title],' ',rn) else [position-title] end as 'position-title'
                , DateReceived
from jobdup where [position-contactId] = 'default'
)
--select * from final



, jobacc as ( select a.InputDate, a.ClientID,a.ClientContactID, a.Comment, replace(replace(a.Comment,'Job Spec created for a ',''),'.','') as title from activities a where a.Comment like '%Job Spec created for a%' )

select X.InputDate
, X.ClientID
, X.ClientContactID
, X.title as job_title_in_comment
, Y.[position-externalId]
, Y.[position-contactId]
, Y.[ClientID]
, Y.[position-title]
, Y.[DateReceived]
from jobacc X
left join final Y on Y.[position-title] = X.title

--and f.[position-externalId] is not null



  
--left join ( select ClientContactID from Contacts) c on a.ClientContactID = c.ClientContactID where a.ClientContactID is null
            
-- select * from JobSpecs j