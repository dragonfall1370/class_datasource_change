
with comments as (
        SELECT --top 200
                  c.candidate --,UC.Userid
                --, CONVERT(VARCHAR(10),c.AddedDate,120) as date
                , c.AddedDate as 'date'
                , ltrim(Stuff( 
                                  Coalesce('Job Title: ' + NULLIF(cast(j.position_title as nvarchar(max)) , '') + char(10), '')
                                + Coalesce('Status: ' + NULLIF(cast(c.Status as nvarchar(max)) , '') + char(10), '')
                                + Coalesce('Rating: ' + NULLIF(cast(c.Rating as nvarchar(max)), '') + char(10), '')
                                + Coalesce('History: ' + NULLIF(cast(c.History as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Notes: ' + NULLIF(cast(c.Notes as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Added By: ' + NULLIF(cast(c.AddedBy as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Added Date: ' + NULLIF(cast(c.AddedDate as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Start Date: ' + NULLIF(cast(c.StartDate as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Contract Duration: ' + NULLIF(cast(c.ContractDuration as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Salary: ' + NULLIF(cast(c.Salary as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Revenue: ' + NULLIF(cast(c.Revenue as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select count(*) --10200 --select top 1000 UC.comments -- select top 10 c.Addeddate
        from VacanciesCandidates c
        left join (select position_externalId,position_title from JobImportAutomappingTemplate) j on cast(j.position_externalId as varchar(max)) = cast(c.Vacancy as varchar(max))
        where (cast(c.candidate as varchar(max)) <> '' and cast(c.candidate as varchar(max)) not LIKE '%,%')
              and (cast(c.AddedDate as varchar(max)) LIKE '%/%' or cast(c.AddedDate as varchar(max)) LIKE '')
UNION ALL
        SELECT --top 100
                  c.candidate
                --, CONVERT(VARCHAR(10),c.Date,120) as date
                , c.date as 'date'
                , ltrim(Stuff(   'INTERVIEW HISTORY:'  + char(10)
                                + Coalesce('Job Title: ' + NULLIF(cast(j.position_title as nvarchar(max)) , '') + char(10), '')
                                + Coalesce('Date: ' + NULLIF(cast(c.Date as nvarchar(max)) , '') + char(10), '')
                                + Coalesce('Location: ' + NULLIF(cast(c.Location as nvarchar(max)), '') + char(10), '')
                                + Coalesce('Notes: ' + NULLIF(cast(c.Notes as nvarchar(max)), '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select count(*) --select top 1000 comments
        from VacanciesInterviews c
        left join (select position_externalId,position_title from JobImportAutomappingTemplate) j on cast(j.position_externalId as varchar(max)) = cast(c.Vacancy as varchar(max))
        where (cast(c.candidate as varchar(max)) <> '' and cast(c.candidate as varchar(max)) not LIKE '%,%')
              and (cast(c.date as varchar(max)) LIKE '%/%' or cast(c.date as varchar(max)) LIKE '')
UNION ALL
	select --top 100
	          j.candidates as candidate
	        --, convert(varchar(10),j.Date,120) as date
	        , j.date as 'date'
                , Stuff(        'JOURNAL HISTORY:' + char(10)
                                + Coalesce('Date: ' + NULLIF(convert(varchar(10),j.Date,120), '') + char(10), '')
                                + Coalesce('Subject: ' + NULLIF(cast(j.Subject as varchar(max)), '') + char(10), '')
                                + Coalesce('Body: ' + NULLIF(cast(j.Body as varchar(max)), '') + char(10), '')
                                + Coalesce('Type: ' + NULLIF(cast(j.Type as varchar(max)), '') + char(10), '')
                                + Coalesce('Consultant: ' + NULLIF(cast(Consultant as varchar(max)), '') + char(10), '')
                                + Coalesce('Company Name: ' + NULLIF(cast(c.company_name as varchar(max)), '') + char(10), '')
                                + Coalesce('Contact Name: ' + NULLIF(cast(con.fullname as varchar(max)), '') + char(10), '')
                                + Coalesce('Job Title: ' + NULLIF(cast(con.contact_jobTitle as varchar(max)), '') + char(10), '')
                        , 1, 0, '') as 'comment'
                -- select candidates, case when convert(varchar(50),j.date) like '%\/%\/%\/%' then '' else j.date end as 'date'
                from Journals j
                left join CompanyImportAutomappingTemplate c on cast(c.company_externalid as varchar(max))= cast(j.Clients as varchar(max))
                left join (select contact_externalId, concat(contact_firstName,' ',contact_lastName) as fullname,contact_jobTitle from ContactsImportAutomappingTemplate) con on cast(con.contact_externalId as varchar(max)) = cast(j.Contacts as varchar(max))
                where (cast(j.candidates as varchar(max)) <> '' and cast(j.candidates as varchar(max)) not LIKE '%,%')
                      and (cast(j.date as varchar(max)) LIKE '%/%' or cast(j.date as varchar(max)) LIKE '')
)

--select count(*) from comments --74358
--select CONVERT(datetime, CONVERT(VARCHAR(19),replace(convert(varchar(50),date),'',''),120) , 103) as 'feedback_timestamp_insert_timestamp' from comments
select
                  candidate
                , CONVERT(datetime, CONVERT(VARCHAR(19),replace(convert(varchar(50),date),'',''),120) , 103) as 'feedback_timestamp_insert_timestamp'
                --, date as 'feedback_timestamp_insert_timestamp'
                , cast('-10' as int) as 'user_account_id'
                , cast('4' as int) as 'contact_method'
                , cast('1' as int) as 'related_status'
                , comment as comment_body
from comments
--where convert(varchar,candidate) = '689'

-- select CURRENT_TIMESTAMP