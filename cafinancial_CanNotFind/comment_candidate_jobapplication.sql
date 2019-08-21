
with 
ja_comment as (
       select
                  cv.CVID as 'externalID'
                , cv. DateSent as 'insert_timestamp'
                --, cv.ConsultantID
                , stuff( 'CVSent:' + char(10)
                     + Coalesce('Owners: ' + NULLIF(cast(o.fullname as varchar(max)), '') + char(10), '') --, ConsultantID as 'Owners'
                     + Coalesce('Position: ' + NULLIF(cast(j.Position as varchar(max)), '') + char(10), '') --, JobSpecID as 'application-positionExternalId'
              , 1, 0, '') AS 'content'        
       -- select top 20 *
       from CVSent cv
       left join owners o on o.id = cv.ConsultantID
       left join JobSpecs j on j.JobSpecID = cv.JobSpecID
UNION ALL
       select
                i.CVID as 'externalID'
              , i.InputDate as 'insert_timestamp'
              , stuff( 'Interviews:' + char(10)
                     + Coalesce('Contact: ' + NULLIF(cast(con.name as varchar(max)), '') + char(10), '') --, ContactID as 'Contact External Id'
                     + Coalesce('Interview Date: ' + NULLIF(cast(i.InterviewDate as varchar(max)), '') + char(10), '')
                     + Coalesce('Owners: ' + NULLIF(cast(o.fullname as varchar(max)), '') + char(10), '') --, ConsultantID as 'Owners'
                     + Coalesce('Position: ' + NULLIF(cast(j.Position as varchar(max)), '') + char(10), '') --, JobSpecID as 'application-positionExternalId'
                     + Coalesce('Stage: ' + NULLIF(cast(i.Stage as varchar(max)), '') + char(10), '')
              , 1, 0, '') AS 'content'     
       -- select distinct Stage -- select top 20 *
       from Interviews i
       left join (select ClientContactID, concat(Firstname,  ' ', Surname) as name  from Contacts ) con on con.ClientContactID = i.ContactID
       left join owners o on o.id = i.ConsultantID
       left join JobSpecs j on j.JobSpecID = i.JobSpecID
UNION ALL
       select
                p.CVID as 'externalID'
              , p.InputDate as 'insert_timestamp'
              , stuff( 'Placements:' + char(10)
                     + Coalesce('Position: ' + NULLIF(cast(j.Position as varchar(max)), '') + char(10), '') --, JobSpecID
                     + Coalesce('Candidate: ' + NULLIF(cast(p.candidate as varchar(max)), '') + char(10), '')
                     + Coalesce('Start Date: ' + NULLIF(cast(p.StartDate as varchar(max)), '') + char(10), '')
                     + Coalesce('End Date: ' + NULLIF(cast(p.EndDate as varchar(max)), '') + char(10), '')
                     + Coalesce('Fee: ' + NULLIF(cast(p.Fee as varchar(max)), '') + char(10), '')
                     + Coalesce('Total Salary: ' + NULLIF(cast(p.TotalSalary as varchar(max)), '') + char(10), '')
              , 1, 0, '') AS 'content'                 
        -- select top 20 *
       from Placements p
       left join JobSpecs j on j.JobSpecID = p.JobSpecID
)


select
        externalID
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'candidate' as 'type'
       , insert_timestamp as 'insert_timestamp'
       , content as 'content'
from ja_comment where content <> ''


