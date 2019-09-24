-- COMPANY
with comments as (
       select
       a.InputDate
       , a.ClientID as 'company-externalID', com.name as 'company-name'
       , a.ClientContactID 'contact-externalID', con.name as 'contact-name'
       , a.CVID as 'candidate-externalID', can.name as 'candidate-name'
       , a.Comment
       -- select count(*) -- select top 100 *
       from activities a --where Comment =''
       left join (select ClientID, CompanyName as name from companies) com on com.ClientID = a.ClientID
       left join (select ClientContactID, concat(Firstname,  ' ', Surname) as name  from Contacts ) con on con.ClientContactID = a.ClientContactID
       left join (select CVID, concat(Firstname,  ' ', Surname) as name from candidates ) can on can.CVID = a.CVID
       left join owners o on o.id = a.userID
       where com.ClientID <> 0 --31825
       --and com.ClientID in (2142983081)
       )
select * from comments where Comment <> '' and Comment like '%Job Spec created for %'




/*
select 
       a.ClientID,a.ClientContactID, a.Comment, a.InputDate
        , t.JobSpecID, t.Position
from activities a
left join (
       select  j.JobSpecID, j.Position, c2.ClientID, c2.ClientContactID
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
) t on a.ClientContactID= t.ClientContactID
where a.Comment like '%Job Spec created for %'
*/