--select * from codes c where c.[codegroup] = 94  and c.description not in ('CV F/Up','F/Up PP','Int F/Up') and c.description like '%/Up%'
--select * from act where uniqueid in ('8081FF01ECBA8C81');
--select * from actdocs0 where uniqueid in ('8081FF01ECBA8C81');
--select * from F01 where uniqueid in ('8081FF01ECBA8C81');
--select * from F02 where uniqueid in ('8081FF01ECBA8C81');
--select * from F03 where uniqueid in ('8081FF01ECBA8C81');
--select * from F04_ where uniqueid in ('8081FF01ECBA8C81');
--select * from F05 where uniqueid in ('8081FF01ECBA8C81');
--select * from F08 where uniqueid in ('8081FF01ECBA8C81');
--select * from F12 where uniqueid in ('8081FF01ECBA8C81');
--select * from F13 where uniqueid in ('8081FF01ECBA8C81');
--select * from F14 where uniqueid in ('8081FF01ECBA8C81');
--select * from F17 where uniqueid in ('8081FF01ECBA8C81');
--select * from F18 where uniqueid in ('8081FF01ECBA8C81');
--select * from F22 where uniqueid in ('8081FF01ECBA8C81');
-- select count(*) from act --1537788



with t as (
SELECT --top 1000
	 a.[4 Ref No Numeric] as 'candidate_externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName'
       --, act.[Post Date], act.[Post Time]
       , concat(
              convert( date,
                     CASE 
                         WHEN SUBSTRING([Post Date], 2, 1) = '/' AND SUBSTRING([Post Date], 4, 1) = '/' THEN '0' + SUBSTRING([Post Date], 1, 2) + '0' + SUBSTRING([Post Date], 3, 20) -- x/y/zzzz to xx/yy/zzzz
                         WHEN SUBSTRING([Post Date], 3, 1) = '/' AND SUBSTRING([Post Date], 5, 1) = '/' THEN SUBSTRING([Post Date], 1, 3) + '0' + SUBSTRING([Post Date], 4, 20) -- xx/y/zzzz to xx/yy/zzzz
                         WHEN SUBSTRING([Post Date], 2, 1) = '/' AND SUBSTRING([Post Date], 5, 1) = '/' THEN '0' + [Post Date] -- x/yy/zzzz to xx/yy/zzzz
                         ELSE [Post Date]
                         END
              , 3)
       , ' ',act.[Post Time]) as insert_timestamp
       , Stuff(
              Coalesce('Action: ' + NULLIF(act.description, '') + char(10), '')
              + Coalesce('Contact: ' + NULLIF(/*, act.[Field 3]*/ job.contactname, '') + char(10), '')
              + Coalesce('Company: ' + NULLIF(/*, act.[Field 2]*/company.companyname, '') + char(10), '')
              + Coalesce('Job: ' + NULLIF(/* job.companyname,*/ job.position_title, '') + char(10), '')
              + Coalesce('Consultant: ' + NULLIF(/*, act.[Field 4]*/owner.ownername, '') + char(10), '')
              + Coalesce('Notes: ' + NULLIF(/*, act.[Field 5]*/ concat(act.[Notes 1], act.[Notes 2], act.[Notes 3], act.[Notes 4], act.[Notes 5], act.[Notes 6], act.[Notes 7]), '') + char(10), '')
              , 1, 0, '') as content     
       --, act.[Field 6], act.[Key Date], act.[Create Date], act.[Create Time], act.[Update Date], act.[Update Time], act.*
FROM  F01 a --where a.[101 Candidate Codegroup  23] = 'Y' --22886
left join ( select * from act left join codes c on c.code = act.[Field 5] where c.[codegroup] = 94 and c.description not in ('CV F/Up','F/Up PP','Int F/Up') ) act on act.[Field 1] = a.[UniqueID]
left join (select UniqueID, [1 Name Alphanumeric] as companyname from F02) company on company.uniqueID = act.[Field 2]
left join (select UniqueID, [1 Name Alphanumeric] as ownername from F17) owner on owner.uniqueID = act.[Field 4]
left join (
       select
              a.uniqueid 
              , a.[1 Job Ref Numeric] as 'position-externalId' --, a.UniqueID
              , coalesce( cast(con.[4 Ref No Numeric] as varchar(20)), 'default') as 'position-contactId', concat(con.firstname,' ', con.lastname) as contactname --[20 Contact Xref] --4 RefNumber Numeric --coalesce('BB - ' +  cast(a.[20 Contact Xref] as varchar(20)), 'BB00000') as 'position-contactId'
              , com.[6 Ref No Numeric] as 'position-companyId' , com.companyName --'BB - ' + cast(a.[2 Company Xref] as varchar(20)) as 'position-companyId'
              , concat(cast(a.[1 Job Ref Numeric] as varchar(20)),' ',[3 Position Alphanumeric])  as 'position_title'
       from (select ROW_NUMBER() over(partition by [3 Position Alphanumeric] order by [1 Job Ref Numeric]) as rnk, * from F03) a
       left join (select UniqueID as id, [4 Ref No Numeric], [186 Forenames Alphanumeric] as firstName, [185 Surname Alphanumeric] as lastname from F01 where [100 Contact Codegroup  23] = 'Y') con on con.id = a.[20 Contact Xref] --[4 Ref No Numeric] 
       left join (select UniqueID as id, [6 Ref No Numeric], [1 Name Alphanumeric] as companyName from F02 ) com on com.id = a.[2 Company Xref] --[6 Ref No Numeric]
       left join F17 on F17.[UniqueID] = a.[7 Consultant Xref]
       --where a.uniqueid in ('80810301EBF78080'); -- [1 Job Ref Numeric] in (4118, 15481, 7362)
       ) job on job.uniqueid = act.[Field 3]
where act.[Field 1] is not null
--and company.companyname <> job.companyname
--and act.description in ('CV F/Up','F/Up PP','Int F/Up')
--and act.description in ('F/Up Contr','F/Up CP','F/Up Mark','To Do F/Up')
--and a.[UniqueID] in ('8081FF0183C28681','8081FF01AADB9381','8081FF0198A78981','8081FF01DBF08581','8081FF01A5D99A81','8081FF0183829D81','8081FF01CD9B8981','8081FF01AF879681','8081FF01F8FB9281','8081FF0182DD8F81','8081FF01978CF280','8081FF01988CF280','8081FF01E5A4A581','8081FF01F1D59881','8081FF01F8FD9081','8081FF01F9A79B81','8081FF01EFA89B81','8081FF019FD69B81','8081FF01B8DE9B81','8081FF01ABF49181','8081FF019CABA581','8081FF01B1A5A581','8081FF01CD849181','8081FF01E3BB9981','8081FF019EC89481','8081FF01E8918881','8081FF01C8AF9A81')
and a.[UniqueID] in ('80810101F2C38980')
--and act.[UniqueID]  = '8081FF018BB68981'
)


select
                   candidate_externalId
                  , cast('-10' as int) as 'user_account_id'
                  , 'comment' as 'category'
                  , 'candidate' as 'type'
                  , convert(datetime, insert_timestamp) as insert_timestamp
                  , content
from t where content <> ''
