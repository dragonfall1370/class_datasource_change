-----Candidate Source
select a.cid, b.sourcename 
from people a
left join Sources b on a.Sources_ID = b.sources_id
where sourcename is not null
-----
with test as (select distinct sourcename, 1 as source_type from Sources)
select *
,getdate() as insert_timestamp,
ROW_NUMBER() over ( partition by source_type order by source_type) as rn from test 



------Cand & contact Division
select CandidDivisions_ID,b.divname as 'CustomValue',cid as 'External_ID'
,'add_cand_info' as 'Additional_type'
,'Cand Divisions' as 'lookup_name'
,getdate() as insert_timestamp
from people a 
left join divisions b on a.CandidDivisions_ID = b.divisions_id

----- company division
select ContactsDivisions_ID,b.divname as 'CustomValue',id as 'External_ID'
,'add_com_info' as 'Additional_type'
,'Company Divisions' as 'lookup_name'
,getdate() as insert_timestamp
from company a 
left join divisions b on a.ContactsDivisions_ID = b.divisions_id

----- Job division
select JobsDivisions_ID,b.divname as 'CustomValue',reference as 'External_ID'
,'add_job_info' as 'Additional_type'
,'Job Divisions' as 'lookup_name'
,getdate() as insert_timestamp
from jobs a 
left join divisions b on a.JobsDivisions_ID = b.divisions_id


