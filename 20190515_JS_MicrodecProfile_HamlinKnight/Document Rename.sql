
with test as (select concat(parent_object_ref,'.',linkfile_ref,'.',file_extension) as file_id,
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name', parent_object_ref,linkfile_ref,parent_object_name
from linkfile)

,test3 as ( select file_id, iif(file_name = '' or file_name is null,'',concat(linkfile_ref,'_',replace(file_name,'*',''))) as file_name,parent_object_ref,parent_object_name from test)

select cast(parent_object_ref as int) as external_id,
'resume' as 'document_type','CANDIDATE' as 'entity_type'
,file_name from test3 where parent_object_name = 'person'



--------- event document
with test as (select a.linkfile_ref, b.opportunity_ref,a.displayname,a.file_extension 
from linkfile a
left join event b on a.parent_object_ref = b.event_ref
where parent_object_name = 'event' and b.opportunity_ref <> '')

select opportunity_ref as 'external_id',
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name',
'job_description' as 'document_type',
'POSITION' as 'entity_type'
from test



with test as (select a.linkfile_ref, b.opportunity_ref,a.displayname,a.file_extension, a.parent_object_name, a.parent_object_ref
from linkfile a
left join event b on a.parent_object_ref = b.event_ref
where parent_object_name = 'event' and b.opportunity_ref <> '')

,company_document as (select linkfile_ref,parent_object_name,parent_object_ref,file_extension, opportunity_ref,
case when reverse(file_extension) = left(reverse(displayname),3) 
--then displayname end as a
then replace(displayname,concat('.',file_extension),'') else displayname end as displayname
from test)

select opportunity_ref as 'external_id',
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name',
'job_description' as 'document_type',
'POSITION' as 'entity_type'
from company_document



--------------
-------------- rename document

with test as (select concat(parent_object_ref,'.',linkfile_ref,'.',file_extension) as file_id,
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name',
parent_object_ref,linkfile_ref,parent_object_name,file_extension
from linkfile)

,test3 as ( select file_id, iif(file_name = '' or file_name is null,'',concat(linkfile_ref,'_',replace(file_name,'*',''))) as file_name,parent_object_ref,parent_object_name,linkfile_ref,file_extension from test)

,test4 as (select concat(cast(parent_object_ref as int),'.',linkfile_ref,'.',file_extension) as external_id
,file_name from test3)

select concat([external_id],'*',file_name) from test4
--where parent_object_name = 'person'



