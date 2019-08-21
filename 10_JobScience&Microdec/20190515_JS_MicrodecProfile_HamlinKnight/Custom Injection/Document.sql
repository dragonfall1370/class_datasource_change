-----Document rename list------
with test as (select concat(parent_object_ref,'.',linkfile_ref,'.',file_extension) as file_id,
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name'
from linkfile)

 ,test2 as (select *,ROW_NUMBER() over ( partition by file_name order by file_name) as rn from test)


,test3 as ( select file_id, iif(file_name = '' or file_name is null,'',iif(rn=1,file_name,concat(rn,'_',replace(file_name,'*','')))) as file_name from test2)

select * from test3 where file_name <> ''

-----company file
with test as (select 
cast(parent_object_ref as int) as 'external_id',
'COMPANY' as 'entity_type',
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name',
'legal_document' as 'document_type'
 from linkfile where parent_object_name = 'organisation')


 ,test2 as (select *,ROW_NUMBER() over ( partition by file_name order by file_name) as rn from test)

,test3 as ( select external_id,entity_type, document_type, iif(file_name = '' or file_name is null,'',iif(rn=1,file_name,concat(rn,'_',replace(file_name,'*','')))) as file_name from test2)

select * from test3 where file_name <> ''
and external_id = 11011

 ------candidate file

with test as (select 
cast(parent_object_ref as int) as 'external_id',
'CANDIDATE' as 'entity_type',
iif(displayname is null or displayname = '','',concat(replace(replace(replace(replace(replace(displayname,'*',''),'/',''),'\',''),'?',''),':',''),'.',file_extension)) as 'file_name',
'resume' as 'document_type'
 from linkfile where parent_object_name = 'person')


 ,test2 as (select *,ROW_NUMBER() over ( partition by file_name order by file_name) as rn from test)

,test3 as ( select external_id,entity_type, document_type, iif(file_name = '' or file_name is null,'',iif(rn=1,file_name,concat(rn,'_',replace(file_name,'*','')))) as file_name from test2)

select * from test3 where file_name <> ''



