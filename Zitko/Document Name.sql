with filedup as (select Id,iif(Attachment.Name is null or Attachment.Name = '','',Attachment.Name) as filename,ParentId from Attachment)

,filename as (select *,ROW_NUMBER() over ( partition by filename order by filename) as 'row_num' from filedup)

select id, iif(row_num = 1,REPLACE(filename,',','-'),concat(row_num,'-',REPLACE(filename,',','-'))) as filename,ParentId from filename

--select iif(row_num = 1,concat(id,',',REPLACE(filename,',','-')),concat(id,',',row_num,'-',REPLACE(filename,',','-'))) as filename from filename