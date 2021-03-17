--PARSE DOCUMENT CONTENT INTO DATABASE COLUMN

--STEP 2: Preconfiguration
USE master
GO
sp_configure 'xp_cmdshell',1
GO
reconfigure WITH override
GO

---STEP 2: CREATE TEMP TABLE
USE misco_parsed_documents
GO
create table document 
(id int identity(1,1)
, doc_name varchar(max)
, doc_content varbinary(max)
)

--STEP 3: CREATE STORED PROCEDURE
USE master
GO
IF EXISTS (SELECT * FROM sys.objects 
   WHERE object_id = OBJECT_ID(N'[dbo].[usp_uploadfiles]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_uploadfiles]

GO
set quoted_identifier off
GO
create procedure usp_uploadfiles 
@databasename varchar(128),
@schemaname varchar(128),
@tablename varchar(128),
@FileNameColumn varchar(128),
@blobcolumn varchar(128),
@path varchar(500), 
@filetype varchar(10),
@printorexec varchar(5) = 'print'
as
set nocount on
declare @dircommand varchar(1500)
declare @insertquery varchar(2000)
declare @updatequery varchar(2000)
declare @count int
declare @maxcount int
declare @filename varchar(500)
set @count=1
set @dircommand = 'dir /b '+@path+@filetype
create table #dir (name varchar(1500))
insert #dir(name) exec master..xp_cmdshell @dircommand
delete from #dir where name is NULL
create table #dir2 (id int identity(1,1), name varchar(1500))
insert into #dir2 select name from #dir

--select * from #dir2
set @maxcount = ident_current('#dir2')
while @count <=@maxcount
begin
set @filename =(select name from #dir2 where id = @count)
set @insertquery = 'Insert into ['+@databasename+'].['+@schemaname+'].['+@tablename+'] 
   ([' +@filenamecolumn +']) values ("'+@filename+'")'
set @updatequery = 'update ['+@databasename+'].['+@schemaname+'].['+@tablename+'] 
   set ['+@blobcolumn+'] = 
   (SELECT * FROM OPENROWSET(BULK "'+@path+@filename+'", SINGLE_BLOB)AS x ) 
   WHERE ['+@filenamecolumn +']="'+@filename+'"'
if @printorexec ='print'
begin
print @insertquery
print @updatequery
end
if @printorexec ='exec'
begin
exec (@insertquery)
exec (@updatequery)
end
set @count = @count +1
end
GO

/* EXPLANATION
@databasename = Name of the database where the schema and table exist
@schemaname = Schema of the database where the table exists
@tablename = Name of the table where files are going to be uploaded
@FileNameColumn = Name of the column in the table where the file name is going to be stored
@blobcolumn = The actual varbinary(max) column where the file is going to be stored as blob data
@path = Path of all the files that are suppose to be uploaded. e.g. “C:\Mike\documents\”
@filetype = Type of file you want to upload. e.g. “*.jpeg”
@printorexec = if “Print” is passed as a parameter it will generate and display the commands. If “Exec” is passed as a parameter it will execute the command directly--meaning upload all the files. 
*/

--STEP 4: PRINT COMMAND
Exec master..usp_uploadfiles 
@databasename ='misco_parsed_documents',
@schemaname ='dbo',
@tablename ='document',
@FileNameColumn ='doc_name',
@blobcolumn = 'doc_content',
@path = 'E:\DataMigration\MiscoMalta\working\documents\',
@filetype ='*.txt',
@printorexec ='print'


--STEP 5: EXECUTE
Exec master..usp_uploadfiles 
@databasename ='misco_parsed_documents',
@schemaname ='dbo',
@tablename ='document_prod',
@FileNameColumn ='doc_name',
@blobcolumn = 'doc_content',
@path = 'E:\DataMigration\MiscoMalta\working\documents\',
@filetype ='*.txt',
@printorexec ='exec'


--STEP 6: CONVERT TO READABLE FORMAT
select doc_name
, doc_content
, convert(varchar(max), doc_content)
from document


---REFERENCE: https://www.databasejournal.com/features/mssql/article.php/3632741/Upload-multiple-files-to-VarBinary-column-in-SQL-Server-2005.htm