
declare @RowNum int, @CustId VARCHAR(500), @Name1 VARCHAR(500), @bcpCommand VARCHAR(500)
select @CustId= MAX(DOC_ID) FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31159) --AND SIZE =0 --start with the highest ID
select @RowNum = count(*) FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31159) --AND SIZE =0   --get total number of records : 25148 rows
WHILE @RowNum > 0                                                                                                                      --loop until no more records
BEGIN
    select @Name1 = concat('D:\staranise\candidate\adaptcv_31159\',DOC_ID,'.',case when FILE_EXTENSION = 'txt' then 'rtf' else FILE_EXTENSION end) from StaraniseDomain.dbo.DOCUMENTS where DOC_ID= @CustID    --get other info from that row
	SET @bcpCommand = 'BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = ' + @CustId + '" queryout "' + @Name1 + '" -T -f D:\export.fmt -S '
	EXEC master..xp_cmdshell @bcpCommand
	--print @CustId + ' ' + @Name1  --do whatever
	--print cast(@RowNum as nvarchar(12)) + ' ' + @CustId + ' ' + @Name1  --do whatever

--    select top 1 @CustId = DOC_ID from StaraniseDomain.dbo.DOCUMENTS WHERE SIZE > 45000000 AND DOC_ID < @CustID order by DOC_ID desc--get the next one
    select top 1 @CustId = DOC_ID from StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31159) AND DOC_ID < @CustID order by DOC_ID desc--get the next one
	--select top 1 @CustId = DOC_ID from StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31185,6532841,6532897,6532850) AND DOC_ID < @CustID order by DOC_ID desc--get the next one
    set @RowNum = @RowNum - 1                               --decrease count
END

/*
##########################################################################################################################################################
------------ RUN THESE COMMANDS FIRST ------------
EXEC sp_configure 'show advanced options', 1  
GO  
RECONFIGURE  
GO  
EXEC sp_configure 'xp_cmdshell', 1  
GO  
RECONFIGURE  
GO  
---------- 
Declare @sql varchar(500); 
SET @sql = 'bcp StaraniseDomain.dbo.DOCUMENTS format nul -T -n -f D:\export.fmt -S ' + @@SERVERNAME; 
select @sql;  
EXEC master.dbo.xp_CmdShell @sql; 
----------
DECLARE @SQL VARCHAR(500)
SET @SQL = 'BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = 878292" QueryOut "D:\test.docx" -T -f D:\export.fmt -S '
EXEC MASTER.dbo.xp_CmdShell @SQL
##########################################################################################################################################################

C:\Users\truong> BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = 877991" queryout "D:\test.txt" -T -N
EXEC master..xp_cmdshell 'BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_CATEGORY = 31185 " queryout "D:\temp\exptruong.pdf" -T -N'
in (6532839,6532840,6532918,31159)
##########################################################################################################################################################

COMPANY - PSA Documents:	SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 7022996
Client Description:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023000 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4373 rows
Client Overview:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 6532843 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4845 rows
Client Visit Notes:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023004 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4406 rows
Client Email:				SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 31190 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 52329 rows

CANDIDATE - General Note:		select count(*) from DOCUMENTS where DOC_CATEGORY = 6532839 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> -- Candidate Profile --57536
CANDIDATE - Adapt CV:			SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 31159 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --13737
CANDIDATE - Interview Notes:		select count(*) from DOCUMENTS where DOC_CATEGORY = 6532840 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --6625
CANDIDATE - Adapt CV:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 6532857 AND OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> -- 15760 rows <<<<<<<<<<<<<<<<<<<<

########################
company-note		SELECT * FROM DOCUMENTS where DOC_CATEGORY = 31185 and OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>>
---------
contact-Note		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 6532841 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --22156
--------
job-publicDescription		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532897 and OWNER_ID = <<PROP_JOB_GEN.REFERENCE>> --2543
job-internalDescription		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532850 and OWNER_ID = <<PROP_JOB_GEN.REFERENCE>> --2471
--------
candidate-comments	select count(*) from DOCUMENTS where DOC_CATEGORY = 6532839 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> -- Candidate Profile - General Notes --57536
candidate-resume	SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 31159 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --13737
candidate-note		select count(*) from DOCUMENTS where DOC_CATEGORY = 6532840 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --6625
candidate-photo		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 6532918 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> <<<<<<<<<<<<<<<<<<<< --56

########################

SELECT count (*) from DOCUMENTS where DOC_CATEGORY IN (6532839,6532840,6532918,31159)
SELECT top 10 * FROM StaraniseDomain.dbo.DOCUMENTS 
SELECT distinct FILE_EXTENSION FROM DOCUMENTS
SELECT top 10 doc_id,file_extension FROM DOCUMENTS where DOC_CATEGORY = 31159 and file_extension in ('docx')
select top 10 DOC_ID,DOC_NAME from DOCUMENTS WHERE SIZE > 45000000 order by DOC_ID desc

Ref: https://dba.stackexchange.com/questions/80817/how-to-export-an-image-column-to-files-in-sql-server
*/
