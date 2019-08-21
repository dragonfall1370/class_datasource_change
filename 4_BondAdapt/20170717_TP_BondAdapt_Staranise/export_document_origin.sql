
declare @RowNum int, @CustId nvarchar(255), @Name1 nvarchar(255), @bcpCommand nvarchar(255)
select @CustId= MAX(DOC_ID) FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (6532840,6532918,31159) --AND SIZE =0 --start with the highest ID
select @RowNum = count(*) FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (6532840,6532918,31159) --AND SIZE =0   --get total number of records : 25148 rows
WHILE @RowNum > 0                                                                                                                      --loop until no more records
BEGIN
    select @Name1 = concat('D:\candidate\',DOC_ID,'.',case when FILE_EXTENSION = 'txt' then 'rtf' else FILE_EXTENSION end) from StaraniseDomain.dbo.DOCUMENTS where DOC_ID= @CustID    --get other info from that row
	SET @bcpCommand = 'BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = ' + @CustId + '" queryout "' + @Name1 + '" -T -N'
	EXEC master..xp_cmdshell @bcpCommand
	--print @CustId + ' ' + @Name1  --do whatever
	--print cast(@RowNum as nvarchar(12)) + ' ' + @CustId + ' ' + @Name1  --do whatever

--    select top 1 @CustId = DOC_ID from StaraniseDomain.dbo.DOCUMENTS WHERE SIZE > 45000000 AND DOC_ID < @CustID order by DOC_ID desc--get the next one
    select top 1 @CustId = DOC_ID from StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (6532840,6532918,31159) AND DOC_ID < @CustID order by DOC_ID desc--get the next one
	--select top 1 @CustId = DOC_ID from StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31185,6532841,6532897,6532850) AND DOC_ID < @CustID order by DOC_ID desc--get the next one
    set @RowNum = @RowNum - 1                               --decrease count
END

/*
------------
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


C:\Users\truong> BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = 877991" queryout "D:\test.txt" -T -N
EXEC master..xp_cmdshell 'BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_CATEGORY = 31185 " queryout "D:\temp\exptruong.pdf" -T -N'
in (6532839,6532840,6532918,31159)
-----
SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =31185   --and OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> --COMPANY
SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532841 --and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --CONTACT
SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532897 --and OWNER_ID = <<PROP_JOB_GEN.REFERENCE>> --job-publicDescription
SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532850 --and OWNER_ID = <<PROP_JOB_GEN.REFERENCE>> --job-internalDescription
select count(*) FROM DOCUMENTS where DOC_CATEGORY = 6532839
SELECT top 10 doc_id,file_extension FROM DOCUMENTS where DOC_CATEGORY = 31159 and file_extension in ('docx')
SELECT top 10 * FROM StaraniseDomain.dbo.DOCUMENTS 

select count(*) FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31185,6532841,6532897,6532850) AND SIZE > 100000
select count(*) FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31185,6532841,6532897,6532850) --27172 rows
select FILE_EXTENSION FROM StaraniseDomain.dbo.DOCUMENTS where DOC_CATEGORY in (31185,6532841,6532897,6532850)

SELECT distinct FILE_EXTENSION FROM DOCUMENTS
SELECT count (*) from DOCUMENTS

select top 1 DOC_ID,DOC_NAME from StaraniseDomain.dbo.DOCUMENTS WHERE SIZE > 45000000 order by DOC_ID desc

SELECT * FROM StaraniseDomain.dbo.DOCUMENTS --where SIZE = 0 -- order by SIZE desc
--where DOC_CATEGORY = 31185 -- and OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>>
where DOC_ID = 877718


-----------
declare @filename varbinary(max) = (select DOC_NAME from StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = @fileid)

declare bcpCommand varchar(1000)
declare @filepath nvarchar(4000) = N'D:\temp\' + (select DOC_ID from StaraniseDomain.dbo.DOCUMENTS) + 
SET @bcpCommand = 'bcp "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_CATEGORY = 31185 + '" queryout "' + @filepath + '" -T -N'
EXEC master..xp_cmdshell @bcpCommand

EXEC master..xp_cmdshell 'BCP "SELECT DOCUMENT FROM StaraniseDomain.dbo.DOCUMENTS WHERE DOC_ID = 1468889 " queryout "D:\temp\Molly Hoi-Yin Wong.pdf" -T -N'
*/