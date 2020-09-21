-- Ref: https://dba.stackexchange.com/questions/80817/how-to-export-an-image-column-to-files-in-sql-server

declare @RowNum int, @CustId VARCHAR(500), @Name1 VARCHAR(500), @bcpCommand VARCHAR(500)
select @CustId= MAX(DOC_ID) FROM Initi8_130219.dbo.DOCUMENTS --where DOC_CATEGORY in (31159) --AND SIZE =0 --start with the highest ID
select @RowNum = count(*) FROM Initi8_130219.dbo.DOCUMENTS --where DOC_CATEGORY in (31159) --AND SIZE =0   --get total number of records : 25148 rows
WHILE @RowNum > 0                                                                                                                      --loop until no more records
BEGIN
    select @Name1 = concat('D:\initi8\20190218\doc\',DOC_ID,'.',case when FILE_EXTENSION = 'txt' then 'rtf' else FILE_EXTENSION end) from Initi8_130219.dbo.DOCUMENTS where DOC_ID= @CustID    --get other info from that row
	SET @bcpCommand = 'BCP "SELECT DOCUMENT FROM Initi8_130219.dbo.DOCUMENTS WHERE DOC_ID = ' + @CustId + '" queryout "' + @Name1 + '" -T -f D:\initi8\export.fmt -S '
	EXEC master..xp_cmdshell @bcpCommand
	--print @CustId + ' ' + @Name1  --do whatever
	--print cast(@RowNum as nvarchar(12)) + ' ' + @CustId + ' ' + @Name1  --do whatever

--    select top 1 @CustId = DOC_ID from Initi8_130219.dbo.DOCUMENTS WHERE SIZE > 45000000 AND DOC_ID < @CustID order by DOC_ID desc--get the next one
    select top 1 @CustId = DOC_ID from Initi8_130219.dbo.DOCUMENTS where /*DOC_CATEGORY in (31159) AND*/ DOC_ID < @CustID order by DOC_ID desc--get the next one
	--select top 1 @CustId = DOC_ID from Initi8_130219.dbo.DOCUMENTS where DOC_CATEGORY in (31185,6532841,6532897,6532850) AND DOC_ID < @CustID order by DOC_ID desc--get the next one
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
SET @sql = 'bcp Initi8_130219.dbo.DOCUMENTS format nul -T -n -f D:\initi8\export.fmt -S ' + @@SERVERNAME; 
select @sql;  
EXEC master.dbo.xp_CmdShell @sql; 
----------
DECLARE @SQL VARCHAR(500)
SET @SQL = 'BCP "SELECT DOCUMENT FROM Initi8_130219.dbo.DOCUMENTS WHERE DOC_ID = 18044" queryout "D:\initi8\20190218\doc\test.doc" -T -f D:\initi8\export.fmt -S '
EXEC MASTER.dbo.xp_CmdShell @SQL
##########################################################################################################################################################

C:\Users\truong> BCP "SELECT DOCUMENT FROM Initi8_130219.dbo.DOCUMENTS WHERE DOC_ID = 877991" queryout "D:\test.txt" -T -N
EXEC master..xp_cmdshell 'BCP "SELECT DOCUMENT FROM Initi8_130219.dbo.DOCUMENTS WHERE DOC_CATEGORY = 31185 " queryout "D:\temp\exptruong.pdf" -T -N'
in (6532839,6532840,6532918,31159)
##########################################################################################################################################################

COMPANY - PSA Documents:	SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 7022996
Client Description:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023000 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4373 rows
Client Overview:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 6532843 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4845 rows
Client Visit Notes:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 7023004 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 4406 rows
Client Email:				SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 31190 AND OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>> -- 52329 rows
company-note		SELECT * FROM DOCUMENTS where DOC_CATEGORY = 31185 and OWNER_ID = <<PROP_CLIENT_GEN.REFERENCE>>
---
contact-Note		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 6532841 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --22156
---
job-publicDescription		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532897 and OWNER_ID = <<PROP_JOB_GEN.REFERENCE>> --2543
job-internalDescription		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY =6532850 and OWNER_ID = <<PROP_JOB_GEN.REFERENCE>> --2471
---
CANDIDATE - General Note:		select count(*) from DOCUMENTS where DOC_CATEGORY = 6532839 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> -- Candidate Profile --57536
CANDIDATE - Adapt CV:			SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 31159 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --13737
CANDIDATE - Interview Notes:		select count(*) from DOCUMENTS where DOC_CATEGORY = 6532840 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --6625
CANDIDATE - Adapt CV:			SELECT count(*) FROM DOCUMENTS WHERE DOC_CATEGORY = 6532857 AND OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> -- 15760 rows <<<<<<<<<<<<<<<<<<<<
candidate-comments	select count(*) from DOCUMENTS where DOC_CATEGORY = 6532839 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> -- Candidate Profile - General Notes --57536
candidate-resume	SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 31159 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --13737
candidate-note		select count(*) from DOCUMENTS where DOC_CATEGORY = 6532840 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> --6625
candidate-photo		SELECT count(*) FROM DOCUMENTS where DOC_CATEGORY = 6532918 and OWNER_ID = <<PROP_PERSON_GEN.REFERENCE>> <<<<<<<<<<<<<<<<<<<< --56
*/


/*
create table dbo.DOCUMENTS_TMP
        (DOC_ID int,
        OWNER_ID int,
        DOCUMENT varchar(max),
        UPDATED_DATE datetime
        ) */

SELECT * from DOCUMENTS where DOC_ID IN (97595573, 832823)
SELECT * from DOCUMENTS where doc_description like '%recruit'
SELECT count (*) from DOCUMENTS where DOC_CATEGORY IN (6532839,6532840,6532918,31159)
SELECT top 10 * FROM DOCUMENTS 
SELECT distinct FILE_EXTENSION, count(*) FROM DOCUMENTS group by FILE_EXTENSION
SELECT top 10 doc_id,file_extension FROM DOCUMENTS where DOC_CATEGORY = 31159 and file_extension in ('docx')
select top 10 DOC_ID,DOC_NAME from DOCUMENTS WHERE SIZE > 45000000 order by DOC_ID desc

-- FILE_EXTENSION, amount
select distinct FILE_EXTENSION, count(*) as amount
from DOCUMENTS
group by FILE_EXTENSION


-- DOC_CATEGORY, DESCRIPTION, FILE_EXTENSION, amount
select doc.doc_category, mn.DESCRIPTION, fe.FILE_EXTENSION , count(*) as amount --STRING_AGG( convert(nvarchar(max),doc.FILE_EXTENSION),',') as all_FILE_EXTENSION, 
from DOCUMENTS doc
inner join (SELECT ID, DESCRIPTION FROM MD_MULTI_NAMES WHERE LANGUAGE = 10010) MN ON MN.ID = doc.doc_category 
left join (
       select doc_category, DESCRIPTION, STRING_AGG(FILE_EXTENSION,',') AS FILE_EXTENSION, count(*) as amount
       from ( 
              SELECT doc.doc_category, mn.DESCRIPTION, doc.FILE_EXTENSION
              from DOCUMENTS doc
              inner join (SELECT ID, DESCRIPTION FROM MD_MULTI_NAMES WHERE LANGUAGE = 10010) MN ON MN.ID = doc.doc_category
              GROUP BY doc.doc_category, mn.DESCRIPTION, doc.FILE_EXTENSION
              ) as T  group by doc_category, DESCRIPTION
       ) fe on fe.doc_category = doc.doc_category
group by doc.doc_category, mn.DESCRIPTION, fe.FILE_EXTENSION



-- CONVERT
select 
         doc.doc_id
       , doc.owner_id
       , mn.DESCRIPTION as 'DOC_CATEGORY'
       , doc.DOC_NAME, doc.DOC_DESCRIPTION, doc.FILE_EXTENSION
       , doc.UPDATED_DATE
       , doc.NOTES
       --, doc.DOCUMENT
       --,[dbo].[RTF2TXT]( convert(varchar(max),cast(DOCUMENT as varbinary(max))) )
       --,[dbo].[RTF2TXT2]( convert(varchar(max),cast(DOCUMENT as varbinary(max))) )
       --,[dbo].[RTF2TXT2]([dbo].[RTF2TXT]( convert(varchar(max),cast(DOCUMENT as varbinary(max))) ) )
       --,[dbo].[RTF2Text](convert(varchar(max),cast(DOCUMENT as varbinary(max))) )
       -- , CAST(DECOMPRESS(DOCUMENT) AS NVARCHAR(MAX))        
       , ltrim(replace(replace( [dbo].[udf_StripHTML](convert(varchar(max),convert(varbinary(max),DOCUMENT))) ,'Â',''),'ï»¿','') ) as 'DOCUMENT'  
-- SELECT count (*) --562888 -- select distinct MN.ID, mn.DESCRIPTION, count(*) -- select *
from DOCUMENTS doc
inner join (SELECT ID, DESCRIPTION FROM MD_MULTI_NAMES WHERE LANGUAGE = 10010) MN ON MN.ID = doc.doc_category group by MN.ID, mn.DESCRIPTION
left join PROP_CLIENT_GEN cg on cg.REFERENCE = doc.OWNER_ID --where cg.client_id = 1094161  --[COMPANY]
left join PROP_PERSON_GEN pg on pg.REFERENCE = doc.OWNER_ID --where pg.REFERENCE in (66096, 71174, 44530, 116689764500) or pg.person_id in (1136780) --[CONTACT & CANDIDATE]
left join PROP_JOB_GEN jg on jg.REFERENCE = doc.OWNER_ID --where jg.JOB_ID in (882917) --[JOB]
where cg.REFERENCE is not null
--where pg.REFERENCE is not null
--where jg.REFERENCE is not null
--where FILE_EXTENSION in ('txt','rtf')
and DOC_NAME in ('NOTES')
where owner_id in (44563,74269,101104,161384,438706,440299,668791)
group by cg.REFERENCE having count(*) > 1


-- REFLECTION
select * from PROP_CLIENT_GEN cg where name = 'Liverpool Victoria' or client_id = 1094161
