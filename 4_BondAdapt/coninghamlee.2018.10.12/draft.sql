
select [57 Cons Perm Xref] from F02 where  [57 Cons Perm Xref] is not null
select [57 Perm Cons Xref] from F02 where  [57 Perm Cons Xref] is not null
select [89 Temp Cons Xref] from F02 where  [89 Temp Cons Xref] is not null
select [33 E-Mail Alphanumeric] from F01 where [33 E-Mail Alphanumeric] is null or [33 E-Mail Alphanumeric] = ''
select [4 Ref No Numeric], [1 Name Alphanumeric],[16 Employer Xref],[33 E-Mail Alphanumeric], * from F01 where [4 Ref No Numeric] in (88780);-- ,114533,100014)
select * from F17 where [5 Initials Alphanumeric] = 'SSW' ;
select *   from F03 where [1 Job Ref Numeric] in (4118, 15481)
select distinct [27 Title Codegroup  16] from F01 where 
select * from ACT where [Field 1] = '8081010199AE8580'
select * from ACT where [Field 2] = '80810201C3CA8080'
select * from ACT where  [Notes 1] like '%him and Sherizia Ramluckan%' or [Notes 1] like  '%Looking for another risk analyst%'
select [72 Email Add Alphanumeric] from F17
select * from F01Docs1 where [Absolute Document Path] like '%8081010189908780-102215-25.msg%'
select * from F01 where [186 Forenames Alphanumeric] like '%Lee%'
  [4 Ref No Numeric] in (114602)
  
select UniqueID from F01 a where a.[100 Contact Codegroup  23] = 'Y' and a.[101 Candidate Codegroup  23] = 'Y' --12649
select count(*) from F01 a where a.[100 Contact Codegroup  23] = 'Y' and a.[101 Candidate Codegroup  23] = 'Y' --12649
-- DOCUMENT
with scd as ( select top 23 UniqueID, REVERSE(LEFT(REVERSE([Relative Document Path]), CHARINDEX('\', REVERSE([Relative Document Path])) - 1)) as FN from F01Docs1 where UniqueID in (select UniqueID from F01 a where a.[100 Contact Codegroup  23] = 'Y' and a.[101 Candidate Codegroup  23] = 'Y') )
, cd as (select UniqueID,  STUFF((select ', ' + x.FN from scd x where x.uniqueID = scd.UniqueID for xml path('')), 1,2,'') as FN FROM scd GROUP BY UniqueID)
select * from scd
select * from F13 where [5 Cand id Numeric] <> ''

update F17 set [72 Email Add Alphanumeric] = 'info@coninghamlee.co.za' where [72 Email Add Alphanumeric] = 'noemailaddress@coninghamlee.co.za'

select
CASE 
    -- x/y/zzzz to xx/yy/zzzz
    WHEN SUBSTRING([16 Lastactnda Date], 2, 1) = '/' AND SUBSTRING([16 Lastactnda Date], 4, 1) = '/' THEN '0' + SUBSTRING([16 Lastactnda Date], 1, 2) + '0' + SUBSTRING([16 Lastactnda Date], 3, 20)
    -- xx/y/zzzz to xx/yy/zzzz
    WHEN SUBSTRING([16 Lastactnda Date], 3, 1) = '/' AND SUBSTRING([16 Lastactnda Date], 5, 1) = '/' THEN SUBSTRING([16 Lastactnda Date], 1, 3) + '0' + SUBSTRING([16 Lastactnda Date], 4, 20)
    -- x/yy/zzzz to xx/yy/zzzz
    WHEN SUBSTRING([16 Lastactnda Date], 2, 1) = '/' AND SUBSTRING([16 Lastactnda Date], 5, 1) = '/' THEN '0' + [16 Lastactnda Date]
    ELSE [16 Lastactnda Date]
END as [FormattedDate]
from F13

alter table bulk_upload_document_mapping add column tmp_txt int
update bulk_upload_document_mapping set tmp_txt = 1 where file_name like '%.txt' 

select file_name from	bulk_upload_document_mapping where file_name like '%.txt' limit 1000
update bulk_upload_document_mapping set file_name = replace(file_name,'.txt','.rtf') where file_name like '%.txt' 
