
with
-- FILES
  doc (clientCorporationID,ResumeId) as (
        SELECT clientCorporationID
                     , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.png') /*('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select * from doc
--select distinct fileExtension from bullhorn1.BH_ClientCorporationFile
--select * from bullhorn1.BH_ClientCorporationFile where clientCorporationFileID in (4892,4530,4531,3521,1590)

, dup as (SELECT clientCorporationID,ltrim(rtrim(name)) as name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'
--select * from dup where name like '%bullhorn%' clientcorporationid in (8149,5146,8860,8782)


select --top 100 
         CC.clientCorporationID as 'company-externalId'
       , iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name)) as 'company-name'
       , doc.ResumeId as 'company-document'
-- select count (*) --560 -- select top 10 * -- select distinct CC.status
from bullhorn1.BH_ClientCorporation CC
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
where CC.status <> 'Archive'
