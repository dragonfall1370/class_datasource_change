
with note as (
	select clientCorporationID
	, Stuff( Coalesce('BH Company ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
                        + Coalesce('Address 2: ' + NULLIF(CC.address2, '') + char(10), '')
                        + Coalesce('Date Added: ' + NULLIF(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
                        + Coalesce('Company Description: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML](CC.companyDescription))), ''), '')
                        + Coalesce('Year Founded: ' + NULLIF(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
                        + Coalesce('Industry: ' + NULLIF(cast(CC.industryList as varchar(max)), '') + char(10), '')
                        + Coalesce('Competitors: ' + NULLIF(cast(CC.competitors as varchar(max)), '') + char(10), '')
                        + Coalesce('Business Sector: ' + NULLIF(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(CC.status, '') + char(10), '')
                        --+ Coalesce('Company Coverage: ' + NULLIF(CC.customText5, '') + char(10), '')
                        + Coalesce('No. of Employees: ' + NULLIF(cast(CC.numEmployees as varchar(max)), '') + char(10), '')
                        + Coalesce('Ownership: ' + NULLIF(CC.ownership, '') + char(10), '')
                        + Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '')
                        + Coalesce('Twitter: ' + NULLIF(CC.twitterHandle, '') + char(10), '')
                        --+ Coalesce('Facebook: ' + NULLIF(CC.facebookProfileName, '') + char(10), '')
                        + Coalesce('LinkedIn: ' + NULLIF(CC.linkedinProfileName, '') + char(10), '')
                        --+ Coalesce('Culture: ' + NULLIF(cast(CC.culture as varchar(max)), '') + char(10), '')
                        + Coalesce('Standard Perm Fee (%): ' + NULLIF(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
                        + Coalesce('Exclusivity: ' + NULLIF(customText3, '') + char(10), '')
                        + Coalesce('Invoice on: ' + NULLIF(customText17, '') + char(10), '')
                        + Coalesce('Permanent Fee Structure: ' + NULLIF(cast(customTextBlock4 as varchar(max)), '') + char(10), '')
                        + Coalesce('Rebate Terms: ' + NULLIF(cast(customTextBlock5 as varchar(max)), '') + char(10), '')
                        + Coalesce('Monthly Internship Fee (): ' + NULLIF(cast(customFloat1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Internship Fee Deductible: ' + NULLIF(customText5, '') + char(10), '')
                        + Coalesce('Month definition: ' + NULLIF(customText10, '') + char(10), '')
                        + Coalesce('Billing Contact: ' + NULLIF(billingContact, '') + char(10), '')
                        + Coalesce('Billing Email: ' + NULLIF(customText2, '') + char(10), '')
                , 1, 0, '') as note
        from bullhorn1.BH_ClientCorporation CC)
--select * from note
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'


/* Get candidates files  */
--, doc0 (clientCorporationID, name) as (select a.clientCorporationID, concat('company-',a.clientCorporationFileID,a.fileExtension) from bullhorn1.BH_ClientCorporationFile a where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf')) --where a.type = 'Resume' ==> get all candidates files
, doc0 (clientCorporationID, name) as (select a.clientCorporationID, concat(a.clientCorporationFileID,a.fileExtension) from bullhorn1.BH_ClientCorporationFile a where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf')) --where a.type = 'Resume' ==> get all candidates files
-- select * from bullhorn1.BH_ClientCorporationFile -- (in ClientCorporationFiles DIRECTORY)
, doc (clientCorporationID,ResumeId) as (SELECT clientCorporationID, STUFF((SELECT DISTINCT ',' + name from doc0 WHERE clientCorporationID = a.clientCorporationID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string FROM doc0 as a GROUP BY a.clientCorporationID)
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null

, dup as (SELECT clientCorporationID,name,ROW_NUMBER() OVER(PARTITION BY CC.name ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'

, headquarter as ( select distinct parentClientCorporationID,h.name from bullhorn1.BH_ClientCorporation c
                left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
                where parentClientCorporationID is not null and parentClientCorporationID <> '' )
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)


select --top 200 
          CC.clientCorporationID as 'company-externalId'
        , iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn)
	, iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	, headquarter.name as 'company-switchboard'
        , ltrim(Stuff( Coalesce(NULLIF(CC.address1, ''), CC.name)
                        + Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        + Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 0, '') ) as 'company-locationName'
        , ltrim(Stuff( Coalesce(NULLIF(CC.address1, ''), CC.name)
                        + Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        + Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 0, '') ) as 'company-locationAddress'
	, CC.city as 'company-locationCity'
	, CC.state as 'company-locationState'
	, Coalesce(NULLIF(tc.abbreviation, ''), '') as 'company-locationCountry'
	, CC.zip as 'company-locationZipCode'
	, CC.phone as 'company-phone'
	, CC.fax as '(company-fax)'
	, CC.companyURL as 'company-website' --limitted by 100 characters
	, doc.ResumeId as 'company-document'
	, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'company-note'
	, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
-- select * -- select count (*) --4962
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.ClientCorporationID in (102,153,226,289,656,774,2056,4936)
