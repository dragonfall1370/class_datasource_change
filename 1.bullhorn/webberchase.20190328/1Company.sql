
with 
note as (
	select CC.clientCorporationID
	, [dbo].[udf_StripHTML](
	Stuff( coalesce('BH Company ID: ' + nullif(cast(CC.clientCorporationID as nvarchar(max)), '') + char(10), '')
              --+ coalesce('Date Last Modified: ' + nullif(cast(v.DateLastModified as varchar(max)), '') + char(10), '')
              --+ coalesce('Parent Company: ' + nullif(cast(CC.parentClientCorporationID as varchar(max)), '') + char(10), '')
--              + coalesce('Billing Address 1: ' + nullif(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
--              + coalesce('Billing Address 2: ' + nullif(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
--              + coalesce('Billing City: ' + nullif(cast(CC.billingCity as varchar(max)), '') + char(10), '')
--              + coalesce('Billing Contact: ' + nullif(cast(CC.billingContact as varchar(max)), '') + char(10), '')
--              + coalesce('Billing Frequency: ' + nullif(cast(CC.billingFrequency as varchar(max)), '') + char(10), '')
--              + coalesce('Billing Phone: ' + nullif(cast(CC.billingPhone as varchar(max)), '') + char(10), '')
--              + coalesce('Billing State: ' + nullif(cast(CC.billingState as varchar(max)), '') + char(10), '')
--              + coalesce('Billing Post Code: ' + nullif(cast(CC.billingZip as varchar(max)), '') + char(10), '')
--              + coalesce('Purchase Orders: ' + nullif(cast(CC.CustomComponent1 as varchar(max)), '') + char(10), '')
--              + coalesce('ABN Number: ' + nullif(cast(CC.customText2 as varchar(max)), '') + char(10), '')
--              + coalesce('Locations: ' + nullif(cast(CC.customText4 as varchar(max)), '') + char(10), '')
--              + coalesce('Date Added: ' + nullif(cast(CC.dateAdded as varchar(max)), '') + char(10), '')
--              + coalesce('Standard Fee Arrangement %: ' + nullif(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
--              + coalesce('Invoice Format: ' + nullif(cast(CC.invoiceFormat as varchar(max)), '') + char(10), '')
--              + coalesce('# of Offices: ' + nullif(cast(CC.numOffices as varchar(max)), '') + char(10), '')
--              + coalesce('Revenue: ' + nullif(cast(CC.revenue as varchar(max)), '') + char(10), '')
--              + coalesce('Status: ' + nullif(cast(CC.status as varchar(max)), '') + char(10), '')             
              --+ coalesce('Fax: ' + nullif(cast(CC.fax as varchar(max)), '') + char(10), '')
              + coalesce('Company Description: ' + nullif(cast(CC.companyDescription as nvarchar(max)), '') + char(10), '')
              + coalesce('Company Overview: ' + nullif(cast(CC.notes as nvarchar(max)), '') + char(10), '')
              --+ coalesce('Opportunities: ' + nullif(cast(CC.opportunityTable as varchar(max)), '') + char(10), '')
              --+ coalesce('Ownership: ' + nullif(cast(CC.ownership as varchar(max)), '') + char(10), '')            
               --+ coalesce('System Date Added: ' + nullif(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
               --+ coalesce('Year Founded: ' + nullif(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
               --+ coalesce('Industry: ' + nullif(cast(CC.industryList as varchar(max)), '') + char(10), '')                      
               --+ coalesce('Business Sector: ' + nullif(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
               --+ coalesce('Company Coverage: ' + nullif(CC.customText5, '') + char(10), '')
               --+ coalesce('No. of Employees: ' + nullif(cast(CC.numEmployees as varchar(max)), '') + char(10), '')
               --+ coalesce('Ownership: ' + nullif(CC.ownership, '') + char(10), '')
               --+ coalesce('Company Overview: ' + nullif([dbo].[udf_StripHTML](CC.notes), '') + char(10), '')
               --+ coalesce('Twitter: ' + nullif(CC.twitterHandle, '') + char(10), '')
               --+ coalesce('Facebook: ' + nullif(CC.facebookProfileName, '') + char(10), '')
               --+ coalesce('LinkedIn: ' + nullif(CC.linkedinProfileName, '') + char(10), '')
               --+ coalesce('Culture: ' + nullif(cast(CC.culture as varchar(max)), '') + char(10), '')
               --+ coalesce('Ownership: ' + nullif(cast(CC.Ownership as varchar(max)), '') + char(10), '')
               --+ coalesce('Exclusivity: ' + nullif(customText3, '') + char(10), '')
               --+ coalesce('Invoice on: ' + nullif(customText17, '') + char(10), '')
               --+ coalesce('Permanent Fee Structure: ' + nullif(cast(customTextBlock4 as varchar(max)), '') + char(10), '')
               --+ coalesce('Rebate Terms: ' + nullif(cast(customTextBlock5 as varchar(max)), '') + char(10), '')
               --+ coalesce('Monthly Internship Fee (): ' + nullif(cast(customFloat1 as varchar(max)), '') + char(10), '')
               --+ coalesce('Internship Fee Deductible: ' + nullif(customText5, '') + char(10), '')
               --+ coalesce('Month definition: ' + nullif(customText10, '') + char(10), '')
               --+ coalesce('Billing Contact: ' + nullif(billingContact, '') + char(10), '')
               --+ coalesce('Billing Email: ' + nullif(customText2, '') + char(10), '')
               --+ coalesce('Date added: ' + nullif(convert(varchar(10),CC.customdate1,120), '') + char(10), '')
               --+ coalesce('Billing Email: ' + nullif(CC.customText2, '') + char(10), '')
               --+ coalesce('Main Location Info: ' + nullif(CC.customHeader1, '') + char(10), '')
               --+ coalesce('Alternate Phone: ' + nullif(CC.customText14, '') + char(10), '')
               --+ coalesce('Region: ' + nullif(cast(CC.customTextBlock2 as varchar(max)), '') + char(10), '')
               --+ coalesce('Billing Info: ' + nullif(CC.customHeader2, '') + char(10), '')
               --+ coalesce('Billing Contact: ' + nullif(cast(CC.billingContact as varchar(max)), '') + char(10), '')
               --+ coalesce('Billing Phone: ' + nullif(CC.customText5, '') + char(10), '')
               --+ coalesce('Billing Address 1: ' + nullif(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
               --+ coalesce('Billing Address 2: ' + nullif(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
               --+ coalesce('Billing City: ' + nullif(cast(CC.billingCity as varchar(max)), '') + char(10), '')
               --+ coalesce('Billing ZIP: ' + nullif(cast(CC.billingZIP as varchar(max)), '') + char(10), '')
               --+ coalesce('SSIC No.: ' + nullif(cast(CC.customInt1 as varchar(max)), '') + char(10), '')
               --+ coalesce('Competitors: ' + nullif(cast(CC.competitors as varchar(max)), '') + char(10), '')
               --+ coalesce('Billing Country: ' + nullif(tc.country, '') + char(10), '')
                , 1, 0, '') 
       ) as note
       -- select  top 10 * -- select companyDescription
        from bullhorn1.BH_ClientCorporation CC --where CC.clientCorporationID = 255
        left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) pc on pc.clientCorporationID = CC.clientCorporationID
        left join bullhorn1.View_ClientCorporationLastModified v on v.ClientCorporationID = CC.clientCorporationID
        --left join tmp_country tc ON CC.customText11 = tc.code
        ) --where CC.customText11 is not null )
-- select top 100 * from bullhorn1.BH_ClientCorporation CC
--select clientCorporationID,[dbo].[fn_ConvertHTMLToText](note) from note where clientCorporationID = 255 --where note like '%&%;%'
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'


-- FILES
/*, doc (clientCorporationID,ResumeId) as (
        SELECT clientCorporationID
                     , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )*/
, doc (clientCorporationID,ResumeId) as ( SELECT clientCorporationID, STRING_AGG(cast(concat(clientCorporationFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY clientCorporationFileID) att from bullhorn1.BH_ClientCorporationFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ GROUP BY clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc where clientCorporationID = 426


, dup as (SELECT clientCorporationID,ltrim(rtrim(name)) as name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'


, headquarter as ( 
       select distinct parentClientCorporationID,h.name 
       from bullhorn1.BH_ClientCorporation c
       left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
       where parentClientCorporationID is not null and parentClientCorporationID <> '' )
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)


, owner0 as (select distinct C.clientCorporationID, C.recruiterUserID, UC.firstName, UC.lastname, UC.email /*, UC.email2, UC.email3, UC.email_old*/ FROM bullhorn1.BH_Client C left join bullhorn1.BH_UserContact UC on UC.userid = C.recruiterUserID where  UC.email like '%_@_%.__%' /*UC.email is not null and UC.email <> ''*/ )
/*, owner as (
       SELECT clientCorporationID
                     , STUFF((SELECT ',' + email from owner0 WHERE clientCorporationID = a.clientCorporationID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS owners
        FROM (select clientCorporationID from owner0) AS a GROUP BY a.clientCorporationID ) */
, owner (clientCorporationID,owners) as (SELECT clientCorporationID, STRING_AGG( email,',' ) WITHIN GROUP (ORDER BY email) att from owner0 GROUP BY clientCorporationID)
--select * from owner where clientCorporationID in (3768, 2606, 2678, 3740, 1246, 747, 4687, 2605, 9228, 9227, 9229)



select --top 10
         CC.clientCorporationID as 'company-externalId'
       , iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name)) as 'company-name'
       --, headquarter.name as 'company-headquarter'
       , ltrim(Stuff( coalesce(' ' + nullif(CC.address1, ''), '') + coalesce(', ' + nullif(CC.address2, ''), '') + coalesce(', ' + nullif(CC.city, ''), '') + coalesce(', ' + nullif(CC.state, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'company-locationAddress'  --coalesce(nullif(CC.address1, ''), CC.name) + coalesce(', ' + nullif(CC.address2, ''), '') + coalesce(nullif(CC.address2, ''), CC.name)
       , ltrim(Stuff( coalesce(', ' + nullif(CC.city, ''), '') + coalesce(', ' + nullif(CC.state, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'company-locationName'  --coalesce(nullif(CC.address1, ''), CC.name) + coalesce(' ' + nullif(CC.address1, ''), '') + coalesce(', ' + nullif(CC.address2, ''), '')
       , CC.city as 'company-locationCity'
       , CC.state as 'company-locationState'
       , CC.zip as 'company-locationZipCode'
       , tc.abbreviation as 'company-locationCountry'
       , CC.phone as 'company-phone'
       --, CC.phone as 'company-switchboard'
       , CC.fax as 'company-fax'
       , LEFT(CC.companyURL, 100) as 'company-website' --[limitted by 100 characters]
        , owner.owners as 'company-owners'
       , doc.ResumeId as 'company-document'
       , note.note as 'company-note'
--       , coalesce('Company Overview: ' + nullif([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
--       , CC.industryList as 'Industry'
--       , CC.numEmployees as 'No. of Employees'
       , CC.dateadded as 'registration date'
-- select count (*) --560 -- select top 10 * -- select distinct CC.ownership
from bullhorn1.BH_ClientCorporation CC
left join owner on owner.clientCorporationID = CC.clientCorporationID
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
where CC.status <> 'Archive' 
--where CC.ClientCorporationID in (102,153,226,289,656,774,2056,4936)
--where CC.NAME like '%THEQA%'

/*
select --top 100 
         CC.clientCorporationID as 'company-externalId'
        , CC.name as 'company-name'
        , CC.numEmployees
       ,  CC.industryList as 'Industry'
from bullhorn1.BH_ClientCorporation CC
where CC.numEmployees <> 0
*/
