
with 
note as (
	select CC.clientCorporationID
	, Stuff( Coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing Address 1: ' + NULLIF(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing Address 2: ' + NULLIF(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing City: ' + NULLIF(cast(CC.billingCity as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing Contact: ' + NULLIF(cast(CC.billingContact as varchar(max)), '') + char(10), '')
--              --+ Coalesce('Billing Frequency: ' + NULLIF(cast(CC.billingFrequency as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing Phone: ' + NULLIF(cast(CC.billingPhone as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing State: ' + NULLIF(cast(CC.billingState as varchar(max)), '') + char(10), '')
--              + Coalesce('Billing Post Code: ' + NULLIF(cast(CC.billingZip as varchar(max)), '') + char(10), '')
              --+ Coalesce('Purchase Orders: ' + NULLIF(cast(CC.CustomComponent1 as varchar(max)), '') + char(10), '')
              + Coalesce('Discussed Payroll Solutions: ' + NULLIF(cast(CC.customText11 as varchar(max)), '') + char(10), '')
              + Coalesce('TOB Signed: ' + NULLIF(cast(CC.customText3 as varchar(max)), '') + char(10), '')
--              + Coalesce('Date Added: ' + NULLIF(cast(CC.dateAdded as varchar(max)), '') + char(10), '')
--              + Coalesce('Date Last Modified: ' + NULLIF(cast(v.DateLastModified as varchar(max)), '') + char(10), '')
--              + Coalesce('Standard Fee Arrangement %: ' + NULLIF(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
--              + Coalesce('Invoice Format: ' + NULLIF(cast(CC.invoiceFormat as varchar(max)), '') + char(10), '')
--              + Coalesce('# of Offices: ' + NULLIF(cast(CC.numOffices as varchar(max)), '') + char(10), '')
--              + Coalesce('Parent Company: ' + NULLIF(cast(CC.parentClientCorporationID as varchar(max)), '') + char(10), '')
--              + Coalesce('Revenue: ' + NULLIF(cast(CC.revenue as varchar(max)), '') + char(10), '')
--              + Coalesce('Status: ' + NULLIF(cast(CC.status as varchar(max)), '') + char(10), '')             
              --+ Coalesce('Fax: ' + NULLIF(cast(CC.fax as varchar(max)), '') + char(10), '')
              + Coalesce('Ownership: ' + NULLIF(cast(CC.ownership as varchar(max)), '') + char(10), '')            
              + Coalesce('Company Description: ' + NULLIF(cast(CC.companyDescription as varchar(max)), '') + char(10), '')
              + Coalesce('Company Comments: ' + NULLIF(cast(CC.notes as varchar(max)), '') + char(10), '')
              --+ Coalesce('Opportunities: ' + NULLIF(cast(CC.opportunityTable as varchar(max)), '') + char(10), '')
              
               --+ Coalesce('System Date Added: ' + NULLIF(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
               --+ Coalesce('Year Founded: ' + NULLIF(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
               --+ Coalesce('Industry: ' + NULLIF(cast(CC.industryList as varchar(max)), '') + char(10), '')                      
               --+ Coalesce('Business Sector: ' + NULLIF(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
               --+ Coalesce('Company Coverage: ' + NULLIF(CC.customText5, '') + char(10), '')
               --+ Coalesce('No. of Employees: ' + NULLIF(cast(CC.numEmployees as varchar(max)), '') + char(10), '')
               --+ Coalesce('Ownership: ' + NULLIF(CC.ownership, '') + char(10), '')
               --+ Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '')
               --+ Coalesce('Twitter: ' + NULLIF(CC.twitterHandle, '') + char(10), '')
               --+ Coalesce('Facebook: ' + NULLIF(CC.facebookProfileName, '') + char(10), '')
               --+ Coalesce('LinkedIn: ' + NULLIF(CC.linkedinProfileName, '') + char(10), '')
               --+ Coalesce('Culture: ' + NULLIF(cast(CC.culture as varchar(max)), '') + char(10), '')
               --+ Coalesce('Ownership: ' + NULLIF(cast(CC.Ownership as varchar(max)), '') + char(10), '')
               --+ Coalesce('Exclusivity: ' + NULLIF(customText3, '') + char(10), '')
               --+ Coalesce('Invoice on: ' + NULLIF(customText17, '') + char(10), '')
               --+ Coalesce('Permanent Fee Structure: ' + NULLIF(cast(customTextBlock4 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Rebate Terms: ' + NULLIF(cast(customTextBlock5 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Monthly Internship Fee (): ' + NULLIF(cast(customFloat1 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Internship Fee Deductible: ' + NULLIF(customText5, '') + char(10), '')
               --+ Coalesce('Month definition: ' + NULLIF(customText10, '') + char(10), '')
               --+ Coalesce('Billing Contact: ' + NULLIF(billingContact, '') + char(10), '')
               --+ Coalesce('Billing Email: ' + NULLIF(customText2, '') + char(10), '')
               --+ Coalesce('Date added: ' + NULLIF(convert(varchar(10),CC.customdate1,120), '') + char(10), '')
               --+ Coalesce('Billing Email: ' + NULLIF(CC.customText2, '') + char(10), '')
               --+ Coalesce('Main Location Info: ' + NULLIF(CC.customHeader1, '') + char(10), '')
               --+ Coalesce('Alternate Phone: ' + NULLIF(CC.customText14, '') + char(10), '')
               --+ Coalesce('Region: ' + NULLIF(cast(CC.customTextBlock2 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Billing Info: ' + NULLIF(CC.customHeader2, '') + char(10), '')
               --+ Coalesce('Billing Contact: ' + NULLIF(cast(CC.billingContact as varchar(max)), '') + char(10), '')
               --+ Coalesce('Billing Phone: ' + NULLIF(CC.customText5, '') + char(10), '')
               --+ Coalesce('Billing Address 1: ' + NULLIF(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Billing Address 2: ' + NULLIF(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Billing City: ' + NULLIF(cast(CC.billingCity as varchar(max)), '') + char(10), '')
               --+ Coalesce('Billing ZIP: ' + NULLIF(cast(CC.billingZIP as varchar(max)), '') + char(10), '')
               --+ Coalesce('SSIC No.: ' + NULLIF(cast(CC.customInt1 as varchar(max)), '') + char(10), '')
               --+ Coalesce('Competitors: ' + NULLIF(cast(CC.competitors as varchar(max)), '') + char(10), '')
               --+ Coalesce('Billing Country: ' + NULLIF(tc.country, '') + char(10), '')
                , 1, 0, '') as note
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
, doc (clientCorporationID,ResumeId) as (
        SELECT clientCorporationID
                     , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc


, dup as (SELECT clientCorporationID,ltrim(rtrim(name)) as name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'
--select * from dup where name like '%bullhorn%' clientcorporationid in (8149,5146,8860,8782)

, headquarter as ( 
       select distinct parentClientCorporationID,h.name 
       from bullhorn1.BH_ClientCorporation c
       left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
       where parentClientCorporationID is not null and parentClientCorporationID <> '' )
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)



select --top 100 
         CC.clientCorporationID as 'company-externalId'
       , iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name)) as 'company-name'
       --, headquarter.name as 'company-headquarter'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(CC.address1, ''), '') + Coalesce(', ' + NULLIF(CC.address2, ''), '') + Coalesce(', ' + NULLIF(CC.city, ''), '') + Coalesce(', ' + NULLIF(CC.state, ''), '') + Coalesce(', ' + NULLIF(tc.country, ''), '') , 1, 1, '') ) as 'company-locationAddress'  --Coalesce(NULLIF(CC.address1, ''), CC.name) + Coalesce(', ' + NULLIF(CC.address2, ''), '') + Coalesce(NULLIF(CC.address2, ''), CC.name)
       , ltrim(Stuff( Coalesce(', ' + NULLIF(CC.city, ''), '') + Coalesce(', ' + NULLIF(CC.state, ''), '') + Coalesce(', ' + NULLIF(tc.country, ''), '') , 1, 1, '') ) as 'company-locationName'  --Coalesce(NULLIF(CC.address1, ''), CC.name) + Coalesce(' ' + NULLIF(CC.address1, ''), '') + Coalesce(', ' + NULLIF(CC.address2, ''), '')
       , CC.city as 'company-locationCity'
       , CC.state as 'company-locationState'
       , tc.abbreviation as 'company-locationCountry'
       , CC.zip as 'company-locationZipCode'
       , CC.phone as 'company-phone'
       --, CC.phone as 'company-switchboard'
       , CC.fax as '#company-fax'
       , LEFT(CC.companyURL, 100) as 'company-website' --[limitted by 100 characters]
       , CC.ownership as 'company-owners'
       , doc.ResumeId as 'company-document'
       , [dbo].[fn_ConvertHTMLToText](note.note) as 'company-note'
       --, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
      --, CC.industryList as 'Industry'
       --, CC.numEmployees as 'No. of Employees'
-- select count (*) --560 -- select * -- select distinct CC.ownership
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.ClientCorporationID in (8149,5146,8860,8782)
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
