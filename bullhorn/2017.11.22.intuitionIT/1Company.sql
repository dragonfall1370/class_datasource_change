
with
  dup as (SELECT clientCorporationID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'

, headquarter as ( select distinct parentClientCorporationID,h.name from bullhorn1.BH_ClientCorporation c
                left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
                where parentClientCorporationID is not null and parentClientCorporationID <> '' )
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)

, note as (
	select clientCorporationID
	, Stuff(        Coalesce('Client Corporation ID: ' + NULLIF(convert(varchar(max),CC.clientCorporationID), '') + char(10), '')
                        --+ Coalesce('Status: ' + NULLIF(convert(varchar(max),CC.status), '') + char(10), '')
                        --+ Coalesce('Parent Company: ' + NULLIF(convert(varchar(max),headquarter.name), '') + char(10), '')
                        + Coalesce('Company Comments: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '')
                        --+ Coalesce('Company Ownership: ' + NULLIF(CC.ownership, '') + char(10), '')
                        + Coalesce('Company Description: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML](CC.companyDescription))), ''), '')
                        + Coalesce('Email: ' + NULLIF(CC.customText2, '') + char(10), '')
                        
                        + Coalesce('Secondary Address: ' + NULLIF(CC.customText20, '') + char(10), '')
                        + Coalesce('Secondary Post Code & City: ' + NULLIF(CC.customText17, '') + char(10), '')
                        + Coalesce('Standard Fee Arrangement: ' + NULLIF(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
                        --+ Coalesce('System Date Added: ' + NULLIF(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
                        --+ Coalesce('Year Founded: ' + NULLIF(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
                        --+ Coalesce('Industry: ' + NULLIF(cast(CC.industryList as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Competitors: ' + NULLIF(cast(CC.competitors as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Business Sector: ' + NULLIF(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
                        
                        --+ Coalesce('Company Coverage: ' + NULLIF(CC.customText5, '') + char(10), '')
                        --+ Coalesce('No. of Employees: ' + NULLIF(cast(CC.numEmployees as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Ownership: ' + NULLIF(CC.ownership, '') + char(10), '')
                        
                        --+ Coalesce('Twitter: ' + NULLIF(CC.twitterHandle, '') + char(10), '')
                        --+ Coalesce('Facebook: ' + NULLIF(CC.facebookProfileName, '') + char(10), '')
                        --+ Coalesce('LinkedIn: ' + NULLIF(CC.linkedinProfileName, '') + char(10), '')

                        + Coalesce('Payment Terms: ' + NULLIF(cast(CC.customText12 as varchar(max)), '') + char(10), '')
                        + Coalesce('Tax Rate: ' + NULLIF(cc.customText11, '') + char(10), '')
                        + Coalesce('Bank Name: ' + NULLIF(cc.customText18, '') + char(10), '')
                        
                        + Coalesce('Bank Account Nr: ' + NULLIF(cast(cc.customText16 as varchar(max)), '') + char(10), '')
                        + Coalesce('Sort Code: ' + NULLIF(cast(cc.customText19 as varchar(max)), '') + char(10), '')
                        + Coalesce('IBAN: ' + NULLIF(cc.customText15, '') + char(10), '')
                        + Coalesce('BIC: ' + NULLIF(cc.customText14, '') + char(10), '')
                        + Coalesce('Limited Company Contact: ' + NULLIF(cast(cc.customText8 as varchar(max)), '') + char(10), '')
                        
                        --+ Coalesce('Date added: ' + NULLIF(convert(varchar(10),CC.customdate1,120), '') + char(10), '')
                        + Coalesce('Billing Contact: ' + NULLIF(CC.billingContact, '') + char(10), '')
                        + Coalesce('Billing Email: ' + NULLIF(CC.customText13, '') + char(10), '')
                        --+ Coalesce('Main Location Info: ' + NULLIF(CC.customHeader1, '') + char(10), '')
                        --+ Coalesce('Alternate Phone: ' + NULLIF(CC.customText14, '') + char(10), '')
                        --+ Coalesce('Region: ' + NULLIF(cast(CC.customTextBlock2 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Billing Info: ' + NULLIF(CC.customHeader2, '') + char(10), '')
                        + Coalesce('Billing Phone: ' + NULLIF(CC.billingPhone, '') + char(10), '')
                        + Coalesce('Billing Address 1: ' + NULLIF(CC.billingAddress1, '') + char(10), '')
                        + Coalesce('Billing Address 2: ' + NULLIF(CC.billingAddress2, '') + char(10), '')
                        + Coalesce('Billing Post Code: ' + NULLIF(CC.billingZip, '') + char(10), '')
                        + Coalesce('Billing City: ' + NULLIF(CC.billingCity, '') + char(10), '')
                        --+ Coalesce('Billing P.O. Box: ' + NULLIF(CC.customText10, '') + char(10), '')
                        + Coalesce('Billing Country: ' + NULLIF(CC.customText10, '') + char(10), '')
                        --+ Coalesce('Notes: ' + NULLIF(CC.customText9, '') + char(10), '')
                , 1, 0, '') as note
         --select CC.clientCorporationID --, NULLIF(cast(CC.clientCorporationID as varchar(8000)), '') -- select distinct customText10
        from bullhorn1.BH_ClientCorporation CC
        --left join tmp_country tc ON CC.customText10 = tc.code
        left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID ) --where CC.customText11 is not null )
 --select top 100 * from bullhorn1.BH_ClientCorporation CC
--select top 2000 * from note where clientCorporationID = 931
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'


-- FILES
, doc (clientCorporationID,ResumeId) as (
        SELECT    clientCorporationID
                , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc


select --top 100 
          CC.clientCorporationID as 'company-externalId'
        , iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn), iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	--, headquarter.name as 'company-headquarter'
        , ltrim(Stuff( --Coalesce(NULLIF(CC.address1, ''), CC.name)
                          --Coalesce(' ' + NULLIF(CC.address1, ''), '')
                        --+ Coalesce(', ' + NULLIF(CC.address2, ''), '')
                         Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        --+ Coalesce(', ' + NULLIF(tc.country, ''), '')
                        + Coalesce(', ' + NULLIF(CC.customText9, ''), '')
                , 1, 1, '') ) as 'company-locationName'
        , ltrim(Stuff(    --Coalesce(NULLIF(CC.address1, ''), CC.name)
                          Coalesce(' ' + NULLIF(CC.address1, ''), '')
                        + Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        --+ Coalesce(' ' + NULLIF(CC.address2, ''), CC.name)
                        + Coalesce(', ' + NULLIF(CC.city, ''), '')
                        --+ Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                        + Coalesce(', ' + NULLIF(CC.customText9, ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
	, CC.city as 'company-locationCity'
	--, CC.state as 'company-locationState'
	, Coalesce(NULLIF(replace(tc.abbreviation,'NULL',''), ''), '') as 'company-locationCountry'
	, CC.zip as 'company-locationZipCode'
	, CC.phone as 'company-phone'
	, CC.fax as 'company-fax'
	, CC.companyURL as 'company-website' --limitted by 100 characters
	--, CC.ownership as 'company-owners'
	, doc.ResumeId as 'company-document'
	, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'company-note'
	--, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
-- select count (*) --560 -- select distinct CC.ownership -- CC.state --cc.countryID -- select distinct CC.state
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.ClientCorporationID in (508,517,518)
--where CC.NAME like '%THEQA%'
order by CC.clientCorporationID asc

/* COMMENT - INJECT TO VINCERE
with t as (
select
        CC.clientCorporationID as 'externalId'
        , cast('-10' as int) as userid
        , dateadded as 'comment_timestamp|insert_timestamp'
        ,Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'comment_content'
from bullhorn1.BH_ClientCorporation CC )
select * from t where [comment_content] <> '' --and CC.clientCorporationID = '143'

*/