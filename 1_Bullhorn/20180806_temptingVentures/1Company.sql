
with 

-- FILES
  doc (clientCorporationID,ResumeId) as (
        SELECT    clientCorporationID
                , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc

, dup as (SELECT clientCorporationID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'

, headquarter as ( select distinct parentClientCorporationID,h.name 
                from bullhorn1.BH_ClientCorporation c
                left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
                where parentClientCorporationID is not null and parentClientCorporationID <> '' )
--select * from headquarter                
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)

, note as (
	select CC.clientCorporationID
	, Stuff( Coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
+ Coalesce('Billing Address 1: ' + NULLIF(cast(CC.billingAddress1 as varchar(max)), '') + char(10), '')
+ Coalesce('Billing Address 2: ' + NULLIF(cast(CC.billingAddress2 as varchar(max)), '') + char(10), '')
+ Coalesce('Billing City: ' + NULLIF(cast(CC.billingCity as varchar(max)), '') + char(10), '')
+ Coalesce('Billing Contact: ' + NULLIF(cast(CC.billingContact as varchar(max)), '') + char(10), '')
+ Coalesce('Billing Phone: ' + NULLIF(cast(CC.billingPhone as varchar(max)), '') + char(10), '')
+ Coalesce('Billing County: ' + NULLIF(cast(CC.billingState as varchar(max)), '') + char(10), '')
+ Coalesce('Billing Post Code: ' + NULLIF(cast(CC.billingZip as varchar(max)), '') + char(10), '')
+ Coalesce('Commission Cycle: ' + NULLIF(cast(CC.customText3 as varchar(max)), '') + char(10), '')
+ Coalesce('Commission Structure: ' + NULLIF(cast(CC.customTextBlock1 as varchar(max)), '') + char(10), '')
+ Coalesce('Interview Prep: ' + NULLIF(cast(CC.customTextBlock2 as varchar(max)), '') + char(10), '')
+ Coalesce('Standard Fee Arrangement (%): ' + NULLIF(cast(CC.feeArrangement as varchar(max)), '') + char(10), '')
--+ Coalesce('Address: ' + NULLIF(cast(CC.fullAddress as varchar(max)), '') + char(10), '')
--+ Coalesce('Billing Address: ' + NULLIF(cast(CC.fullBillingAddress as varchar(max)), '') + char(10), '')
+ Coalesce('Invoice Format Information: ' + NULLIF(cast(CC.invoiceFormat as varchar(max)), '') + char(10), '')
+ Coalesce('# of Offices: ' + NULLIF(cast(CC.numOffices as varchar(max)), '') + char(10), '')
+ Coalesce('Parent Company: ' + NULLIF(convert(varchar(max),headquarter.name), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(cast(CC.status as varchar(max)), '') + char(10), '')
+ Coalesce('Tax %: ' + NULLIF(cast(CC.taxRate as varchar(max)), '') + char(10), '')
+ Coalesce('Company Description: ' + NULLIF(cast(CC.companyDescription as varchar(max)), '') + char(10), '')
                , 1, 0, '') as note
                -- select  top 10 * -- select companyDescription
        from bullhorn1.BH_ClientCorporation CC --where CC.clientCorporationID = 255
        --left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) pc on pc.clientCorporationID = CC.parentClientCorporationID
        --left join bullhorn1.View_ClientCorporationLastModified v on v.ClientCorporationID = CC.clientCorporationID
        left join headquarter on headquarter.parentClientCorporationID =  CC.parentClientCorporationID
        --left join tmp_country tc ON CC.customText11 = tc.code
        ) --where CC.customText11 is not null )
-- select top 100 * from bullhorn1.BH_ClientCorporation CC
--select clientCorporationID,[dbo].[fn_ConvertHTMLToText](note) from note where clientCorporationID = 255 --where note like '%&%;%'
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'





select --top 100 
          CC.clientCorporationID as 'company-externalId'
        , iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn), iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	, headquarter.name as 'company-headquarter'
         , ltrim(Stuff( --Coalesce(NULLIF(CC.address1, ''), CC.name)
                        --+ Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        --+ Coalesce(NULLIF(CC.address2, ''), CC.name)
                          Coalesce(' ' + NULLIF(CC.address1, ''), '')
                        + Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        + Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
       , ltrim(Stuff( --Coalesce(NULLIF(CC.address1, ''), CC.name)
                          --Coalesce(' ' + NULLIF(CC.address1, ''), '')
                        --+ Coalesce(', ' + NULLIF(CC.address2, ''), '')
                         Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 1, '') ) as 'company-locationName'

	, CC.city as 'company-locationCity'
	, CC.state as 'company-locationState'
	, replace(coalesce(NULLIF(tc.abbreviation, ''), ''),'NULL','') as 'company-locationCountry'
	, CC.zip as 'company-locationZipCode'
	--, CC.phone as 'company-phone'
	, CC.phone as 'company-switchboard'
	--, CC.fax as 'company-fax'
	, CC.companyURL as 'company-website' --limitted by 100 characters
	, CC.ownership as 'company-owners'
	, doc.ResumeId as 'company-document'
	, [dbo].[fn_ConvertHTMLToText](note.note) as 'company-note'
	--, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
-- select count (*) --560 -- select distinct CC.ownership
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.parentClientCorporationID
--where CC.ClientCorporationID in (102,153,226,289,656,774,2056,4936)
--where CC.NAME like '%THEQA%'

/* COMMENT - INJECT TO VINCERE
select top 100
        CC.clientCorporationID as 'externalId'
        , cast('-10' as int) as userid
        , dateadded as 'comment_timestamp|insert_timestamp'
        ,Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'comment_content'
from bullhorn1.BH_ClientCorporation CC
where CC.clientCorporationID = '143'
*/

select
        CC.clientCorporationID as 'externalId'
        , businessSectorList
        , customText1
from bullhorn1.BH_ClientCorporation CC
