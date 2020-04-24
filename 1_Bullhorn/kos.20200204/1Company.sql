
with 
  dup as (SELECT clientCorporationID,ltrim(rtrim(name)) as name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'

-- FILES
/*, doc (clientCorporationID,ResumeId) as (
        SELECT clientCorporationID
                     , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )*/
, doc (clientCorporationID,ResumeId) as ( SELECT clientCorporationID, STRING_AGG(cast(concat(clientCorporationFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY clientCorporationFileID) att from bullhorn1.BH_ClientCorporationFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ GROUP BY clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc where clientCorporationID = 426


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


, note as (
	select CC.clientCorporationID
	       , [bullhorn1].[fn_ConvertHTMLToText](Stuff(
	        coalesce('BH Company ID: ' + nullif(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')

+ Coalesce('Billing Contact: ' + NULLIF(convert(nvarchar(max),cc.billingContact), '') + char(10), '')
 
+ Coalesce('Priority: ' + NULLIF(convert(nvarchar(max),cc.customText1), '') + char(10), '')
--+ Coalesce('HK District: ' + NULLIF(convert(nvarchar(max),cc.customText3), '') + char(10), '')
+ coalesce('Date Last Modified: ' + nullif(cast(v.DateLastModified as nvarchar(max)), '') + char(10), '')
              + coalesce('Standard Fee Arrangement %: ' + nullif(cast(CC.feeArrangement as nvarchar(max)), '') + char(10), '')

+ coalesce('Parent Company: ' + nullif(cast(headquarter.name as nvarchar(max)), '') + char(10), '')
              + coalesce('Status: ' + nullif(cast(CC.status as nvarchar(max)), '') + char(10), '')                           

--              + coalesce('Business Sector: ' + nullif(cast(CC.businessSectorList as nvarchar(max)), '') + char(10), '')
--              
--              + coalesce('Competitors: ' + nullif(cast(CC.competitors as nvarchar(max)), '') + char(10), '')
--              + coalesce('Culture: ' + nullif(cast(CC.culture as nvarchar(max)), '') + char(10), '')
--              + coalesce('Date Added: ' + nullif(cast(CC.dateAdded as nvarchar(max)), '') + char(10), '')
--              
--              + coalesce('Facebook: ' + nullif(cast(CC.facebookProfileName as nvarchar(max)), '') + char(10), '')
--              + coalesce('Fax: ' + nullif(cast(CC.fax as nvarchar(max)), '') + char(10), '')
--              + coalesce('Industry: ' + nullif(cast(CC.industryList as nvarchar(max)), '') + char(10), '')                      
--              + coalesce('Invoice Format: ' + nullif(cast(CC.invoiceFormat as nvarchar(max)), '') + char(10), '')
--              + coalesce('LinkedIn: ' + nullif(cast(CC.linkedinProfileName as nvarchar(max)), '') + char(10), '')
--              + coalesce('No. of Employees: ' + nullif(cast(CC.numEmployees as nvarchar(max)), '') + char(10), '')
--              + coalesce('# of Offices: ' + nullif(cast(CC.numOffices as nvarchar(max)), '') + char(10), '')
--              + coalesce('Opportunities: ' + nullif(cast(CC.opportunityTable as nvarchar(max)), '') + char(10), '')
--              + coalesce('Ownership: ' + nullif(cast(CC.Ownership as nvarchar(max)), '') + char(10), '')
--                            
--              + coalesce('Revenue: ' + nullif(cast(CC.revenue as nvarchar(max)), '') + char(10), '')
--
--
--              + coalesce('System Date Added: ' + nullif(convert(nvarchar(10),CC.dateAdded,120), '') + char(10), '')
--              + coalesce('Twitter: ' + nullif(convert(CC.twitterHandle as nvarchar(max)), '') + char(10), '')
--              + coalesce('Year Founded: ' + nullif(convert(nvarchar(4),CC.dateFounded,120), '') + char(10), '')

--              + coalesce('Billing Contact: ' + nullif(cast(CC.billingContact as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Address 1: ' + nullif(cast(CC.billingAddress1 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Address 2: ' + nullif(cast(CC.billingAddress2 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing City: ' + nullif(cast(CC.billingCity as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing State: ' + nullif(cast(CC.billingState as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Post Code: ' + nullif(cast(CC.billingZip as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Country: ' + nullif(cast(tc.country as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Frequency: ' + nullif(cast(CC.billingFrequency as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Phone: ' + nullif(cast(CC.billingPhone as nvarchar(max)), '') + char(10), '')
              + Coalesce('Company Description: ' + NULLIF(convert(nvarchar(max),cc.companyDescription), '') + char(10),'')
              + coalesce('Company Comments: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](CC.notes), '') + char(10), '') --Company Overview
                , 1, 0, '') 
       ) as note
       -- select  top 10 * -- select companyDescription
        from bullhorn1.BH_ClientCorporation CC --where CC.clientCorporationID = 255
        left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
        left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) pc on pc.clientCorporationID = CC.clientCorporationID
        left join bullhorn1.View_ClientCorporationLastModified v on v.ClientCorporationID = CC.clientCorporationID
        --left join tmp_country tc ON CC.customText11 = tc.code
        ) --where CC.customText11 is not null )
-- select top 100 * from bullhorn1.BH_ClientCorporation CC
--select clientCorporationID,[dbo].[fn_ConvertHTMLToText](note) from note where clientCorporationID = 255 --where note like '%&%;%'
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'



select --top 10
         CC.clientCorporationID as 'company-externalId'
       , case 
              when CC.status = 'Archive' then concat(iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name)),' (Archive)' )
              else iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name))
              end as 'company-name'
       , headquarter.name as 'company-headquarter'
       , ltrim(Stuff( coalesce(' ' + nullif(CC.address1, ''), '') + coalesce(', ' + nullif(CC.address2, ''), '') + coalesce(', ' + nullif(CC.city, ''), '') + coalesce(', ' + nullif(CC.state, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'company-locationAddress'  --coalesce(nullif(CC.address1, ''), CC.name) + coalesce(', ' + nullif(CC.address2, ''), '') + coalesce(nullif(CC.address2, ''), CC.name)
       , ltrim(Stuff( coalesce(', ' + nullif(CC.city, ''), '') + coalesce(', ' + nullif(CC.state, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'company-locationName'  --coalesce(nullif(CC.address1, ''), CC.name) + coalesce(' ' + nullif(CC.address1, ''), '') + coalesce(', ' + nullif(CC.address2, ''), '')
       , CC.city as 'company-locationCity'
       , CC.state as 'company-locationState'
       , CC.zip as 'company-locationZipCode'
       , tc.abbreviation as 'company-locationCountry'
       , CC.phone as 'company-phone'
       , CC.phone as 'company-switchboard'
       , CC.fax as 'company-fax'
       , LEFT(CC.companyURL, 100) as 'company-website' --[limitted by 100 characters]
       , owner.owners as 'company-owners'
       , doc.ResumeId as 'company-document'
       , note.note as 'company-note'

--       , coalesce('Company Overview: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](CC.notes), '') + char(10), '') as 'company-comment'
--       , CC.industryList as 'Industry'
       , CC.numEmployees as 'No. of Employees'
       , CC.dateadded as 'registration date'
-- select count (*) --560 -- select top 10 * -- select distinct CC.ownership
from bullhorn1.BH_ClientCorporation CC
left join owner on owner.clientCorporationID = CC.clientCorporationID
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.status = 'Archive' 
--where CC.ClientCorporationID in (1293)
--where CC.NAME like '%THEQA%'



-- Client Type
select distinct customText2 as 'Client Type' from bullhorn1.BH_ClientCorporation CC where customText2 is not null and customText2 not in ('','Please Select')

SELECT
         clientCorporationID as additional_id
        , 'add_com_info' as 'additional_type'
        , 1008 as 'form_id'
        , 11265 as 'field_id'
        , case
when customText2 = 'China Domestic' then '1'
when customText2 = 'Hong Kong' then '2'
when customText2 = 'MNC' then '3'
              end as 'field_value'
        , 11265 as 'constraint_id'
from bullhorn1.BH_ClientCorporation CC 
where customText2 is not null and customText2 not in ('','Please Select')



-- HK District
select distinct customText3 as 'HK District' from bullhorn1.BH_ClientCorporation CC where customText3 is not null and customText3 not in ('','Please Select') order by customText3 asc
SELECT
         clientCorporationID as additional_id
        , 'add_com_info' as 'additional_type'
        , 1008 as 'form_id'
        , 11266 as 'field_id'
        , case
when customText3 = 'Central and Western' then '1'
when customText3 = 'Eastern' then '2'
when customText3 = 'Islands' then '3'
when customText3 = 'Kowloon City' then '4'
when customText3 = 'Kwai Tsing' then '5'
when customText3 = 'Kwun Tong' then '6'
when customText3 = 'North' then '7'
when customText3 = 'Sai Kung' then '8'
when customText3 = 'Sha Tin' then '9'
when customText3 = 'Sham Shui Po' then '10'
when customText3 = 'Southern' then '11'
when customText3 = 'Tai Po' then '12'
when customText3 = 'Tsuen Wan' then '13'
when customText3 = 'Tuen Mun' then '14'
when customText3 = 'Wan Chai' then '15'
when customText3 = 'Wong Tai Sin' then '16'
when customText3 = 'Yau Tsim Mong' then '17'
when customText3 = 'Yuen Long' then '18'
              end as 'field_value'
        , 11266 as 'constraint_id'
from bullhorn1.BH_ClientCorporation CC 
where customText3 is not null and customText3 not in ('','Please Select')


-- Priority
select distinct
         CC.clientCorporationID  as additional_id , cc.name
        , 'add_com_info' as 'additional_type'
        , 1008 as 'form_id'
        , 11273 as 'field_id'
        , case
when customtext1 = 'A' then '1'
when customtext1 = 'B' then '2'
when customtext1 = 'C' then '3'
end as 'field_value'
        , 11273 as 'constraint_id'
from bullhorn1.BH_ClientCorporation CC         
where cc.customtext1 is not null and customtext1 <> ''


/*
select
         CC.clientCorporationID as 'company-externalId'
        , CC.name as 'company-name'
        , CC.status
from bullhorn1.BH_ClientCorporation CC where CC.clientCorporationID in (1619, 937, 683)

select --top 100 
         CC.clientCorporationID as 'company-externalId'
        , CC.name as 'company-name'
        , CC.numEmployees
       ,  CC.industryList as 'Industry'
from bullhorn1.BH_ClientCorporation CC
where CC.numEmployees <> 0


select
         cc.clientCorporationID as CompanyExtId
       , left(concat_ws(', ', nullif(billingAddress1,''), nullif(billingAddress2,''), nullif(cc.billingCity,''), nullif(cc.billingState,''), nullif(cc.billingZip,''), nullif(tc.COUNTRY,'')),300) as locationName
       , left(concat_ws(', ', nullif(billingAddress1,''), nullif(billingAddress2,''), nullif(cc.billingCity,''), nullif(cc.billingState,''), nullif(cc.billingZip,''), nullif(tc.COUNTRY,'')),300) as locationAddress
       , cc.billingCity as city
       , cc.billingState as [state]
       , cc.billingZip as post_code
       , cc.billingPhone as phone_number
       , tc.ABBREVIATION as country_code
       , 'BILLING_ADDRESS' as location_type
       , getdate() as insert_timestamp
from bullhorn1.BH_ClientCorporation cc
left join tmp_country tc ON tc.code = CC.billingCountryID
where billingAddress1 <> '' or billingAddress2 <> '' or billingCountryID <> ''

*/
