
with
-- FILES
/*, doc (clientCorporationID,ResumeId) as (
        SELECT clientCorporationID
                     , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )*/
  doc (clientCorporationID,ResumeId) as ( SELECT clientCorporationID, STRING_AGG(cast(concat(clientCorporationFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY clientCorporationFileID) att from bullhorn1.BH_ClientCorporationFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ GROUP BY clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc where clientCorporationID = 426


, dup as (SELECT clientCorporationID,ltrim(rtrim(name)) as name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'


, headquarter as ( 
       select distinct parentClientCorporationID, h.name 
       from bullhorn1.BH_ClientCorporation c
       left join (select clientCorporationID, NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
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
	, [bullhorn1].[fn_ConvertHTMLToText](
	Stuff(   coalesce('BH Company ID: ' + nullif(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
--              + coalesce('Owners: ' + nullif(cast(CC.customText6 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Account Type: ' + nullif(cast(CC.customText1 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Service Office: ' + nullif(cast(CC.customText13 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Region: ' + nullif(cast(CC.customText2 as nvarchar(max)), '') + char(10), '')              
--              + coalesce('Currency: ' + nullif(cast(CC.customText5 as nvarchar(max)), '') + char(10), '')              
--              + coalesce('Date Last Modified: ' + nullif(cast(v.DateLastModified as nvarchar(max)), '') + char(10), '')
--              + coalesce('Parent Company: ' + nullif(cast(headquarter.name as nvarchar(max)), '') + char(10), '')              
--              + coalesce('Status: ' + nullif(cast(CC.status as nvarchar(max)), '') + char(10), '')                           
--              + coalesce('Billing Contact: ' + nullif(cast(CC.billingContact as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Address 1: ' + nullif(cast(CC.billingAddress1 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Address 2: ' + nullif(cast(CC.billingAddress2 as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing City: ' + nullif(cast(CC.billingCity as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing State: ' + nullif(cast(CC.billingState as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Post Code: ' + nullif(cast(CC.billingZip as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Country: ' + nullif(cast(tc.country as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Frequency: ' + nullif(cast(CC.billingFrequency as nvarchar(max)), '') + char(10), '')
--              + coalesce('Billing Phone: ' + nullif(cast(CC.billingPhone as nvarchar(max)), '') + char(10), '')
--              + coalesce('Business Sector: ' + nullif(cast(CC.businessSectorList as nvarchar(max)), '') + char(10), '')
              + coalesce('Company Description: ' + nullif(cast(CC.companyDescription as nvarchar(max)), '') + char(10), '')
--              + coalesce('Critical Info: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](CC.notes), '') + char(10), '') --Company Overview
--              + coalesce('Competitors: ' + nullif(cast(CC.competitors as nvarchar(max)), '') + char(10), '')
--              + coalesce('Culture: ' + nullif(cast(CC.culture as nvarchar(max)), '') + char(10), '')
--              + coalesce('Date Added: ' + nullif(cast(CC.dateAdded as nvarchar(max)), '') + char(10), '')
--              + coalesce('Facebook: ' + nullif(cast(CC.facebookProfileName as nvarchar(max)), '') + char(10), '')
--              + coalesce('Fax: ' + nullif(cast(CC.fax as nvarchar(max)), '') + char(10), '')
--              + coalesce('Industry: ' + nullif(cast(CC.industryList as nvarchar(max)), '') + char(10), '')                      
--              + coalesce('Invoice Format: ' + nullif(cast(CC.invoiceFormat as nvarchar(max)), '') + char(10), '')
--              + coalesce('LinkedIn: ' + nullif(cast(CC.linkedinProfileName as nvarchar(max)), '') + char(10), '')
--              + coalesce('No. of Employees: ' + nullif(cast(CC.numEmployees as nvarchar(max)), '') + char(10), '')
--              + coalesce('# of Offices: ' + nullif(cast(CC.numOffices as nvarchar(max)), '') + char(10), '')
--              + coalesce('Opportunities: ' + nullif(cast(CC.opportunityTable as nvarchar(max)), '') + char(10), '')
--              + coalesce('Ownership: ' + nullif(cast(CC.Ownership as nvarchar(max)), '') + char(10), '')
--              + coalesce('Revenue: ' + nullif(cast(CC.revenue as nvarchar(max)), '') + char(10), '')
--              + coalesce('Standard Fee Arrangement %: ' + nullif(cast(CC.feeArrangement as nvarchar(max)), '') + char(10), '')
--              + coalesce('System Date Added: ' + nullif(convert(nvarchar(10),CC.dateAdded,120), '') + char(10), '')
--              + coalesce('Twitter: ' + nullif(convert(CC.twitterHandle as nvarchar(max)), '') + char(10), '')
--              + coalesce('Year Founded: ' + nullif(convert(nvarchar(4),CC.dateFounded,120), '') + char(10), '')
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
         CC.clientCorporationID as 'company-externalId', CC.status
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
--       , LEFT(CC.companyURL, 100) as 'company-website' --[limitted by 100 characters]
--       , owner.owners as 'company-owners'
       , doc.ResumeId as 'company-document'
       , note.note as 'company-note'
       , CC.dateadded as 'registration date'

--       , coalesce('Company Overview: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](CC.notes), '') + char(10), '') as 'company-comment'
--       , CC.industryList as 'Industry'
--       , CC.numEmployees as 'No. of Employees'
       , CC.customText11 as 'Custom Field > Contract Type'
-- select count (*) --560 -- select top 10 * -- select distinct CC.ownership -- select distinct customText6 -- select distinct companyURL -- select notes, len( convert(nvarchar(max),notes)) as len
from bullhorn1.BH_ClientCorporation CC --where CC.status <> 'Archive'
left join owner on owner.clientCorporationID = CC.clientCorporationID
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.status <> 'Archive'
--and CC.clientcorporationid not in (select distinct CC.clientcorporationid from bullhorn1.BH_ClientCorporation CC left join bullhorn1.BH_JobPosting j on j.clientcorporationid = cc.clientcorporationid where CC.countryID = 1 and j.clientcorporationid is null)
--and CC.ClientCorporationID in (644)
--where CC.NAME like '%THEQA%'

--select * from tmp_country


/*
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


-- Company Owners
--select distinct customText6 from bullhorn1.BH_ClientCorporation CC
with
customText6 (clientCorporationID,customText6) as (
       SELECT 
              clientCorporationID
              , trim( replace(replace(replace(customText6.value,'  ',' '),' )',')'),'( ','(') ) as customText6 --, trim( ind.value ) as ind 
       FROM (
              SELECT clientCorporationID, trim( customText6.value ) as customText6 
              FROM bullhorn1.BH_ClientCorporation m 
              CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.customText6) ), ',') AS customText6
              ) m
       CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.customText6) ), ';') AS customText6
       where (customText6 is not null and convert(nvarchar(max),customText6) <> '')
       )
--select distinct customText6, count(*) from customText6 where customText6 <> '#N/A' group by customText6       

, t as (
select distinct clientCorporationID
       --, customText6
       , case
when customText6 = 'Kimi Abdullah' then 29035
when customText6 = 'ADELE' then 29011
when customText6 = 'Sharon Alderson' then 29051
when customText6 = 'Lisa Armstrong' then 29040
when customText6 = 'CARLA' then 29014
when customText6 = 'Carole Feeny' then 29015
when customText6 = 'CHRISTEL' then 29016
when customText6 = 'CRAIG' then 29017
when customText6 = 'Pat Craven' then 29048
when customText6 = 'DEBRA' then 28960
when customText6 = 'Debra Sharpe' then 28960
when customText6 = 'DOROTHY' then 28975
when customText6 = 'Dorothy Moe' then 28975
when customText6 = 'Elaine Diona' then 28956
when customText6 = 'Stacy Foster' then 29052
when customText6 = 'Gabi Syberg-Olsen' then 28981
when customText6 = 'Craig Hodges' then 29018
when customText6 = 'Stephen Hodges' then 29018
when customText6 = 'HOUSE' then 29028
when customText6 = 'Alesia Jackson' then 29013
when customText6 = 'JANET' then 29029
when customText6 = 'JENNY' then 28966
when customText6 = 'Jenny Gilbert' then 28966
when customText6 = 'JOANNECORE' then 29032
when customText6 = 'KAREN' then 29033
when customText6 = 'Emily Keyes' then 29024
when customText6 = 'KIMBERLEY' then 29034
when customText6 = 'Laura Camozzi' then 28959
when customText6 = 'LILIANA' then 29038
when customText6 = 'LISA' then 29039
when customText6 = 'MAE' then 29042
when customText6 = 'Gassia Maljian' then 29027
when customText6 = 'MANDY' then 28970
when customText6 = 'Mandy Gilbert' then 28970
when customText6 = 'Maria De Los Reyes' then 28978
when customText6 = 'Mike' then 29046
when customText6 = 'Krista Moss' then 29036
when customText6 = 'Luis Oreamuno' then 29041
when customText6 = 'Erin Phillips' then 29025
when customText6 = 'Sandra' then 29049
when customText6 = 'Sharon' then 29050
when customText6 = 'STEPHEN' then 29053
when customText6 = 'Mike Stiavnicky' then 29047
when customText6 = 'TANYA' then 29055
when customText6 = 'Theresa' then 28958
when customText6 = 'Theresa Casarin' then 28958
when customText6 = 'Theresa Perrotta' then 28958
when customText6 = 'TRINA' then 29059
when customText6 = 'Adele Wootton' then 29012
else '' end as 'owner'
from customText6 where customText6 <> '.'
)
--select distinct owner from t where owner is not null
--select distinct customText6, owner from t where owner = 0
SELECT clientCorporationID, STRING_AGG(owner,',' ) WITHIN GROUP (ORDER BY clientCorporationID) owner from t GROUP BY clientCorporationID



-- CUSTOM FIELD > Service Office
select distinct customText13 from bullhorn1.BH_ClientCorporation CC
with
customText13 (clientCorporationID,customText13) as (
       SELECT 
              clientCorporationID
              , trim( replace(replace(replace(customText13.value,'  ',' '),' )',')'),'( ','(') ) as customText13 --, trim( ind.value ) as ind 
       FROM (
              SELECT clientCorporationID, trim( customText13.value ) as customText13 
              FROM bullhorn1.BH_ClientCorporation m 
              CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.customText13) ), ',') AS customText13
              ) m
       CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.customText13) ), ';') AS customText13
       where (customText13 is not null and convert(nvarchar(max),customText13) <> '' and customText13 <> 'Please Select')
       )
--select distinct customText13, count(*) from customText13 where customText13 <> '#N/A' group by customText13
, a as (
       select clientCorporationID
       , case customText13
       when 'Direct Hire' then 1
       when 'perm' then 2
       when 'permanent' then 2
       when 'Publisher' then 3
       when 'SOW' then 4
       when 'Temp' then 5
       when 'temp to perm' then 6
       end as field_value
       from customText13 where customText13 <> '#N/A'
)
select clientCorporationID, 11267 as field_id, STRING_AGG( field_value,',' ) WITHIN GROUP (ORDER BY field_value asc) field_value
from a
group by clientCorporationID




SELECT
         clientCorporationID as additional_id
        , 'add_com_info' as 'additional_type'
        , 1008 as 'form_id'
        , 11265 as 'field_id'
        , case
when customText13 = 'Cincinnati' then 2
when customText13 =  'Amsterdam' then 1
when customText13 =  'Toronto' then 5
when customText13 =  'Ottawa' then 4
when customText13 =  'NS' then 3
    end as 'field_value'
        , 11265 as 'constraint_id'
-- select distinct customText13  -- select count(*)    
from bullhorn1.BH_ClientCorporation CC 
where customText13 is not null and customText13 not in ('','Please Select')
and CC.status <> 'Archive'
