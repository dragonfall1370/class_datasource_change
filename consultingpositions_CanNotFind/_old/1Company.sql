/*
with note as (
	select CC.clientCorporationID
	, Stuff( Coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Address 2: ' + NULLIF(CC.address2, '') + char(10), '')
                        --+ Coalesce('Fax: ' + NULLIF(CC.fax, '') + char(10), '')
                        + Coalesce('System Date Added: ' + NULLIF(convert(varchar(10),CC.dateAdded,120), '') + char(10), '')
                        + Coalesce('Company Description: ' + NULLIF(ltrim(rtrim([dbo].[udf_StripHTML]( 
                                replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                                        cast(CC.companyDescription as varchar(max))
                                ,'&nbsp;','') ,'&ndash;','') ,'&amp;',''), '&hellip;','') ,'&#39;','') ,'&gt;','') ,'&lt;','') ,'&quot;','') ,'&rsquo;',''), '&ldquo;',''), '&rdquo;','') ,'&reg;','') ,'&euro;','')  ) )), '') + char(10), '')
                        +-- Coalesce('Year Founded: ' + NULLIF(convert(varchar(4),CC.dateFounded,120), '') + char(10), '')
                , 1, 0, '') as note
                -- select  top 10 *
        from bullhorn1.BH_ClientCorporation CC
        left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation) pc on pc.clientCorporationID = CC.clientCorporationID
        --left join tmp_country tc ON CC.customText11 = tc.code
        ) --where CC.customText11 is not null )
-- select top 100 * from bullhorn1.BH_ClientCorporation CC
--select * from note where note like '%&%;%'
--select clientCorporationID, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&lsquo;','"'),'&rsquo;','') as 'company-note' from note where note like '%Company Description%'


-- FILES
, doc (clientCorporationID,ResumeId) as (
        SELECT    clientCorporationID
                , STUFF((SELECT DISTINCT ',' + concat(clientCorporationFileID,fileExtension) from bullhorn1.BH_ClientCorporationFile WHERE clientCorporationID = a.clientCorporationID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS ResumeId 
        FROM (select clientCorporationID from bullhorn1.BH_ClientCorporationFile) as a GROUP BY a.clientCorporationID )
--select CC.NAME, doc.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join doc on CC.clientCorporationID = doc.clientCorporationID where doc.ResumeId is not null
--select directory from bullhorn1.BH_ClientCorporationFile where directory <> ''
--select top 100 * from doc


, dup as (SELECT clientCorporationID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(CC.name)) ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC ) --where name like 'Azurance'

, headquarter as ( select distinct parentClientCorporationID,h.name from bullhorn1.BH_ClientCorporation c
                left join (select clientCorporationID,NAME from bullhorn1.BH_ClientCorporation ) h on c.parentClientCorporationID = h.clientCorporationID
                where parentClientCorporationID is not null and parentClientCorporationID <> '' )
--select clientCorporationID,NAME,parentClientCorporationID from bullhorn1.BH_ClientCorporation where clientCorporationID in (102,153,226,289,656,656,656,774,2056,4936)


select --top 100 
          CC.clientCorporationID as 'company-externalId'
        , iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn), iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	--, headquarter.name as 'company-headquarter'
        , ltrim(Stuff( --Coalesce(NULLIF(CC.address1, ''), CC.name)
                          Coalesce(' ' + NULLIF(CC.address1, ''), '')
                        + Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        + Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 1, '') ) as 'company-locationName'
        , ltrim(Stuff( --Coalesce(NULLIF(CC.address1, ''), CC.name)
                        --+ Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        --+ Coalesce(NULLIF(CC.address2, ''), CC.name)
                          Coalesce(' ' + NULLIF(CC.address1, ''), '')
                        + Coalesce(', ' + NULLIF(CC.address2, ''), '')
                        + Coalesce(', ' + NULLIF(CC.city, ''), '')
                        + Coalesce(', ' + NULLIF(CC.state, ''), '')
                        + Coalesce(', ' + NULLIF(tc.country, ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
	, CC.city as 'company-locationCity'
	, CC.state as 'company-locationState'
	, replace(coalesce(NULLIF(tc.abbreviation, ''), ''),'NULL','') as 'company-locationCountry'
	, CC.zip as 'company-locationZipCode'
	, CC.phone as 'company-phone'
	--, CC.fax as 'company-fax'
	, CC.companyURL as 'company-website' --limitted by 100 characters
	--, CC.ownership as 'company-owners'
	, doc.ResumeId as 'company-document'
	, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'company-note'
	--, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'
-- select count (*) --560 -- select distinct CC.ownership
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join note on CC.clientCorporationID = note.clientCorporationID
left join doc on CC.clientCorporationID = doc.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID
left join headquarter on headquarter.parentClientCorporationID =  CC.clientCorporationID
--where CC.ClientCorporationID in (102,153,226,289,656,774,2056,4936)
--where CC.NAME like '%THEQA%'
*/

with 
  doc0 as ( select at.ParentId, concat(at.id,'_',replace(at.Name,',','') ) as doc, a.name
        -- select count(*) --10
        from Attachment at
        left join Account a on a.id = at.ParentId
        where (at.name like '%doc' or at.name like '%docx' or at.name like '%pdf' or at.name like '%rtf' or at.name like '%xls' or at.name like '%xlsx')
        and a.id is not null
         )
, doc (ParentId, docs) as (SELECT ParentId, STUFF((SELECT ', ' + doc from doc0 WHERE doc0.ParentId <> '' and ParentId = a.ParentId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS docs FROM doc0 as a where a.ParentId <> '' GROUP BY a.ParentId)
--select * from doc

, dup as (SELECT ID,name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(a.name)) ORDER BY a.ID ASC) AS rn FROM Account a) --where name in ('LAgroup','ProConseil','Sustainable','You Improve','Azurance')
--select * from dup where rn > 1

, headquarter as ( select distinct a.ID, h.name 
                   from Account a
                   left join (select ID,NAME from Account ) h on a.ID = h.ID
                   where a.ID is not null and a.ID <> '' )
--select * from headquarter 

, note as (
	select a.ID
	, Stuff( Coalesce('ID: ' + NULLIF(cast(a.ID as varchar(max)), '') + char(10), '')
                        + Coalesce('Name: ' + NULLIF(a.ParentId, '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(a.Source__c, '') + char(10), '')
                        + Coalesce('Account Status: ' + NULLIF(a.Account_status__c, '') + char(10), '')
                        + Coalesce('Priority: ' + NULLIF(a.Priority__c, '') + char(10), '')
                        + Coalesce('Description: ' + NULLIF(a.Description, '') + char(10), '')
                        + Coalesce('Comments_FJ_Fr__c: ' + NULLIF(a.Comments_FJ_Fr__c, '') + char(10), '')
                        + Coalesce('Comments FNJ_Fr: ' + NULLIF(a.Comments_FNJ_Fr__c, '') + char(10), '')
                        + Coalesce('Operational_consulting__c: ' + NULLIF(a.Operational_consulting__c, '') + char(10), '')
                , 1, 0, '') as note
                -- select  top 10 *
        from Account a
        )
--select * from note where note like '%&%;%'

select
          a.id as "company-externalId"
        , 'franck@consultingpositions.net' as "owners" --, a.OwnerID as "owners"
        , iif(a.ID in (select ID from dup where dup.rn > 1),concat(dup.name,' - ',a.BillingCountry), iif(a.NAME = '' or a.name is null,'No CompanyName',a.NAME)) as 'company-name'   --NOTE: CHANGE "Z_Punkt" TO "Z_Punkt Germany"
        , a.Phone as "Switchboard"
        , a.Fax as "Fax"
        , a.website as "Website"
        --, a.Source__c
        , h.name as 'company-headquarter'        
        --, a.BillingStreet as "street"
        , a.BillingCity as "locationCity"
        --, ltrim(Stuff(    Coalesce(' ' + NULLIF(a.BillingStreet, ''), '')
        --                + Coalesce(', ' + NULLIF(a.BillingCity, ''), '')
        --        , 1, 1, '') ) as 'company-locationCity'
        , a.BillingState as "locationState"
        , a.BillingPostalCode as "locationZipCode"
        --, a.BillingCountry as "locationCountry"
        , case
		when a.BillingCountry like '59491%' then ''
		when a.BillingCountry like '75248%' then ''
		when a.BillingCountry like 'Africa%' then 'CF ZA'
		when a.BillingCountry like 'Austral%' then 'AU'
		when a.BillingCountry like 'Austria%' then 'AT'
		when a.BillingCountry like 'Belgium%' then 'BE'
		when a.BillingCountry like 'Cambodi%' then 'KH'
		when a.BillingCountry like 'Canada%' then 'CA'
		when a.BillingCountry like 'Denmark%' then 'DK'
		when a.BillingCountry like 'Finland%' then 'FI'
		when a.BillingCountry like 'France%' then 'FR'
		when a.BillingCountry like 'FR%' then 'FR'
		when a.BillingCountry like 'Germany%' then 'DE'
		when a.BillingCountry like 'Gremany%' then 'DE'
		when a.BillingCountry like 'Holland%' then 'NL'
		when a.BillingCountry like 'Hong%' then 'HK'
		when a.BillingCountry like 'India%' then 'IN'
		when a.BillingCountry like 'Indones%' then 'ID'
		when a.BillingCountry like 'Ireland%' then 'IE'
		when a.BillingCountry like 'Israel%' then 'IL'
		when a.BillingCountry like 'Italy%' then 'IT'
		when a.BillingCountry like 'Luxembo%' then 'LU'
		when a.BillingCountry like 'Malaysi%' then 'MY'
		when a.BillingCountry like 'Netherl%' then 'NL'
		when a.BillingCountry like 'Norway%' then 'NO'
		when a.BillingCountry like 'Poland%' then 'PL'
		when a.BillingCountry like 'Singapo%' then 'SG'
		when a.BillingCountry like 'State%' then 'QA'
		when a.BillingCountry like 'SUA%' then ''
		when a.BillingCountry like 'Sweden%' then 'SE'
		when a.BillingCountry like 'Switzer%' then 'CH'
		when a.BillingCountry like 'Thailan%' then 'TH'
		when a.BillingCountry like 'UK%' then 'GB'
		when a.BillingCountry like 'United%Kingdom' then 'GB'
		when a.BillingCountry like 'USA%' then 'US'
		when a.BillingCountry like 'US%' then 'US'
		when a.BillingCountry like 'Vietnam%' then 'VN'
		when a.BillingCountry like '%UNITED%ARAB%' then 'AE'
		when a.BillingCountry like '%UAE%' then 'AE'
		when a.BillingCountry like '%U.A.E%' then 'AE'
		when a.BillingCountry like '%UNITED%KINGDOM%' then 'GB'
		when a.BillingCountry like '%UNITED%STATES%' then 'US'
        else '' end as "locationCountry"
        
        , ltrim(Stuff(    Coalesce(' ' + NULLIF(a.BillingStreet, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingCity, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingState, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingCountry, ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
        , ltrim(Stuff(    Coalesce(', ' + NULLIF(a.BillingCity, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingState, ''), '')
                        + Coalesce(', ' + NULLIF(a.BillingCountry, ''), '')
                , 1, 1, '') ) as 'company-locationName'
        , n.note as 'company-note'
        , doc.docs as 'company-document'
-- select distinct OwnerID --BillingCountry -- select *
from Account a
left join dup on dup.ID = a.ID
left join headquarter h on h.ID = a.ID
left join note n on n.ID = a.ID
left join doc on doc.ParentId = a.id
where a.name like '%Brooks%'
--where a.name in ('LAgroup','ProConseil','Sustainable','You Improve','Azurance')