
with
headquarter0 as ( 
        select    cast(cs.Client as varchar(max)) as Client
                , ltrim(Stuff( ' ' + Coalesce(NULLIF(cast(cs.Address1 as varchar(max)) , ''), '')
                                + Coalesce(', ' + NULLIF(cast(cs.Address2 as varchar(max)), ''), '')
                                + Coalesce(', ' + NULLIF(cast(cs.Address2 as varchar(max)), ''), '')
                        , 1, 1, '') ) as 'company_locationAddress'
        from ClientsSites cs
        left join CompanyImportAutomappingTemplate c on cast(c.company_externalId as varchar(max)) = cast(cs.Client as varchar(max)) 
        where convert(varchar,cs.Main) = 'Yes' and convert(varchar,cs.Client) = '1095')
        
, headquarter0a as (
        SELECT cast(a.Client as varchar(max)) as Client
                , comment = STUFF(( SELECT ' OR ' + cast(company_locationAddress as varchar(max)) FROM headquarter0 b WHERE cast(b.Client as varchar(max)) <> '' and cast(Client as varchar(max)) = cast(a.Client as varchar(max)) FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 3, '') 
                            FROM headquarter0 a where convert(varchar,a.Client) = '1095'
                            GROUP BY cast(a.Client as varchar(max))

        )
--select * from headquarter0a
, headquarter1 as ( 
        select    cast(cs.Client as varchar(max)) as Client
                , ltrim(Stuff( ' ' + Coalesce(NULLIF(cast(cs.Address1 as varchar(max)) , ''), '')
                                + Coalesce(', ' + NULLIF(cast(cs.Address2 as varchar(max)), ''), '')
                                + Coalesce(', ' + NULLIF(cast(cs.Address2 as varchar(max)), ''), '')
                        , 1, 1, '') ) as 'company_locationAddress'
        from ClientsSites cs
        left join CompanyImportAutomappingTemplate c on cast(c.company_externalId as varchar(max)) = cast(cs.Client as varchar(max)) 
        where convert(varchar,cs.Main) = 'Yes' and convert(varchar,cs.Client) <> '1095')
--select * from headquarter1
, headquarter(Client,[company_locationAddress]) as (select * from headquarter0a UNION ALL select * from headquarter1 )
--select * from headquarter

, note as (
	select c.company_externalId
	, Stuff( Coalesce('Note: ' + NULLIF(cast(c.company_note as varchar(max)), '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(cast(c.Status as varchar(max)), '') + char(10), '')
                        + Coalesce('LinkedIn: ' + NULLIF(cast(c.LinkedIn as varchar(max)), '') + char(10), '')
                        + Coalesce('Twitter: ' + NULLIF(cast(c.Twitter as varchar(max)), '') + char(10), '')
                        + Coalesce('Facebook: ' + NULLIF(cast(c.Facebook as varchar(max)), '') + char(10), '')
                , 1, 0, '') as note
        from CompanyImportAutomappingTemplate c
        )
-- select * from note

, csnote1 as
        (select cs.Client
	, Stuff(          Coalesce('Name: ' + NULLIF(cast(cs.Name as varchar(max)), '') + char(10), '')
                        + Coalesce('Phone: ' + NULLIF(cast(cs.Phone as varchar(max)), '') + char(10), '')
                        + Coalesce('Fax: ' + NULLIF(cast(cs.Fax as varchar(max)), '') + char(10), '')
                        + Coalesce('Notes: ' + NULLIF(cast(cs.Notes as varchar(max)), '') + char(10), '')
                        + Coalesce('Tags: ' + NULLIF(cast(cs.Tags as varchar(max)), '') + char(10), '')
                , 1, 0, '') as comment
        from ClientsSites cs --where cast(Client as varchar(max)) = '414'
        )
, csnote2 (Client,note) as (SELECT cast(Client as varchar(max)), comment = STUFF(( SELECT char(10) + cast(comment as varchar(max)) FROM csnote1 b WHERE cast(b.Client as varchar(max)) <> '' and cast(Client as varchar(max)) = cast(a.Client as varchar(max)) FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 1, '') FROM csnote1 a GROUP BY cast(a.Client as varchar(max)) )
--select * from csnote2

-- FILES
, doc (Client,Filename) as (
        --SELECT cast(Client as varchar(max)), Files = STUFF(( SELECT DISTINCT ', ' + cast(Filename as varchar(max)) FROM Documents b WHERE cast(b.Client as varchar(max)) <> '' and cast(Client as varchar(max)) = cast(a.Client as varchar(max)) FOR XML PATH('')), 1, 2, '') FROM Documents a GROUP BY cast(a.Client as varchar(max))
        SELECT cast(Client as varchar(max)), Files = STUFF(( SELECT DISTINCT ', ' + cast(Filename as varchar(max)) FROM Documents b WHERE cast(b.Client as varchar(max)) <> '' and cast(Client as varchar(max)) = cast(a.Client as varchar(max)) FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 1, '') FROM Documents a GROUP BY cast(a.Client as varchar(max))
        )
--select * from doc



select
          c.company_externalId as 'company-externalId'
        --, iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn), iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	, c.company_name as 'company-name'
        , ltrim(Stuff( Coalesce(' ' + NULLIF(cast(c.company_locationName as nvarchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationAddress as nvarchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationCity as nvarchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationState as nvarchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationCountry as nvarchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationZipCode as nvarchar(max)), ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
        , ltrim(Stuff( Coalesce(' ' + NULLIF(cast(c.company_locationName as varchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationAddress as varchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationCity as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationState as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationCountry as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.company_locationZipCode as varchar(max)), ''), '')
                , 1, 1, '') ) as 'company-locationName'
	--, c.company_locationCountry as 'company-locationCountry'
	, case
	       	when c.company_locationCountry like 'Abkhazi%' then ''
		when c.company_locationCountry like 'Denmark%' then 'DK'
		when c.company_locationCountry like 'Finland%' then 'FI'
		when c.company_locationCountry like 'Ireland%' then 'IE'
		when c.company_locationCountry like '%UNITED%KINGDOM%' then 'GB'
                end as 'company-locationCountry'
	, c.company_locationState as 'company-locationState'
	, c.company_locationCity as 'company-locationCity'
        , c.company_locationZipCode as 'company-locationZipCode'
	, c.company_nearestTrainStation as 'company-nearestTrainStation'
	, ltrim(convert(varchar(max),h.company_locationAddress)) as 'company-headquarter' --, c.company_headQuarter as 'company-headquarter'
	, c.company_phone as 'company-phone' --,cs.phone
	, c.company_fax as 'company-fax'
	, left(c.company_website,100) as 'company-website' --limitted by 100 characters
	, o.email as 'company-owners' --, c.company_owners
	, doc.Filename as 'company-document'
	--, replace(replace(replace(replace(replace(note.note,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rsquo;','"') as 'company-note'
	, concat(note.note, char(10), csnote2.note) as 'company-note'
	--, Coalesce('Company Overview: ' + NULLIF([dbo].[udf_StripHTML](CC.notes), '') + char(10), '') as 'company-comment'       
-- select * -- select count (*) --3825 -- select distinct cast(c.company_locationCountry as varchar(max)) -- select distinct cast(c.sector as varchar(max))
from CompanyImportAutomappingTemplate c
--left join ClientsSites cs on cast(c.company_externalId as varchar(max)) = cast(cs.Client as varchar(max)) 
left join doc on cast(c.company_externalId as varchar(max)) = cast(doc.client as varchar(max))
left join note on cast(c.company_externalId as varchar(max)) = cast(note.company_externalId as varchar(max))
left join csnote2 on cast(csnote2.Client as varchar(max)) = cast(c.company_externalId as varchar(max))
left join owner o on cast(o.fullname as varchar(max)) =  cast(c.company_owners as varchar(max))
left join headquarter h on cast(h.Client as varchar(max)) = cast(c.company_externalId as varchar(max))
--where cast(c.company_externalid as varchar(max)) = '2'
--where convert(varchar,c.company_externalId) = '143'
--where convert(varchar,c.company_headQuarter) <> ''


--ClientsSites - INJECT TO VINCERE
select --top 1000
          RecordID
        , com.company_name
        , Client as 'company-externalId'
        , Main
        /*, ltrim(Stuff( Coalesce(NULLIF(cast(cs.Address1 as varchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Address2 as varchar(max)), ''), '')
                , 1, 0, '') ) as 'company-locationAddress' */
        , ltrim(Stuff(    Coalesce(' ' + NULLIF(cast(cs.Address1 as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Address2 as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Town as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.County as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Country as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Postcode as varchar(max)), ''), '')
                , 1, 1, '') ) as 'company-locationAddress'
        , ltrim(Stuff( ' ' + Coalesce(NULLIF(cast(c.company_name as varchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Town as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.County as varchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(cs.Country as varchar(max)), ''), '')
                        + Coalesce(', Phone: ' + NULLIF(cast(cs.Phone as varchar(max)), ''), '')
                , 1, 1, '') ) as 'company-locationName'        
        , Town as 'company-locationCity2'
        , County as 'company-locationState2'
        , Postcode as 'company-locationZipCode2'
        , case
		when cs.Country like 'Abkhazi%' then ''
		when cs.Country like 'Banglad%' then 'BD'
		when cs.Country like 'Belgium%' then 'BE'
		when cs.Country like 'Denmark%' then 'DK'
		when cs.Country like 'Finland%' then 'FI'
		when cs.Country like 'Ireland%' then 'IE'
		when cs.Country like 'Netherl%' then 'NL'
		when cs.Country like 'Poland%' then 'PL'
		when cs.Country like '%UNITED%ARAB%' then 'AE'
		when cs.Country like '%UNITED%KINGDOM%' then 'GB'
                end as 'company-locationCountry2'
        , Fax as 'company-fax'
        , ltrim(Stuff( 'Location Note : ' + char(10) + 'Name: ' + Coalesce(NULLIF(cast(c.company_name as varchar(max)) , '') + char(10), '')
                        + Coalesce('Phone: ' + NULLIF(cast(cs.Phone as varchar(max)), '') + char(10), '')
                        + Coalesce('Fax: ' + NULLIF(cast(cs.Fax as varchar(max)), '') + char(10), '')
                        + Coalesce('Notes: ' + NULLIF(cast(cs.Notes as varchar(max)), '') + char(10), '')
                        + Coalesce('Tags: ' + NULLIF(cast(cs.Tags as varchar(max)), '') + char(10), '')
                , 1, 0, '') ) as 'company-note'
-- select distinct convert(varchar(max),Country)
from ClientsSites cs
left join (select company_externalId , company_name from CompanyImportAutomappingTemplate) c on cast(c.company_externalId as varchar(max)) = cast(cs.Client as varchar(max)) 
left join (select company_externalId, company_name from CompanyImportAutomappingTemplate) com on convert(varchar,com.company_externalId) = cast(cs.Client as varchar(max)) 
where convert(varchar,cs.Client) = '186' --and convert(varchar,cs.Main) = 'Yes'


/*
-----------
-- COMMENT
WITH comment (Clients,date,comment) as (
	select 
	  j.Clients
	, j.Date as 'date'
	, Stuff(          Coalesce('Date: ' + NULLIF(convert(varchar(10),j.Date,120), '') + char(10), '')
                        + Coalesce('Subject: ' + NULLIF(cast(j.Subject as varchar(max)), '') + char(10), '')
                        + Coalesce('Body: ' + NULLIF(cast(j.Body as varchar(max)), '') + char(10), '')
                        + Coalesce('Type: ' + NULLIF(cast(j.Type as varchar(max)), '') + char(10), '')
                        + Coalesce('Consultant: ' + NULLIF(cast(Consultant as varchar(max)), '') + char(10), '')
                        + Coalesce('Company Name: ' + NULLIF(cast(c.company_name as varchar(max)), '') + char(10), '')
                        + Coalesce('Contact Name: ' + NULLIF(cast(con.fullname as varchar(max)), '') + char(10), '')
                        + Coalesce('Job Title: ' + NULLIF(cast(con.contact_jobTitle as varchar(max)), '') + char(10), '')
                , 1, 0, '') as comment
        from Journals j
        left join CompanyImportAutomappingTemplate c on cast(c.company_externalid as varchar(max))= cast(j.Clients as varchar(max))
        left join (select contact_externalId, concat(contact_firstName,' ',contact_lastName) as fullname,contact_jobTitle from ContactsImportAutomappingTemplate) con on cast(con.contact_externalId as varchar(max)) = cast(j.Contacts as varchar(max))
        where (cast(j.Clients as varchar(max)) <> '' and cast(j.Clients as varchar(max)) not LIKE '%,%')
              and (cast(j.Date as varchar(max)) LIKE '%/%' or cast(j.Date as varchar(max)) LIKE '')
        )
--select count(*) from comment --11856
--select Clients,date from comment
select top 200 
          Clients as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(datetime, CONVERT(VARCHAR(19),replace(convert(varchar(50),date),'',''),120) , 103) as 'comment_timestamp|insert_timestamp'
        , comment as 'comment_content'
from comment

*/