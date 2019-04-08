
with 
note as (
	select c.companyid
	, Stuff( 
	        Coalesce('ID: ' + NULLIF(cast(c.companyid as varchar(max)), '') + char(10), '')
	      + Coalesce('Owner: ' + NULLIF(cast( iif(u1.email is null, c.owner, '') as varchar(max)), '') + char(10), '')
	      + Coalesce('Date Agreed: ' + NULLIF(cast(p.OfferAgreedDate as varchar(max)), '') + char(10), '')
	      + Coalesce('Review Date: ' + NULLIF(cast(p.reviewdate as varchar(max)), '') + char(10), '')
	      + Coalesce('Fee: ' + NULLIF(cast(p.fee as varchar(max)), '') + char(10), '')
	      + Coalesce('Payment Terms: ' + NULLIF(cast(p.paymentterms as varchar(max)), '') + char(10), '')
	      + Coalesce('Rebate Period: ' + NULLIF(cast(p.rebateperiod as varchar(max)), '') + char(10), '')
	      + Coalesce('ReferenceNo: ' + NULLIF(cast(c.CompanyRegNo as varchar(max)), '') + char(10), '')
	      + Coalesce('Notes: ' + NULLIF(cast(p.notes as varchar(max)), '') + char(10), '')
	      + Coalesce('Status: ' + NULLIF(cast(c.companystatus as varchar(max)), '') + char(10), '')
	      + Coalesce('Postcode: ' + NULLIF(cast(c.postcode as varchar(max)), '') + char(10), '')
	      + Coalesce('Next Call: ' + NULLIF(cast(c.DateNextCall as varchar(max)), '') + char(10), '')
             + Coalesce('EMail: ' + NULLIF(cast(c.email as varchar(max)), '') + char(10), '')
             + Coalesce('Flag (Checkbox): ' + NULLIF(cast(c.RedFlag as varchar(max)), '') + char(10), '')
             + Coalesce('Stars: ' + NULLIF(cast(c.StarRating as varchar(max)), '') + char(10), '')
             + Coalesce('Parent Co: ' + NULLIF(cast(c.ParentCompany as varchar(max)), '') + char(10), '')
             + Coalesce('Terms Agreed (checkbox): ' + NULLIF(cast(c.TermsAgreed as varchar(max)), '') + char(10), '')
             + Coalesce('Location: ' + NULLIF(cast(c.location as varchar(max)), '') + char(10), '')
             + Coalesce('Sub Location: ' + NULLIF(cast(c.sublocation as varchar(max)), '') + char(10), '')
             + Coalesce('Accounts: ' + NULLIF(cast(c.AccountsId as varchar(max)), '') + char(10), '') --AccountsContact
             + Coalesce('Credit Limit: ' + NULLIF(cast(c.CreditLimit as varchar(max)), '') + char(10), '')
             + Coalesce('Current Invoice Total: ' + NULLIF(cast(c.CurrentInvoiceTotal as varchar(max)), '') + char(10), '')
             + Coalesce('Prepaid Days Credit: ' + NULLIF(cast(c.PrePaidDays as varchar(max)), '') + char(10), '')
             --+ Coalesce('Invoicing Type: ' + NULLIF(cast(c.email as varchar(max)), '') + char(10), '')
             --+ Coalesce('Invoicing Method: ' + NULLIF(cast(c.email as varchar(max)), '') + char(10), '')
             + Coalesce('VAT Number: ' + NULLIF(cast(c.VATNo as varchar(max)), '') + char(10), '')
             + Coalesce('Timezone: ' + NULLIF(cast(c.TimeZoneName as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Last User: ' + NULLIF(cast(iif(u.username is null,c.lastuser, concat(c.lastuser,' - ',u.username) ) as varchar(max)), '') + char(10), '')
	      + Coalesce('Last User: ' + NULLIF(cast(u.username as varchar(max)), '') + char(10), '')
             + Coalesce('Hotlist (checkbox): ' + NULLIF(cast(c.HotList as varchar(max)), '') + char(10), '')
             + Coalesce('About: ' + NULLIF(cast(c.description as varchar(max)), '') + char(10), '')
             --+ Coalesce('CUSTOM DATA TAB: ' + NULLIF(cast(c. as varchar(max)), '') + char(10), '')           
             
	      --+ Coalesce('Skills: ' + NULLIF(cast(c.CompanySkills as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Source: ' + NULLIF(cast(c.companysource as varchar(max)), '') + char(10), '')
              , 1, 0, '') as note
-- select /*top 100*/ * -- select AccountsId, AccountsContact
from dbo.companies c
--left join ClientTerms ct on ct.
left join Placements p on p.clientid = c.companyid
left join users u1 on u1.username = c.owner
left join dbo.users u on u.userid = c.lastuser)
-- select * from dbo.clientterms
--select top 100 * from note

-- FILES
, doc (id,doc) as (
        SELECT id
                     , STUFF((SELECT DISTINCT ',' + filename from attachments WHERE id = a.id and filename is not null /*and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS doc 
        FROM (select id from attachments) as a GROUP BY a.id )
--select top 100 * from doc
--SELECT id, STRING_AGG( cast(filename as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY id) att from attachments where id is not null and filename is not null GROUP BY id

, dup as (SELECT companyid,ltrim(rtrim(companyname)) as name,ROW_NUMBER() OVER(PARTITION BY ltrim(rtrim(companyname)) ORDER BY companyid ASC) AS rn FROM dbo.companies )


select
         c.companyid as 'company-externalId'
       --, iif(c.companyname in (null,''), 'No CompanyName', c.companyname) as 'company-name'
       , iif(dup.rn > 1,concat(dup.name,' ',dup.rn), iif(dup.name in (null,''),'No CompanyName',dup.name)) as 'company-name'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(a.address1, ''), '') + Coalesce(', ' + NULLIF(a.address2, ''), '') + Coalesce(', ' + NULLIF(a.address3, ''), '') + Coalesce(', ' + NULLIF(a.city, ''), '') + Coalesce(', ' + NULLIF(a.county, ''), '') + Coalesce(', ' + NULLIF(a.postcode, ''), '') + Coalesce(', ' + NULLIF(a.country, ''), '') , 1, 1, '') ) as 'company-locationAddress'      
       , a.city as 'company-locationCity'
       , a.county as 'company-locationState'
       , a.postcode as 'company-locationZipCode'
       , case
		when a.country like 'Africa%' then 'ZA'
		when a.country like '%Romania%' then 'RO'
		when a.country like '%Gibraltar%' then 'GB'
		when a.country like 'Austral%' then 'AU'
		when a.country like 'Austria%' then 'AT'
		when a.country like 'Begium%' then 'BE'
		when a.country like 'Beglium%' then 'BE'
		when a.country like 'Belgie%' then 'BE'
		when a.country like 'België%' then 'BE'
		when a.country like 'Belgiqu%' then 'BE'
		when a.country like 'Belgium%' then 'BE'
		when a.country like 'Berkshi%' then 'GB'
		when a.country like 'Birming%' then 'GB'
		when a.country like 'Brussel%' then 'BE'
		when a.country like 'Bulgari%' then 'BG'
		when a.country like 'Canada%' then 'CA'
		when a.country like 'CA%' then 'CA'
		when a.country like 'Denmark%' then 'DK'
		when a.country like 'Deutsch%' then 'DE'
		when a.country like 'Dubai%' then 'AE'
		when a.country like 'England%' then 'GB'
		when a.country like 'Essex%' then 'GB'
		when a.country like 'Finland%' then 'FI'
		when a.country like 'France%' then 'FR'
		when a.country like 'Frankfu%' then 'DE'
		when a.country like 'Germany%' then 'DE'
		when a.country like 'Gibralt%' then 'ES'
		when a.country like 'Greece%' then 'GR'
		when a.country like '%Hong K%' then 'HK'
		when a.country like 'Hamburg%' then 'DE'
		when a.country like 'Hampshi%' then 'GB'
		when a.country like 'Hungary%' then 'HU'
		when a.country like 'Ireland%' then 'IE'
		when a.country like 'Italy%' then 'IT'
		when a.country like 'Kiel%' then 'DE'
		when a.country like 'Lancs%' then 'GB'
		when a.country like 'London%' then 'GB'
		when a.country like 'Lübeck%' then 'DE'
		when a.country like 'Luxembo%' then 'LU'
		when a.country like 'Malta%' then 'MT'
		when a.country like 'Münche%' then 'DE'
		when a.country like 'Munich%' then 'DE'
		when a.country like 'Netherl%' then 'NL'
		when a.country like 'Norway%' then 'NO'
		when a.country like 'Poland%' then 'PL'
		when a.country like 'russia%' then 'RU'
		when a.country like 'Scotlan%' then 'GB'
		when a.country like 'Singapo%' then 'SG'
		when a.country like 'Spain%' then 'ES'
		when a.country like 'Sweden%' then 'SE'
		when a.country like 'Switzer%' then 'CH'
		when a.country like 'UAE%' then 'AE'
		when a.country like 'United Kingdom%' then 'GB'
		when a.country like 'UKnited%' then 'GB'
		when a.country like 'UKraine%' then 'UA'
		when a.country like 'UK%' then 'GB'
		when a.country like 'USA%' then 'US'
		when a.country like 'Wales%' then 'GB'
		when a.country like 'Western%' then  'AU'
		when a.country like 'West%' then 'GB'
		when a.country like 'Yorkshi%' then 'GB'
		when a.country like '%UNITED%ARAB%' then 'AE'
		when a.country like '%UAE%' then 'AE'
		when a.country like '%U.A.E%' then 'AE'
		when a.country like '%UNITED%KINGDOM%' then 'GB'
		when a.country like '%UNITED%STATES%' then 'US'
		when a.country like '%US%' then 'US'		
              else '' end as 'company-locationCountry' --, a.country

       , a.telno as 'Location > Phone number' -->>
       , a.email as 'Location > Note'  -->>
       , c.longitude as 'Longitude'  -->>
       , c.latitude as 'Latitude'  -->>
       
       , u.email as 'company-owners' --, c.owner 
       , c.telno as 'company-phone'
       , c.website as 'company-website'
       , c.sector as 'company-industry' -->>
       , c.headcount as 'company-headcount' -->>
       , c.companyregno as 'Company Number' --EMPTY ***
       , n.note as 'company-note'
       , d.doc as 'company-document'
-- select count(*) --2377 -- select c.companyregno -- select top 100 * -- select headcount
from companies c
left join dup on dup.companyid = c.companyid
left join addresses a on a.contactid = c.companyid
left join users u on u.username = c.owner
left join note n on n.companyid = c.companyid
left join doc d on d.id = c.companyid
--where c.companyname = '360i'



/*
-- LOG
select --top 10
         c.companyid as 'externalId'
       , c.companyname as 'companyname'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'company' as 'type'       
       , l.logdate as 'insert_timestamp'
	, Stuff( 
	        Coalesce('Name: ' + NULLIF(cast(u.username as nvarchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Subject: ' + NULLIF(cast(l.subject as nvarchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Log Item Text: ' + char(10) + NULLIF(cast(ld.text as nvarchar(max)), '') + char(10), '')
              , 1, 0, '')  as 'content'
       --, l.*, ld.*
-- select count(*)       
from dbo.companies c 
left join logitems l on l.itemid =  c.companyid
left join LogDataIndex ldi on ldi.logitemid = l.logitemid
left join logdata ld on ld.logdataid = ldi.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where l.logdate is not null 

where ld.logdataid is not null and l.itemid is not null
where c.companyid = '666487-9663-18190' or l.companyid = '666487-9663-18190'
--or l.subject like '%DM Note - Spoke to Ian%'
*/

