--select distinct country from dbo.addresses where addresses.lastname is not null
--select distinct c.ismalegender from dbo.contacts c 
--select distinct c.type from dbo.contacts c 
-- select * from dbo.contacts c where c.type in ('Client','Contractor')

-- ALTER DATABASE [energize2] SET COMPATIBILITY_LEVEL = 130
with
  mail1 (ID,email) as (select contactid, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ltrim(rtrim(email)),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from dbo.contacts)
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--select * from ed


, note as (
	select c.contactid
	, Stuff( 
	          Coalesce('ID: ' + NULLIF(cast(c.contactid as varchar(max)), '') + char(10), '')
	      + Coalesce('Title: ' + NULLIF(cast(c.title as varchar(max)), '') + char(10), '')
	      + Coalesce('Gender: ' + NULLIF(case when c.ismalegender = 1 then 'Male' when c.ismalegender = 0 then 'Female' else '' end, '') + char(10), '')
	      + Coalesce('Last User: ' + NULLIF(cast(u.username as varchar(max)), '') + char(10), '')
	      + Coalesce('Home City: ' + NULLIF(cast(c.homecity as varchar(max)), '') + char(10), '')
	      + Coalesce('Calling Plan: ' + NULLIF(cast(c.datelastcalled as varchar(max)), '') + char(10), '')
	      + Coalesce('Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
	      + Coalesce('Location: ' + NULLIF(cast(c.location as varchar(max)), '') + char(10), '')
	      + Coalesce('Sub Location: ' + NULLIF(cast(c.sublocation as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Last User: ' + NULLIF(cast(iif(u.username is null,c.lastuser, concat(c.lastuser,' - ',u.username) ) as varchar(max)), '') + char(10), '')
	      + Coalesce('Source: ' + NULLIF(cast(c.ContactSource as varchar(max)), '') + char(10), '')
	      + Coalesce('Sector: ' + NULLIF(cast(c.sector as varchar(max)), '') + char(10), '')
	      + Coalesce('About: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
              , 1, 0, '') as note
from dbo.contacts c
left join dbo.users u on u.userid = c.lastuser)
--select top 100 * from note where c.contactid = '828087-8837-17213'
-- select top 10 * from dbo.contacts where contactid = '828087-8837-17213'


-- FILES
, doc (id,doc) as (
        SELECT id
                     , STUFF((SELECT DISTINCT ',' + filename from attachments WHERE id = a.id /*and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS doc 
        FROM (select id from attachments) as a GROUP BY a.id )
--select top 100 * from doc


--with 
, com0 as ( select con.contactid, con.companyid, con.company from dbo.contacts con where con.companyid not in (select companyid from dbo.companies) )
, com1 as (select distinct company from com0 where company is not null and company <> ''  )
--select * from com1
, com2 as (select com.companyid, com.companyname  from com1 left join companies com on com.companyname = com1.company where com.companyid is not null )
--select * from com2
, company as (
       select
                distinct con.contactid
              , con.company
              , case 
                     when con.companyid in (select companyid from dbo.companies) then con.companyid
                     when com2.companyid is not null then com2.companyid
                     when con.companyid in (null,'') then 'default'
                     else 'default' end as company_id
              --, dupcompany.companyid) as '#contact-companyId', con.company
       from dbo.contacts con
       left join com2 on com2.companyname = con.company
       where con.type in ('Client','Contact','Contractor') )
--select * from company


select 
         distinct replace(c.contactid,'Z','') as 'contact-externalId'
--       , iif(c.companyid in (null,''), 'default', c.companyid) as '#contact-companyId'
--       , iif(c.companyid in (select companyid from dbo.companies), c.companyid, 'default') as 'contact-companyId'
       , company.company_id as 'contact-companyId' --, c.company
       , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'contact-Lastname'
       , c.jobtitle as 'contact-jobTitle'
       , c.department as 'Department'
       , c.directtel as 'contact-phone'
       , c.mobiletel  as 'mobile_phone'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(c.worktel, ''), '') + Coalesce(', ' + NULLIF(c.hometel, ''), ''), 1, 1, '') )  as 'home_phone'
       , ltrim(Stuff( Coalesce(' ' + NULLIF(a.address1, ''), '') + Coalesce(', ' + NULLIF(a.address2, ''), '') + Coalesce(', ' + NULLIF(a.address3, ''), '') + Coalesce(', ' + NULLIF(a.city, ''), '') + Coalesce(', ' + NULLIF(a.county, ''), '') + Coalesce(', ' + NULLIF(a.postcode, ''), '') + Coalesce(', ' + NULLIF(a.country, ''), '') , 1, 1, '') ) as 'contact-locationAddress'      
       , a.city as 'contact-locationCity'
       , a.county as 'contact-locationState'
       , a.postcode as 'contact-locationZipCode'
       , case
		when a.country like 'Africa%' then 'ZA'
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
		when a.country like 'UKnited%' then 'GB'
		when a.country like 'UKraine%' then 'UA'
		when a.country like 'UK%' then 'GB'
		when a.country like 'USA%' then 'US'
		when a.country like 'Wales%' then 'GB'
		when a.country like 'Western%' then  'AU'
		when a.country like 'West%' then 'GB'
		when a.country like 'Yorkshi%' then 'GB'
              else '' end as 'contact-locationCountry'       
       , case when c.website like '%linkedin%' then c.website else '' end as 'contact-linkedIn' --, c.linkedinconnected 
       , c.alsoknownas as 'PreferredName'

       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email' --, c.email  as 'contact-email'      
--       , c.email2 as 'PersonalEmail'
--       , a.email as 'Details-Address-Email: Personal Email'
       , ltrim(Stuff( Coalesce( ' ' + NULLIF(c.email2, ''), '') + Coalesce(', ' + NULLIF( a.email, ''), '') , 1, 1, '') ) as 'PersonalEmail'
       
       , u.email as 'contact-owners' --, c.username
       , c.longitude as 'Longitude'
       , c.latitude as 'Latitude'
       , n.note as 'contact-note'
       , d.doc as 'contact-document'       
-- select count(*) --5845 --  select distinct c.type, count(*) -- select *
from dbo.contacts c --group by c.type
left join dbo.addresses a on a.contactid = c.companyid
left join dbo.users u on u.userid = c.userid
left join note n on n.contactid = c.contactid
left join doc d on d.id = c.contactid
left join ed ON ed.id = c.contactid
left join company on company.contactid = c.contactid
where c.type in ('Client','Contact','Contractor')
--where a.email is not null
--where c.firstname = 'Ian' and c.lastname like 'Bazz%'
--where userid = '773931-5517-1697'


/*
-- LOG
select 
         distinct replace(c.contactid,'Z','') as 'contact-externalId'      
       , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'contact-Lastname'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'contact' as 'type'       
       , l.logdate as 'insert_timestamp'
	, Stuff( 
	          Coalesce('Name: ' + NULLIF(cast(u.username as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Subject: ' + NULLIF(cast(l.subject as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Log Item Text: ' + char(10) + NULLIF(cast(ld.text as varchar(max)), '') + char(10), '')
              , 1, 0, '')  as 'content'
       --, ld.*
-- select count(*)       
from dbo.contacts c 
left join dbo.logitems l on l.itemid =  c.contactid
left join dbo.logdata ld on ld.logdataid = l.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where c.type in ('Client','Contact','Contractor')
and c.contactid = '828087-8837-17213'
--or l.subject like '%DM Note - Spoke to Ian%'

*/
