--select distinct country from dbo.addresses where addresses.lastname is not null
--select distinct c.ismalegender from dbo.contacts c 
--select distinct c.type from dbo.contacts c 
-- select * from dbo.contacts c where c.type in ('Client','Contractor')


-- ALTER DATABASE [dano] SET COMPATIBILITY_LEVEL = 130
with 
  com0 as ( select con.contactid, con.companyid, con.company from dbo.contacts con where con.companyid not in (select companyid from dbo.companies) )
, com1 as ( select distinct(ltrim(rtrim(company))) as company from com0 where company is not null and company <> ''  )
--select * from com1
, com2 as ( select com.companyname, max(com.companyid) as companyid from com1 left join companies com on com.companyname = com1.company where com.companyid is not null group by com.companyname )
--select * from com2 where companyname in (select companyname from com2 group by companyname having count(*) > 1)

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
--select contactid from company group by contactid having count(*) > 1
--select * from company where contactid in (select contactid from company group by contactid having count(*) > 1)



-- EMAIL
, mail1 (ID,email) as (select contactid, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ltrim(rtrim(email)),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' ') as email from dbo.contacts)
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
-- Details
	        Coalesce('ID: ' + NULLIF(cast(c.contactid as varchar(max)), '') + char(10), '')
	      + Coalesce('Title: ' + NULLIF(cast(c.title as varchar(max)), '') + char(10), '')
	      + Coalesce('Gender: ' + NULLIF(case when c.ismalegender = 1 then 'Male' when c.ismalegender = 0 then 'Female' else '' end, '') + char(10), '')
	      --+ Coalesce('Skype: ' + NULLIF(cast(c. as varchar(max)), '') + char(10), '')
	      + Coalesce('Contact Type: ' + NULLIF(cast(c.Type as varchar(max)), '') + char(10), '')
	      + Coalesce('Relocation: ' + NULLIF(cast(c.relocationstatus as varchar(max)), '') + char(10), '')
-- Details > More
	      + Coalesce('URL: ' + NULLIF(cast(c.website as varchar(max)), '') + char(10), '')
	      + Coalesce('Fax: ' + NULLIF(cast(c.fax as varchar(max)), '') + char(10), '')
	      + Coalesce('Last User: ' + NULLIF(cast(u.username as varchar(max)), '') + char(10), '')
	      + Coalesce('Home City: ' + NULLIF(cast(c.homecity as varchar(max)), '') + char(10), '')
	      + Coalesce('Reg Date (date): ' + NULLIF(cast(c.regdate as varchar(max)), '') + char(10), '')
	      + Coalesce('Updated (date): ' + NULLIF(cast(c.LastUpdate as varchar(max)), '') + char(10), '')
	      + Coalesce('Reviewed (date): ' + NULLIF(cast(c.LastReviewDate as varchar(max)), '') + char(10), '')
	      + Coalesce('Emailed (date): ' + NULLIF(cast(c.dateemailed as varchar(max)), '') + char(10), '')
	      + Coalesce('Last Call (date): ' + NULLIF(cast(c.DateLastCalled as varchar(max)), '') + char(10), '')
	      + Coalesce('Email 2: ' + NULLIF(cast(c.Email2 as varchar(max)), '') + char(10), '')
	      + Coalesce('Email 3: ' + NULLIF(cast(c.Email3 as varchar(max)), '') + char(10), '')
	      + Coalesce('Years of Experiencing: ' + NULLIF(cast(c.experienceinyears as varchar(max)), '') + char(10), '')
	      + Coalesce('Date Experience Entered: ' + NULLIF(cast(c.dateexperienceset as varchar(max)), '') + char(10), '')
	      + Coalesce('Start year in Industry / Sector: ' + NULLIF(cast(c.StartYear as varchar(max)), '') + char(10), '')
	      + Coalesce('Date started at current company: ' + NULLIF(cast(c.CompanyStartDate as varchar(max)), '') + char(10), '')
--Details > Calling Plan
	      + Coalesce('Date Last Called: ' + NULLIF(cast(c.datelastcalled as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Select how often you want to call this contact (radio button): ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
--Details
	      + Coalesce('Alt Cont.: ' + NULLIF(cast(c.AltContact as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Availability: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
	      + Coalesce('Time Zone: ' + NULLIF(cast(c.timezonename as varchar(max)), '') + char(10), '')
	      + Coalesce('Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
	      + Coalesce('Discipline: ' + NULLIF(cast(c.discipline as varchar(max)), '') + char(10), '')
	      + Coalesce('Location: ' + NULLIF(cast(c.location as varchar(max)), '') + char(10), '')
	      + Coalesce('Sub Loc: ' + NULLIF(cast(c.sublocation as varchar(max)), '') + char(10), '')
	      + Coalesce('Source: ' + NULLIF(cast(c.source as varchar(max)), '') + char(10), '')
--Details > Qualify
	      + Coalesce('Name: ' + NULLIF(cast(mm.username as varchar(max)), '') + char(10), '')
	      + Coalesce('Company: ' + NULLIF(cast(c.company as varchar(max)), '') + char(10), '')
	      + Coalesce('Job Title: ' + NULLIF(cast(c.jobtitle as varchar(max)), '') + char(10), '')
	      + Coalesce('Coverage: ' + NULLIF(cast(mm.coverage as varchar(max)), '') + char(10), '')
	      + Coalesce('Preferred Locations: ' + NULLIF(cast(mm.locations as varchar(max)), '') + char(10), '')	      
	      + Coalesce('Reporting To: ' + NULLIF(cast(mm.reportingto as varchar(max)), '') + char(10), '')	      
	      + Coalesce('Hire Authority: ' + NULLIF(cast(mm.hireauthority as varchar(max)), '') + char(10), '')	      
	      + Coalesce('Key Buying Condition: ' + NULLIF(cast(c.importkey as varchar(max)), '') + char(10), '')
	      + Coalesce('Assessed: ' + NULLIF(cast(mm.assessed as varchar(max)), '') + char(10), '')
	      + Coalesce('Exam Status: ' + NULLIF(cast(mm.examstatus as varchar(max)), '') + char(10), '')
	      + Coalesce('Salary / Rate: ' + NULLIF(cast(mm.salary as varchar(max)), '') + char(10), '')
	      + Coalesce('Contract: ' + NULLIF(cast(mm.contract as varchar(max)), '') + char(10), '')
	      + Coalesce('Last Update: ' + NULLIF(cast(mm.lastupdate as varchar(max)), '') + char(10), '')
--Details > Terms
	      + Coalesce('Client Term > Date Agreed: ' + NULLIF(cast(ct.dateagreed as varchar(max)), '') + char(10), '')
	      + Coalesce('Client Term > Review Date: ' + NULLIF(cast(ct.reviewdate as varchar(max)), '') + char(10), '')
	      + Coalesce('Client Term > Fee: ' + NULLIF(cast(ct.fee as varchar(max)), '') + char(10), '')
	      + Coalesce('Client Term > Payment Terms: ' + NULLIF(cast(ct.paymentterms as varchar(max)), '') + char(10), '')
	      + Coalesce('Client Term > Rebate Period: ' + NULLIF(cast(ct.rebateperiod as varchar(max)), '') + char(10), '')
	      + Coalesce('Client Term > Reference No: ' + NULLIF(cast(ct.referenceno as varchar(max)), '') + char(10), '')
	      + Coalesce('Client Term > Notes: ' + NULLIF(cast(ct.notes as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Connected (checkbox): ' + NULLIF(cast(ct. as varchar(max)), '') + char(10), '')
	      + Coalesce('Terms (checkbox): ' + NULLIF(case when c.AgreedTerms = 1 then 'Yes' when c.AgreedTerms = 0 then 'No' else '' end, '') + char(10), '')
	      + Coalesce('Embargo (checkbox): ' + NULLIF(case when c.Embargoed = 1 then 'Yes' when c.Embargoed = 0 then 'No' else '' end, '') + char(10), '')
	      + Coalesce('Hotlist (checkbox): ' + NULLIF(case when c.hotlist = 1 then 'Yes' when c.hotlist = 0 then 'No' else '' end, '') + char(10), '')
	      + Coalesce('Can Email (checkbox): ' + NULLIF(case when c.AgreedToEmail = 1 then 'Yes' when c.AgreedToEmail = 0 then 'No' else '' end, '') + char(10), '')
	      + Coalesce('Email (checkbox): ' + NULLIF(case when c.CanText = 1 then 'Yes' when c.CanText = 0 then 'No' else '' end, '') + char(10), '')
	      --+ Coalesce('Add Picture ?: ' + NULLIF(cast(c. as varchar(max)), '') + char(10), '')
	      + Coalesce('Company Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
	      + Coalesce('ABOUT TAB: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
	      /*+ Coalesce('Calling Plan: ' + NULLIF(cast(c.datelastcalled as varchar(max)), '') + char(10), '')
	      + Coalesce('Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
	      + Coalesce('Location: ' + NULLIF(cast(c.location as varchar(max)), '') + char(10), '')
	      + Coalesce('Sub Location: ' + NULLIF(cast(c.sublocation as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Last User: ' + NULLIF(cast(iif(u.username is null,c.lastuser, concat(c.lastuser,' - ',u.username) ) as varchar(max)), '') + char(10), '')
	      + Coalesce('Source: ' + NULLIF(cast(c.ContactSource as varchar(max)), '') + char(10), '')
	      + Coalesce('Sector: ' + NULLIF(cast(c.sector as varchar(max)), '') + char(10), '')
	      + Coalesce('About: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')*/
              , 1, 0, '') as note
from dbo.contacts c
left join dbo.users u on u.userid = c.lastuser
left join ClientTerms ct on ct.contactid = c.contactid
left join MarketMap mm on mm.contactid = c.contactid
)

--select top 100 * from note where c.contactid = '828087-8837-17213'
-- select top 10 * from dbo.contacts where contactid = '828087-8837-17213'


-- FILES
, doc (id,doc) as (
        SELECT id
                     , STUFF((SELECT DISTINCT ',' + filename from attachments WHERE id = a.id /*and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS doc 
        FROM (select id from attachments) as a GROUP BY a.id )
--select top 100 * from doc


select 
         distinct replace(c.contactid,'Z','') as 'contact-externalId'
--       , iif(c.companyid in (null,''), 'default', c.companyid) as '#contact-companyId'
--       , iif(c.companyid in (select companyid from dbo.companies), c.companyid, 'default') as 'contact-companyId'
       , company.company_id as 'contact-companyId' --, c.company
       , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'contact-firstName'
       , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'contact-Lastname'

       , c.directtel as 'contact-phone'
       , c.mobiletel  as 'mobile_phone' -->>
       , ltrim(Stuff( Coalesce(' ' + NULLIF(c.worktel, ''), '') + Coalesce(', ' + NULLIF(c.hometel, ''), ''), 1, 1, '') )  as 'home_phone' -->>

       , c.jobtitle as 'contact-jobTitle'
       , case when c.website like '%linkedin%' then c.website else '' end as 'contact-linkedIn' --, c.linkedinconnected
       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email' --, c.email  as 'contact-email'      
--       , c.email2 as 'PersonalEmail'
--       , a.email as 'Details-Address-Email: Personal Email'
       , n.note as 'contact-note'
       , d.doc as 'contact-document'        
       , u.email as 'contact-owners' --, c.username
              
       , ltrim(Stuff( Coalesce(' ' + NULLIF(a.address1, ''), '') + Coalesce(', ' + NULLIF(a.address2, ''), '') + Coalesce(', ' + NULLIF(a.address3, ''), '') + Coalesce(', ' + NULLIF(a.city, ''), '') + Coalesce(', ' + NULLIF(a.county, ''), '') + Coalesce(', ' + NULLIF(a.postcode, ''), '') + Coalesce(', ' + NULLIF(a.country, ''), '') , 1, 1, '') ) as 'contact-locationAddress'      
       , a.city as 'contact-locationCity'
       , a.county as 'contact-locationState'
       , a.postcode as 'contact-locationZipCode'
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
              else '' end as 'contact-locationCountry' --, a.country  

       , c.alsoknownas as 'PreferredName'
       , ltrim(Stuff( Coalesce( ' ' + NULLIF(c.email2, ''), '') + Coalesce(', ' + NULLIF( a.email, ''), '') , 1, 1, '') ) as 'PersonalEmail'
       , c.department as 'Department'
       , c.longitude as 'Longitude'
       , c.latitude as 'Latitude'

--select count(*) --5037 --  select distinct c.type, count(*) -- select * -- select distinct c.country -- select contactid
from dbo.contacts c --where c.type in ('Client','Contact','Contractor') --group by c.type
left join company on company.contactid = c.contactid
left join dbo.addresses a on a.contactid = c.contactid
left join dbo.users u on u.userid = c.userid
left join note n on n.contactid = c.contactid
left join doc d on d.id = c.contactid
left join ed ON ed.id = c.contactid
where c.type in ('Client','Contact','Contractor')
--where a.email is not null
--where c.firstname = 'Ian' and c.lastname like 'Bazz%'
--where userid = '773931-5517-1697'


/*
-- LOG
select --top 10 
         replace(c.contactid,'Z','') as 'externalId'
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
left join LogDataIndex ldi on ldi.logitemid = l.logitemid
left join logdata ld on ld.logdataid = ldi.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where c.type in ('Client','Contact','Contractor') and l.logdate is not null 

where c.type in ('Client','Contact','Contractor')
and c.contactid = '828087-8837-17213'
--or l.subject like '%DM Note - Spoke to Ian%'
*/
