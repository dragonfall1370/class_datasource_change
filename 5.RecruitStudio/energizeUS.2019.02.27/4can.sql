

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
	select can.contactid
	, Stuff( 
	          Coalesce('CandNo: ' + NULLIF(cast(c.candidateref as varchar(max)), '') + char(10), '')
	      + Coalesce('Owner: ' + NULLIF(cast( iif(u.email is null, c.username, '') as varchar(max)), '') + char(10), '')
             + Coalesce('Contact Type: ' + NULLIF(cast(c.type as varchar(max)), '') + char(10), '')
+ Coalesce('Department: ' + NULLIF(cast(c.department as varchar(max)), '') + char(10), '')
             --+ Coalesce('Relocation: ' + NULLIF(cast(c.relocationstatus as varchar(max)), '') + char(10), '')
             --+ Coalesce('Relocation: ' + NULLIF(cast(can.canrelocate as varchar(max)), '') + char(10), '')
             + Coalesce('Relocation: ' + NULLIF(case when can.canrelocate = 1 then 'Yes' when can.canrelocate = 0 then '' else '' end, '') + char(10), '')
             + Coalesce('CV Date: ' + NULLIF(cast(c.DateCVAdded as varchar(max)), '') + char(10), '') 
             + Coalesce('Availability: ' + NULLIF(cast(can.availability as varchar(max)), '') + char(10), '') 
             + Coalesce('Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
             + Coalesce('Location: ' + NULLIF(cast(c.location as varchar(max)), '') + char(10), '')
	      + Coalesce('Sub Location: ' + NULLIF(cast(c.sublocation as varchar(max)), '') + char(10), '')
	      + Coalesce('Source: ' + NULLIF(cast(c.ContactSource as varchar(max)), '') + char(10), '')
+ Coalesce('RS Ref: ' + NULLIF(cast(can.contactid as varchar(max)), '') + char(10), '')
	      + Coalesce('Sector: ' + NULLIF(cast(c.sector as varchar(max)), '') + char(10), '')
	      + Coalesce('Job Wanted > Salary Req''d (Range): ' + NULLIF(cast( concat(can.currency2,' ',can.SalaryWanted,' ',can.SalaryWanted2) as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Registration Notes: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
	      --+ Coalesce('About: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
              , 1, 0, '') as note
from dbo.candidates can
left join dbo.contacts c on c.contactid = can.contactid
left join dbo.users u on u.userid = c.userid
where c.type in ('Candidate') )
--select * from note where c.contactid = '828087-8837-17213'
--select * from dbo.contacts where contactid = '828087-8837-17213'


-- FILES
, doc (id,doc) as (
        SELECT id
                     , STUFF((SELECT DISTINCT ',' + filename from attachments WHERE id = a.id /*and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS doc 
        FROM (select id from attachments) as a GROUP BY a.id )
--select top 100 * from doc


-- select distinct c.title from dbo.contacts c 
-- select distinct c.ismalegender from dbo.contacts c 
-- select distinct can.nationality from dbo.candidates can
-- select distinct can.currency1 from dbo.candidates can


select 
         can.contactid as 'candidate-externalId' --,c.candidateref
	, case 
	      when c.title in ('Dr') then 'DR' 
	      when c.title in ('Mr') then 'MR' 
	      when c.title in ('Mrs') then 'MRS' 
	      when c.title in ('Miss','Ms') then 'MISS'
	      else '' end as 'candidate-title'
	, case when c.ismalegender = 1 then 'MALE' when c.ismalegender = 0 then 'FEMALE' else '' end as 'candidate-gender'     
       , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'candidate-firstName'
       , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'candidate-Lastname'
       , c.alsoknownas as 'PreferredName'
       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'candidate-email' --c.email  
       , c.email2 as 'candidate-workemail'
              
       , ltrim(Stuff( Coalesce(' ' + NULLIF(c.address1, ''), '') + Coalesce(', ' + NULLIF(c.address2, ''), '') + Coalesce(', ' + NULLIF(c.address3, ''), '') + Coalesce(', ' + NULLIF(c.city, ''), '') + Coalesce(', ' + NULLIF(c.county, ''), '') + Coalesce(', ' + NULLIF(c.postcode, ''), '') + Coalesce(', ' + NULLIF(c.country, ''), ''), 1, 1, '') ) as 'candidate-address'
       , c.city as 'candidate-city'
       , c.county as 'candidate-state'
       , c.postcode as 'candidate-zipCode'
       , case
		when c.country like 'Africa%' then 'ZA'
		when c.country like 'Austral%' then 'AU'
		when c.country like 'Austria%' then 'AT'
		when c.country like 'Begium%' then 'BE'
		when c.country like 'Beglium%' then 'BE'
		when c.country like 'Belgie%' then 'BE'
		when c.country like 'België%' then 'BE'
		when c.country like 'Belgiqu%' then 'BE'
		when c.country like 'Belgium%' then 'BE'
		when c.country like 'Berkshi%' then 'GB'
		when c.country like 'Birming%' then 'GB'
		when c.country like 'Brussel%' then 'BE'
		when c.country like 'Bulgari%' then 'BG'
		when c.country like 'Canada%' then 'CA'
		when c.country like 'CA%' then 'CA'
		when c.country like 'Denmark%' then 'DK'
		when c.country like 'Deutsch%' then 'DE'
		when c.country like 'Dubai%' then 'AE'
		when c.country like 'England%' then 'GB'
		when c.country like 'Essex%' then 'GB'
		when c.country like 'Finland%' then 'FI'
		when c.country like 'France%' then 'FR'
		when c.country like 'Frankfu%' then 'DE'
		when c.country like 'Germany%' then 'DE'
		when c.country like 'Gibralt%' then 'ES'
		when c.country like 'Greece%' then 'GR'
		when c.country like 'Hamburg%' then 'DE'
		when c.country like 'Hampshi%' then 'GB'
		when c.country like 'Hungary%' then 'HU'
		when c.country like 'Ireland%' then 'IE'
		when c.country like 'Italy%' then 'IT'
		when c.country like 'Kiel%' then 'DE'
		when c.country like 'Lancs%' then 'GB'
		when c.country like 'London%' then 'GB'
		when c.country like 'Lübeck%' then 'DE'
		when c.country like 'Luxembo%' then 'LU'
		when c.country like 'Malta%' then 'MT'
		when c.country like 'Münche%' then 'DE'
		when c.country like 'Munich%' then 'DE'
		when c.country like 'Netherl%' then 'NL'
		when c.country like 'Norway%' then 'NO'
		when c.country like 'Poland%' then 'PL'
		when c.country like 'russia%' then 'RU'
		when c.country like 'Scotlan%' then 'GB'
		when c.country like 'Singapo%' then 'SG'
		when c.country like 'Spain%' then 'ES'
		when c.country like 'Sweden%' then 'SE'
		when c.country like 'Switzer%' then 'CH'
		when c.country like 'UAE%' then 'AE'
		when c.country like 'UKnited%' then 'GB'
		when c.country like 'UKraine%' then 'UA'
		when c.country like 'UK%' then 'GB'
		when c.country like 'USA%' then 'US'
		when c.country like 'Wales%' then 'GB'
		when c.country like 'Western%' then  'AU'
		when c.country like 'West%' then 'GB'
		when c.country like 'Yorkshi%' then 'GB'
              else '' end as 'candidate-Country'

       --, a.email as 'Summary (Top)|Address|Email: Personal Email'
       , case when c.ishomeaddress = 1 then 'PERSONAL_ADDRESS' when c.ishomeaddress = 0 then 'WORKPLACE' else '' end as 'Location > Type'
       , c.longitude as 'Longitude'
       , c.latitude as 'Latitude'
       , u.email as 'contact-owners' --, c.username
       
       , c.directtel as 'candidate-phone'
	, c.mobiletel as 'candidate-mobile'
	, c.hometel as 'candidate-homePhone'	
	, c.worktel as 'candidate-workPhone'

       , c.department as 'Department'       
       , c.jobtitle as 'candidate-jobTitle1'
	, c.company as 'candidate-employer1'
	, c.company as 'candidate-company1'
       --, 'Summary (Top) User' as 'Contact Owners'
       , case when c.website like '%linkedin%' then c.website else '' end as 'LinkedIn'
       , case
		when can.nationality like 'America%' then 'US'
		when can.nationality like 'British%' then 'GB'
		when can.nationality like 'Dutch%' then 'NL'
		when can.nationality like 'French%' then 'FR'
		when can.nationality like 'German%' then 'DE'
		when can.nationality like 'Italian%' then 'IT'
		when can.nationality like 'Russian%' then 'RU'
              end as 'candidate-citizenship'
       , case
              when can.currency1 like 'Euro' then 'EUR'
              when can.currency1 like 'GBP' then 'GBP'
              when can.currency1 in ('US$','USD') then 'USD'
              else '' end as 'candidate-currency'
       , can.currentsalary as 'candidate-currentSalary'
       , can.dob as 'candidate-dob'

       , n.note as 'contact-note'
       , d.doc as 'contact-resume'
-- select count(*) --3015 -- select *
from dbo.candidates can
left join dbo.contacts c on c.contactid = can.contactid
left join ed ON ed.id = can.contactid
--left join dbo.addresses a on a.contactid = c.companyid
left join dbo.users u on u.userid = c.userid
left join note n on n.contactid = can.contactid
left join doc d on d.id = c.contactid
where c.type in ('Candidate')
--and c.candidateref in ('00125121','00121824')
--and c.contactid in ('851778-5710-188')
--and c.firstname in ('Krishnan(shankar)')




/*
-- LOG
select 
         c.candidateref as 'candidate-externalId'
       , case when (ltrim(replace(c.firstname,'?','')) = '' or  c.firstname is null) then 'Firstname' else ltrim(replace(c.firstname,'?','')) end as 'candidate-firstName'
       , case when (ltrim(replace(c.lastname,'?','')) = '' or c.lastname is null) then concat('Lastname-',c.contactid) else ltrim(replace(c.lastname,'?','')) end as 'candidate-Lastname'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'candidate' as 'type'       
       , l.logdate as 'insert_timestamp'
	, Stuff( 
	          Coalesce('Name: ' + NULLIF(cast(u.username as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Subject: ' + NULLIF(cast(l.subject as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Log Item Text: ' + char(10) + NULLIF(cast(ld.text as varchar(max)), '') + char(10), '')
              , 1, 0, '')  as 'content'
       --, ld.*
-- select count(*)       
from dbo.candidates can
left join dbo.contacts c on c.contactid = can.contactid 
left join dbo.logitems l on l.itemid =  c.contactid
left join dbo.logdata ld on ld.logdataid = l.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where c.type in ('Candidate')
and c.candidateref in ('00121824')
--and c.contactid = '828087-8837-17213'
--where l.subject like '%DM Note - Spoke to Ian%'
*/