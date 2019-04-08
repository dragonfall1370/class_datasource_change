

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
--	      + Coalesce('Owner: ' + NULLIF(cast( iif(u.email is null, c.username, '') as varchar(max)), '') + char(10), '')
--             + Coalesce('Contact Type: ' + NULLIF(cast(c.type as varchar(max)), '') + char(10), '')
+ Coalesce('Department: ' + NULLIF(cast(c.department as varchar(max)), '') + char(10), '')
--             --+ Coalesce('Relocation: ' + NULLIF(cast(c.relocationstatus as varchar(max)), '') + char(10), '')
--             --+ Coalesce('Relocation: ' + NULLIF(cast(can.canrelocate as varchar(max)), '') + char(10), '')
--             + Coalesce('Relocation: ' + NULLIF(case when can.canrelocate = 1 then 'Yes' when can.canrelocate = 0 then '' else '' end, '') + char(10), '')
--             + Coalesce('CV Date: ' + NULLIF(cast(c.DateCVAdded as varchar(max)), '') + char(10), '') 
--             + Coalesce('Availability: ' + NULLIF(cast(can.availability as varchar(max)), '') + char(10), '') 
--             + Coalesce('Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
--             + Coalesce('Location: ' + NULLIF(cast(c.location as varchar(max)), '') + char(10), '')
--	      + Coalesce('Sub Location: ' + NULLIF(cast(c.sublocation as varchar(max)), '') + char(10), '')
--	      + Coalesce('Source: ' + NULLIF(cast(c.ContactSource as varchar(max)), '') + char(10), '')
--+ Coalesce('RS Ref: ' + NULLIF(cast(can.contactid as varchar(max)), '') + char(10), '')
--	      + Coalesce('Sector: ' + NULLIF(cast(c.sector as varchar(max)), '') + char(10), '')
--	      + Coalesce('Job Wanted > Salary Req''d (Range): ' + NULLIF(cast( concat(can.currency2,' ',can.SalaryWanted,' ',can.SalaryWanted2) as varchar(max)), '') + char(10), '')
--	      + Coalesce(char(10) + 'Registration Notes: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
--	      --+ Coalesce('About: ' + NULLIF(cast(c.comments as varchar(max)), '') + char(10), '')
       + Coalesce('Contact Type: ' + NULLIF(convert(nvarchar(max),c.type), '') + char(10), '')
       + Coalesce('Relocation: ' + NULLIF(convert(nvarchar(max),c.relocationstatus), '') + char(10), '')
       + Coalesce('URL: ' + NULLIF(convert(nvarchar(max),c.website), '') + char(10), '')
       + Coalesce('Fax: ' + NULLIF(convert(nvarchar(max),c.fax), '') + char(10), '')
       + Coalesce('Last User: ' + NULLIF(convert(nvarchar(max),u1.username), '') + char(10), '')
       + Coalesce('Home City: ' + NULLIF(convert(nvarchar(max),c.homecity), '') + char(10), '')
       + Coalesce('Reg Date (date): ' + NULLIF(convert(nvarchar(max),c.regdate), '') + char(10), '')
       + Coalesce('Updated (date): ' + NULLIF(convert(nvarchar(max),c.lastupdate), '') + char(10), '')
       + Coalesce('Reviewed (date): ' + NULLIF(convert(nvarchar(max),c.lastreviewdate), '') + char(10), '')
       + Coalesce('Emailed (date): ' + NULLIF(convert(nvarchar(max),c.dateemailed), '') + char(10), '')
       + Coalesce('Last Call (date): ' + NULLIF(convert(nvarchar(max),c.datelastcalled), '') + char(10), '')
       + Coalesce('Email 2: ' + NULLIF(convert(nvarchar(max),c.email2), '') + char(10), '')
       + Coalesce('Email 3: ' + NULLIF(convert(nvarchar(max),c.Email3), '') + char(10), '')
       + Coalesce('Years of Experiencing: ' + NULLIF(convert(nvarchar(max),c.experienceinyears), '') + char(10), '')
       + Coalesce('Date Experience Entered: ' + NULLIF(convert(nvarchar(max),c.dateexperienceset), '') + char(10), '')
       + Coalesce('Start year in Industry / Sector: ' + NULLIF(convert(nvarchar(max),c.StartYear), '') + char(10), '')
       + Coalesce('Date started at current company: ' + NULLIF(convert(nvarchar(max),c.CompanyStartDate), '') + char(10), '')
       + Coalesce('Date Last Called: ' + NULLIF(convert(nvarchar(max),c.datelastcalled), '') + char(10), '')
       --+ Coalesce('Select how often you want to call this contact (radio button): ' + NULLIF(convert(nvarchar(max),c.Select how often you want to call this contact (radio button)), '') + char(10), '')
       --+ Coalesce('Year Qualified: ' + NULLIF(convert(nvarchar(max),c.), '') + char(10), '')
       + Coalesce('CV Date: ' + NULLIF(convert(nvarchar(max),c.datecvadded), '') + char(10), '')
       + Coalesce('Availability: ' + NULLIF(convert(nvarchar(max),can.availability), '') + char(10), '')
       + Coalesce('Time Zone: ' + NULLIF(convert(nvarchar(max),c.timezonename), '') + char(10), '')
       + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),c.contactstatus), '') + char(10), '')
       + Coalesce('Discipline: ' + NULLIF(convert(nvarchar(max),c.discipline), '') + char(10), '')
       + Coalesce('Location: ' + NULLIF(convert(nvarchar(max),c.location), '') + char(10), '')
       + Coalesce('Sub Loc: ' + NULLIF(convert(nvarchar(max),c.sublocation), '') + char(10), '')
       + Coalesce('Source: ' + NULLIF(convert(nvarchar(max),c.source), '') + char(10), '')
       --+ Coalesce('Clearances: ' + NULLIF(convert(nvarchar(max),c.Clearances), '') + char(10), '')
       --+ Coalesce('Langs: ' + NULLIF(convert(nvarchar(max),c.Langs), '') + char(10), '')
       
       + Coalesce('Coverage: ' + NULLIF(convert(nvarchar(max),mm.coverage), '') + char(10), '')
       + Coalesce('Preferred Locations: ' + NULLIF(convert(nvarchar(max),mm.locations), '') + char(10), '')
       + Coalesce('Reporting To: ' + NULLIF(convert(nvarchar(max),mm.reportingto), '') + char(10), '')
       + Coalesce('Hire Authority: ' + NULLIF(convert(nvarchar(max),mm.hireauthority), '') + char(10), '')
       + Coalesce('Key Buying Condition: ' + NULLIF(convert(nvarchar(max),c.importkey), '') + char(10), '')
       + Coalesce('Assessed: ' + NULLIF(convert(nvarchar(max),mm.assessed), '') + char(10), '')
       + Coalesce('Exam Status: ' + NULLIF(convert(nvarchar(max),mm.examstatus), '') + char(10), '')
       + Coalesce('Contract: ' + NULLIF(convert(nvarchar(max),mm.Contract), '') + char(10), '')
       --+ Coalesce('Sector: ' + NULLIF(convert(nvarchar(max),c.sector), '') + char(10), '') --***
       --+ Coalesce('Sub Sector: ' + NULLIF(convert(nvarchar(max),mm.subsector), '') + char(10), '') --***
       + Coalesce('Last Update: ' + NULLIF(convert(nvarchar(max),mm.lastupdate), '') + char(10), '')
       
       --+ Coalesce('Connected (checkbox): ' + NULLIF(convert(nvarchar(max),c.Connected (checkbox)), '') + char(10), '')
       + Coalesce('Embargo (checkbox): ' + NULLIF(case when c.Embargoed = 1 then 'Yes' when c.Embargoed = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Hotlist (checkbox): ' + NULLIF(case when c.hotlist = 1 then 'Yes' when c.hotlist = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Can Email (checkbox): ' + NULLIF(case when c.AgreedToEmail = 1 then 'Yes' when c.AgreedToEmail = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Email (checkbox): ' + NULLIF(case when c.CanText = 1 then 'Yes' when c.CanText = 0 then 'No' else '' end, '') + char(10), '')
       --+ Coalesce('Add Picture ?: ' + NULLIF(convert(nvarchar(max),c.Add Picture ?), '') + char(10), '')
       + Coalesce('Company Status: ' + NULLIF(cast(c.ContactStatus as varchar(max)), '') + char(10), '')
       + Coalesce('Stars: ' + NULLIF(convert(nvarchar(max),c.starrating), '') + char(10), '')
       
       + Coalesce('Personal Check List > Work Permit Required (Checkbox): ' + NULLIF(case when can.workpermitrequired = 1 then 'Yes' when can.WorkPermitRequired = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Personal Check List > Eligible to Work: ' + NULLIF(case when can.hasworkpermit = 1 then 'Yes' when can.hasworkpermit = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Personal Check List > Can Relocate: ' + NULLIF(case when can.canrelocate = 1 then 'Yes' when can.CanRelocate = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Personal Check List > Can Work Abroad: ' + NULLIF(case when can.CanWorkAbroad = 1 then 'Yes' when can.CanWorkAbroad = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Personal Check List > Driving License: ' + NULLIF(case when can.DriveLicence = 1 then 'Yes' when can.drivelicence = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Personal Check List > Contractor: ' + NULLIF(case when can.IsContractor = 1 then 'Yes' when can.IsContractor = 0 then 'No' else '' end, '') + char(10), '')
       
       + Coalesce('Date/Salary (Date): ' + NULLIF(convert(nvarchar(max),can.datesalaryentered), '') + char(10), '')
       + Coalesce('Bonus: ' + NULLIF(convert(nvarchar(max),can.bonus), '') + char(10), '')
       + Coalesce('Social / NI: ' + NULLIF(convert(nvarchar(max),can.NINumber), '') + char(10), '')
       + Coalesce('Notice (dropdown): ' + NULLIF(convert(nvarchar(max),can.noticeperiod), '') + char(10), '')
       + Coalesce('Availability (date): ' + NULLIF(convert(nvarchar(max),can.availability), '') + char(10), '')
       + Coalesce('Avail Status: ' + NULLIF(convert(nvarchar(max),can.availabilitystatus), '') + char(10), '')
       
       + Coalesce('Job Wanted > Sector: ' + NULLIF(convert(nvarchar(max),can.sectorwanted), '') + char(10), '')
       + Coalesce('Job Wanted > Sub Sector: ' + NULLIF(convert(nvarchar(max),can.segmentwanted), '') + char(10), '')
       + Coalesce('Job Wanted > Position: ' + NULLIF(convert(nvarchar(max),can.positionwanted), '') + char(10), '')
       + Coalesce('Job Wanted > Job Type: ' + NULLIF(convert(nvarchar(max),can.jobtype), '') + char(10), '')
       + Coalesce('Job Wanted > Location Wanted: ' + NULLIF(convert(nvarchar(max),can.locationwanted), '') + char(10), '')
       + Coalesce('Job Wanted > Sublocation (dropdown): ' + NULLIF(convert(nvarchar(max),can.sublocation), '') + char(10), '')
       + Coalesce('Job Wanted > Salary Req''d (Range) (dropdown): ' + NULLIF(convert(nvarchar(max),can.StartingSalary), '') + char(10), '')
       + Coalesce('Job Wanted > Salary Req''d (Range) (text) 1: ' + NULLIF(convert(nvarchar(max),can.SalaryWanted), '') + char(10), '')
       + Coalesce('Job Wanted > Salary Req''d (Range) (text) 2: ' + NULLIF(convert(nvarchar(max),can.SalaryWanted2), '') + char(10), '')
       
       + Coalesce('Negotiable (checkbox): '  + NULLIF(case when can.negotiable = 1 then 'Yes' when can.negotiable = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Benefits: ' + NULLIF(convert(nvarchar(max),can.benefits), '') + char(10), '')
       --+ Coalesce('Remark: ' + NULLIF(convert(nvarchar(max),c), '') + char(10), '')
       --+ Coalesce('Preferred Company: ' + NULLIF(convert(nvarchar(max),c.source), '') + char(10), '')
       + Coalesce('References: ' + NULLIF(convert(nvarchar(max),c.candidateref), '') + char(10), '')
       --+ Coalesce('Bank: ' + NULLIF(convert(nvarchar(max),can.ba), '') + char(10), '')
       --+ Coalesce('Registration Notes: ' + NULLIF(convert(nvarchar(max),c.Registration Notes), '') + char(10), '')
       + Coalesce('Job Check List > Refs Checked (checkbox): '  + NULLIF(case when can.refschecked = 1 then 'Yes' when can.refschecked = 0 then 'No' else '' end, '') + char(10), '')
       + Coalesce('Job Check List > Placed (checkbox): '  + NULLIF(case when can.placed = 1 then 'Yes' when can.placed = 0 then 'No' else '' end, '') + char(10), '')
       
       + Coalesce('Company Name: ' + NULLIF(convert(nvarchar(max),Contractors.companyname), '') + char(10), '')
       + Coalesce('Company Type: ' + NULLIF(convert(nvarchar(max),Contractors.companytype), '') + char(10), '')
       + Coalesce('Company Reg No: ' + NULLIF(convert(nvarchar(max),Contractors.companyregno), '') + char(10), '')
       + Coalesce('VAT No: ' + NULLIF(convert(nvarchar(max),Contractors.companyvatno), '') + char(10), '')
       + Coalesce('Tel No: ' + NULLIF(convert(nvarchar(max),Contractors.companytel), '') + char(10), '')
       + Coalesce('E-Mail: ' + NULLIF(convert(nvarchar(max),Contractors.companyemail), '') + char(10), '')
       --+ Coalesce('Company Address > Line 1: ' + NULLIF(convert(nvarchar(max),Contractors.), '') + char(10), '')
       --+ Coalesce('Company Address > Line 2: ' + NULLIF(convert(nvarchar(max),c.Company Address > Line 2), '') + char(10), '')
       --+ Coalesce('Company Address > Line 3: ' + NULLIF(convert(nvarchar(max),c.Company Address > Line 3), '') + char(10), '')
       --+ Coalesce('Company Address > City: ' + NULLIF(convert(nvarchar(max),c.Company Address > City), '') + char(10), '')
       --+ Coalesce('Company Address > County: ' + NULLIF(convert(nvarchar(max),c.Company Address > County), '') + char(10), '')
       --+ Coalesce('Company Address > TelNo: ' + NULLIF(convert(nvarchar(max),c.Company Address > TelNo), '') + char(10), '')
       --+ Coalesce('Company Address > Email: ' + NULLIF(convert(nvarchar(max),c.Company Address > Email), '') + char(10), '')
       
       + Coalesce('Name of Bank: ' + NULLIF(convert(nvarchar(max),b.bankname), '') + char(10), '')
       + Coalesce('Bank Address 1: ' + NULLIF(convert(nvarchar(max),b.bankaddr1), '') + char(10), '')
       + Coalesce('Bank Address 2: ' + NULLIF(convert(nvarchar(max),b.bankaddr2), '') + char(10), '')
       + Coalesce('Bank Address 3: ' + NULLIF(convert(nvarchar(max),b.bankaddr3), '') + char(10), '')
       + Coalesce('Postcode: ' + NULLIF(convert(nvarchar(max),b.bankpostcode), '') + char(10), '')
       + Coalesce('Swift Address: ' + NULLIF(convert(nvarchar(max),b.swiftaddress), '') + char(10), '')
       + Coalesce('Tax Status: ' + NULLIF(convert(nvarchar(max),b.taxstatus), '') + char(10), '')
       + Coalesce('UTR (Personal): ' + NULLIF(convert(nvarchar(max),b.utrpersonal), '') + char(10), '')
       + Coalesce('UTR (Company): ' + NULLIF(convert(nvarchar(max),b.utrcompany), '') + char(10), '')
       + Coalesce('Account Details > Account Name: ' + NULLIF(convert(nvarchar(max),b.accountname), '') + char(10), '')
       + Coalesce('Account Details > Account No: ' + NULLIF(convert(nvarchar(max),b.accountno), '') + char(10), '')
       + Coalesce('Account Details > Sort Code: ' + NULLIF(convert(nvarchar(max),b.sortcode), '') + char(10), '')
       + Coalesce('Account Details > Other References > Roll No: ' + NULLIF(convert(nvarchar(max),b.rollno), '') + char(10), '')
       + Coalesce('Account Details > CIS No: ' + NULLIF(convert(nvarchar(max),b.cisno), '') + char(10), '')
                     , 1, 0, '') as note
       -- select count(*)
       from dbo.candidates can
       left join dbo.contacts c on c.contactid = can.contactid --where c.type in ('Candidate')
       --left join dbo.users u on u.userid = c.userid
       left join (select userid, username from dbo.users) u1 on u1.userid = c.lastuser
       left join MarketMap mm on mm.contactid = c.contactid
       left join Contractors on Contractors.contactid = can.contactid
       left join dbo.bankdetails b on b.accountno = can.contactid
       where c.type in ('Candidate')
)
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
	      when c.title in ('Mr','Mr.') then 'MR' 
	      when c.title in ('Mrs','Lady') then 'MRS' 
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
		when c.country like '%Romania%' then 'RO'
		when c.country like '%Gibraltar%' then 'GB'
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
		when c.country like '%Hong K%' then 'HK'
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
		when c.country like 'United Kingdom%' then 'GB'
		when c.country like 'UKnited%' then 'GB'
		when c.country like 'UKraine%' then 'UA'
		when c.country like 'UK%' then 'GB'
		when c.country like 'USA%' then 'US'
		when c.country like 'Wales%' then 'GB'
		when c.country like 'Western%' then  'AU'
		when c.country like 'West%' then 'GB'
		when c.country like 'Yorkshi%' then 'GB'
		when c.country like '%UNITED%ARAB%' then 'AE'
		when c.country like '%UAE%' then 'AE'
		when c.country like '%U.A.E%' then 'AE'
		when c.country like '%UNITED%KINGDOM%' then 'GB'
		when c.country like '%UNITED%STATES%' then 'US'
		when c.country like '%US%' then 'US'
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
       --, 'Summary (Top) User' as 'Contact Owners'
       , case when c.website like '%linkedin%' then c.website else '' end as 'LinkedIn'
       , c.jobtitle as 'candidate-jobTitle1'
	, c.company as 'candidate-employer1'
	, c.company as 'candidate-company1'
       
       
       , case
		when can.nationality like 'African%' then 'ZA'
		when can.nationality like 'Albania%' then 'AL'
		when can.nationality like 'America%' then 'US'
		when can.nationality like 'Austral%' then 'AU'
		when can.nationality like 'Belgian%' then 'BE'
		when can.nationality like 'Belgium%' then 'BE'
		when can.nationality like 'Brazili%' then 'BR'
		when can.nationality like 'British%' then 'GB'
		when can.nationality like 'Bulgari%' then 'BG'
		when can.nationality like 'Cameroo%' then 'CM'
		when can.nationality like 'Canadia%' then 'CA'
		when can.nationality like 'Chinese%' then 'CN'
		when can.nationality like 'Czech%' then 'CZ'
		when can.nationality like 'Danish%' then 'DK'
		when can.nationality like 'Double%' then 'NL'
		when can.nationality like 'Dutch%' then 'NL'
		when can.nationality like 'Finnish%' then 'FI'
		when can.nationality like 'French%' then 'FR'
		when can.nationality like 'German%' then 'DE'
		when can.nationality like 'Greek%' then 'GR'
		when can.nationality like 'Hong%' then 'HK'
		when can.nationality like 'Hungari%' then 'HU'
		when can.nationality like 'Iceland%' then 'IS'
		when can.nationality like 'Indian%' then 'IN'
		when can.nationality like 'Indones%' then 'ID'
		when can.nationality like 'Iranian%' then 'IR'
		when can.nationality like 'Irish%' then 'IE'
		when can.nationality like 'Israeli%' then 'IL'
		when can.nationality like 'Italian%' then 'IT'
		when can.nationality like 'Japanes%' then 'JP'
		when can.nationality like 'Kazakhs%' then 'KZ'
		when can.nationality like 'Korean%' then 'KR'
		when can.nationality like 'Lithuan%' then 'LT'
		when can.nationality like 'Luxembo%' then 'LU'
		when can.nationality like 'Malaysi%' then 'MY'
		when can.nationality like 'Moldova%' then 'MD'
		when can.nationality like 'Norwegi%' then 'NO'
		when can.nationality like 'Pakista%' then 'PK'
		when can.nationality like 'Philipp%' then 'PH'
		when can.nationality like 'Polish%' then 'PL'
		when can.nationality like 'Romania%' then 'RO'
		when can.nationality like 'Russian%' then 'RU'
		when can.nationality like 'serbian%' then 'RS'
		when can.nationality like 'Singapo%' then 'SG'
		when can.nationality like 'Sloveni%' then 'SI'
		when can.nationality like 'Spanish%' then 'ES'
		when can.nationality like 'Swedish%' then 'SE'
		when can.nationality like 'Swiss%' then 'CH'
		when can.nationality like 'Taiwane%' then 'TW'
		when can.nationality like 'Turkish%' then 'TR'
		when can.nationality like 'Ukraini%' then 'UA'
		when can.nationality like 'Uruguay%' then 'UY'
		when can.nationality like 'Vietnam%' then 'VN'
		when can.nationality like 'Zealand%' then 'NZ'
		when can.nationality like '%UNITED%ARAB%' then 'AE'
		when can.nationality like '%UAE%' then 'AE'
		when can.nationality like '%U.A.E%' then 'AE'
		when can.nationality like '%UNITED%KINGDOM%' then 'GB'
		when can.nationality like '%UNITED%STATES%' then 'US'
		when can.nationality like '%US%' then 'US'
              end as 'candidate-citizenship'
       , case
              when can.currency1 = 'AED' then 'AED'
              when can.currency1 = 'AUD' then 'AUD'
              when can.currency1 = 'CAD' then 'CAD'
              when can.currency1 = 'Canadian Dollar' then 'CAD'
              when can.currency1 = 'CHF' then 'CHF'
              when can.currency1 = 'CNY/RMB' then 'CNY'
              when can.currency1 = 'HKD' then 'HKD'
              when can.currency1 = 'JPY' then 'JPY'
              when can.currency1 = 'MYR' then 'MYR'
              when can.currency1 = 'RM' then 'MYR'
              when can.currency1 = 'SGD' then 'SGD'
              when can.currency1 = 'TWD' then 'TWD'
              when can.currency1 = 'UK£' then 'GBP'
              when can.currency1 = 'US$' then 'USD'
              when can.currency1 like '£' then 'GBP'
              when can.currency1 like '€' then 'EUR'
              when can.currency1 in ('US$','USD') then 'USD'
              else '' end as 'candidate-currency'
       , can.currentsalary as 'candidate-currentSalary'
       , can.dob as 'candidate-dob'

       , n.note as 'contact-note'
       , d.doc as 'contact-resume'
-- select count(*) --3015 -- select top 100 * -- select distinct c.title --can.currency1 --select distinct can.nationality --, can.nationality2
from dbo.candidates can 
left join dbo.contacts c on c.contactid = can.contactid --where c.type in ('Candidate') and nationality2 is not null and nationality2 <> ''
left join ed ON ed.id = can.contactid
--left join dbo.addresses a on a.contactid = c.companyid
left join dbo.users u on u.userid = c.userid
left join note n on n.contactid = can.contactid
left join doc d on d.id = c.contactid
where c.type in ('Candidate')
--and c.candidateref in ('00125121','00121824')
--and c.contactid in ('851778-5710-188')
--and c.firstname in ('Krishnan(shankar)')

--select * from users


/*
-- LOG
select top 10
         c.contactid as 'candidate-externalId'
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
left join LogDataIndex ldi on ldi.logitemid = l.logitemid
left join logdata ld on ld.logdataid = ldi.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where c.type in ('Candidate') and l.logdate is not null 

and c.candidateref in ('00121824')
--and c.contactid = '828087-8837-17213'
--where l.subject like '%DM Note - Spoke to Ian%'
*/