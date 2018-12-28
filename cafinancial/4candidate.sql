
with
-- EMAIL
  mail1 (ID,email) as (select CVID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from candidates )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed

--DOCUMENT
, d (id, name) as (SELECT CandidateID
                 , STUFF((SELECT DISTINCT ',' + Nm from DocFolder WHERE CandidateID <> 0 and CandidateID = a.CandidateID --and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') 
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select CandidateID from DocFolder where CandidateID <> 0) AS a GROUP BY a.CandidateID)
-- select top 100 * from DocFolder
-- select count(*) from d
-- select top 100 * from d


-- Associations
, a (id,name) as (SELECT CVID
                 , STUFF(( SELECT Coalesce('Association: ' + NULLIF(cast(Association as varchar(max)), '') + char(10), '')
                                from Associations WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') AS name 
                 FROM (select CVID from Associations ) AS a GROUP BY a.CVID )


-- SKILL
, s (id,name) as ( SELECT CVID
                 , STUFF((SELECT DISTINCT ', ' + ComputerLiteracy from skills WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select CVID from skills) AS a GROUP BY a.CVID )


-- LANGUAGE
, l (id,name) as ( SELECT CVID
                 , STUFF((SELECT DISTINCT ', ' + Language from Languages WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name 
                 FROM (select CVID from Languages ) AS a GROUP BY a.CVID )


-- WORK HISTORY
, w (id,name) as ( SELECT CVID
                 , STUFF((SELECT char(10)
                                   + Coalesce('Owner: ' + NULLIF(cast( concat(o.fullname,' - ',o.email) as varchar(max) ), '') + char(10), '') --<<< UserID
                                   --+ Coalesce('CompanyRowID: ' + NULLIF(cast(WHCompanyRowID as varchar(max)), '') + char(10), '') --<<<
                                   + Coalesce('Company: ' + NULLIF(cast(WHCompany as varchar(max)), '') + char(10), '')
                                   + Coalesce('Start Date: ' + NULLIF(cast(WHStartDate as varchar(max)), '') + char(10), '')
                                   + Coalesce('Position: ' + NULLIF(cast( WHPosition as varchar(max)), '') + char(10), '')
                                   + Coalesce('Duties: ' + NULLIF(cast( WHDuties_ as varchar(max)), '') + char(10), '')
                                   + Coalesce('Leaving Reason: ' + NULLIF( WHLeavingReason , '') + char(10), '')
                                   + Coalesce('Comment: ' + NULLIF(cast(WHComment as varchar(max)), '') + char(10), '')
                                   + char(10)
                                from WorkHistory w
                                left join owners o on o.id = w.UserID
                                WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name
                 FROM (select CVID from WorkHistory ) AS a GROUP BY a.CVID )


-- QUALIFICATION
, q (id,name) as ( SELECT CVID
                 , STUFF((SELECT char(10)
                                   + Coalesce('Owner: ' + NULLIF( cast( concat(o.fullname,' - ',o.email) as varchar(max) ), '') + char(10), '') --<<<
                                   + Coalesce('Institute: ' + NULLIF(cast(Institute as varchar(max)), '') + char(10), '')
                                   + Coalesce('Qualification Description: ' + NULLIF(cast(QualificationDescription as varchar(max)), '') + char(10), '')
                                   + Coalesce('Enddate: ' + NULLIF(cast(Enddate as varchar(max)), '') + char(10), '')
                                   + Coalesce('Comments: ' + NULLIF(cast(Comments as varchar(max)), '') + char(10), '')
                                   + Coalesce('Institute Type: ' + NULLIF(cast(InstituteType as varchar(max)), '') + char(10), '')
                                   + Coalesce('Qualification Status: ' + NULLIF(cast(QualificationStatus as varchar(max)), ''), '')
                                   + char(10)
                                from Qualifications q
                                left join owners o on o.id = q.UserID
                                WHERE CVID = a.CVID
                                FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS name
                 FROM (select CVID from Qualifications ) AS a GROUP BY a.CVID )
--select top 100 * from q


-- CANDIDATE OWNER
, o1 as (
       select ltrim(case
              when concat(firstname,' ',surname) like 'X % X %' then replace( concat(firstname,' ',surname), 'X ','')
              when concat(firstname,' ',surname) like 'X% X%' then replace( concat(firstname,' ',surname), 'X','')
              else concat(firstname,' ',surname) end) as owner
              , email
       from owners
)
--select * from o1
       
, o2 as (
        SELECT owner, email, ROW_NUMBER() OVER(PARTITION BY owner ORDER BY owner) AS rn 
        from o1
)
--select * from o2

, oc1 as (
select CVID
       , ltrim(replace(
              case
              when ConsultantName like 'X % X %' then replace( ConsultantName, 'X ','')
              when ConsultantName like 'X% X%' then replace( ConsultantName, 'X','')
              else ConsultantName end
       ,'  ',' ')) as owner
from candidates 
)

, oc2 as (
select oc1.CVID, o2.email
from oc1
left join (select * from o2 where rn = 1 ) o2 on o2.owner = oc1.owner
)
--select * from oc2

---------------------------------------------------------------
select --top 20
o.email as 'candidate-owners' --UserID
, c.CVID as 'candidate-ExternalId'
, case
       when c.Title in ('Dr') then 'DR'
       when c.Title in ('Mr','Mr.') then 'MR'
       when c.Title in (' Mrs','Mrs','Mrs.') then 'MRS'
       when c.Title in ('Mis','Miss','Miss.') then 'MISS'
       when c.Title in ('Ms','Ms.') then 'MS'
       else '' end as 'candidate-Title'
, c.FirstName as 'candidate-FirstName'
, c.MiddleName as 'candidate-MiddleName'
, c.Surname as 'candidate-Lastname'
, c.ResAddr1 as 'candidate-Address'
, c.City as 'candidate-City'
, c.Tel_Home as 'candidate-HomePhone'
, c.Tel_Work as 'candidate-workPhone'
, c.Mobile as 'candidate-phone', c.Mobile as 'candidate-Mobile'
, c.SAID as 'SA ID' --<<
, CONVERT(VARCHAR(10),c.DOB,120) as 'candidate-DOB'
       , iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.CVID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
       , c.WorkEMail as 'candidate-workEmail'
, c.CurrentPosition as 'candidate-JobTitle1'
, c.CurrentEmployer as 'candidate-Employer1'
, c.CurrentEmployer as 'candidate-Company1'
, c.Salary_Current as 'candidate-CurrentSalary'
, c.Salary_Desired as 'candidate-DesiredSalary'


, case 
		when c.Nationality like 'African%' then 'ZA'
		when c.Nationality like 'South African' then 'ZA'
		when c.Nationality like 'SA Permanent Resident' then 'ZA'
		when c.Nationality like 'America%' then 'US'
		when c.Nationality like 'Angolan%' then 'AO'
		when c.Nationality like 'Austral%' then 'AU'
		when c.Nationality like 'Austria%' then 'AT'
		when c.Nationality like 'Belgium%' then 'BE'
		when c.Nationality like 'Botswan%' then 'BW'
		when c.Nationality like 'British%' then 'GB'
		when c.Nationality like 'Camaroo%' then 'CM'
		when c.Nationality like 'Canadia%' then 'CA'
		when c.Nationality like 'Chinese%' then 'CN'
		when c.Nationality like 'Congole%' then 'CG' --'CD'
		when c.Nationality like 'Zaïre' then 'CG'
		when c.Nationality like 'Cuban%' then 'CU'
		when c.Nationality like 'Czech%' then 'CZ'
		when c.Nationality like 'Dutch%' then 'NL'
		when c.Nationality like 'Eritrea%' then 'ER'
		when c.Nationality like 'French%' then 'FR'
		when c.Nationality like 'German%' then 'DE'
		when c.Nationality like 'Ghanaia%' then 'GH'
		when c.Nationality like 'Indian%' then 'IN'
		when c.Nationality like 'Irish%' then 'IE'
		when c.Nationality like 'Italian%' then 'IT'
		when c.Nationality like 'Kenyan%' then 'KE'
		when c.Nationality like 'Malagas%' then 'MG'
		when c.Nationality like 'Maurita%' then 'MR'
		when c.Nationality like 'Mauriti%' then 'MU'
		when c.Nationality like 'Mozambi%' then 'MZ'
		when c.Nationality like 'Namibia%' then 'NA'
		when c.Nationality like 'Nigeria%' then 'NG'
		when c.Nationality like 'New Zealand' then 'NZ'
		when c.Nationality like 'Pakista%' then 'PK'
		when c.Nationality like 'Philipp%' then 'PH'
		when c.Nationality like 'Polish%' then 'PL'
		when c.Nationality like 'Portuge%' then 'PT'
		when c.Nationality like 'Russian%' then 'RU'
		when c.Nationality like 'Saudi%' then 'SA'
		when c.Nationality like 'Somali%' then 'SO'
		when c.Nationality like 'Swazi%' then 'SZ'
		when c.Nationality like 'Swiss%' then 'CH'
		when c.Nationality like 'Tanzani%' then 'TZ'
		when c.Nationality like 'Ugandan%' then 'UG'
		when c.Nationality like 'Zambian%' then 'ZM'
		when c.Nationality like 'Zealand%' then 'NZ'
		when c.Nationality like 'Zimbabw%' then 'ZW'
		when c.Nationality like '%UNITED%ARAB%' then 'AE'
		when c.Nationality like '%UAE%' then 'AE'
		when c.Nationality like '%U.A.E%' then 'AE'
		when c.Nationality like '%UNITED%KINGDOM%' then 'GB'
		when c.Nationality like '%UNITED%STATES%' then 'US'
		when c.Nationality like '%US%' then 'US'
       else '' end as 'candidate-Citizenship'

, case c.Gender when 'Male' then 'MALE' when 'Female' then 'FEMALE' else '' end as 'candidate-Gender'
, c.Race as 'Race' --<<
, c.NoticePeriod as 'Notice period (days)' --<<
       --, oc2.email as 'candidate-Owners' --, c.ConsultantName
, c.Category as 'Category' --<<
, c.Source as 'Source' --<<
, c.Status as 'Status'--<<
, STUFF(--Coalesce('Other Email ' + NULLIF(cast(e2.email as varchar(max)), '') + char(10), '')
              Coalesce('Consultant Name: ' + NULLIF(cast(c.ConsultantName as varchar(max)), '') + char(10), '')
          + Coalesce('Cons Comment: ' + NULLIF(cast(c.ConsComment as varchar(max)), '') + char(10), '')
          + Coalesce('Disability: ' + NULLIF(cast(c.Disability as varchar(max)), '') + char(10), '')
          + Coalesce('Disability Nature: ' + NULLIF(cast(c.DisabilityNature as varchar(max)), '') + char(10), '')
       , 1, 0, '') AS 'candidate-note'
--, c.Newsletter as 'Newsletter' --<<
, d.name 'candidate-resume'
, STUFF(Coalesce('' + NULLIF(ltrim( cast(a.name as varchar(max)) ), '') + char(10), '')
          + Coalesce('Profile: ' + NULLIF(cast(c.CandProfile as varchar(max)), '') + char(10), '')
          --+ Coalesce('Qualifications: ' + char(10) + NULLIF(cast(q.name as varchar(max)), '') + char(10), '')
       , 1, 0, '') AS 'candidate-education'
--, c.CandProfile as 'candidate-Skills'
--, l.name as 'candidate-skills'
/*, STUFF( Coalesce('Profile: ' + NULLIF(cast(c.CandProfile as varchar(max)), '') + char(10), '')
          + Coalesce('Languages: ' + NULLIF(cast(l.name as varchar(max)), '') + char(10), '')
       , 1, 0, '') */
, l.name AS 'candidate-languages' -->>
, ltrim(s.name) as 'candidate-skills'
--, w.name as '#candidate-workhistory'
--, q.name as '#candidate-education'

--  select count(*) --21736 -- select distinct ConsultantName --Gender --Title --Nationality -- select 
from candidates c
left join ed on ed.id = c.CVID
--left join e2 on e2.id = c.CVID
left join owners o on o.id = c.UserID
left join a on a.id = c.CVID
left join d on d.id = c.CVID
left join s on s.id = c.CVID
left join l on l.id = c.CVID
--left join w on w.id = c.CVID
--left join q on q.id = c.CVID
--left join oc2 on oc2.CVID = c.CVID

--where d.id is not null and s.id is not null and l.id is not null and w.id is not null and w.id is not null
