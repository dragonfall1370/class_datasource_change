
/*with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + filename from Attachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM Attachments as c GROUP BY c.ParentID )
select count(*) from attachment
*/

-- ALTER DATABASE [currandaly] SET COMPATIBILITY_LEVEL = 130
with
-- EMAIL
  mail1 (ID,email) as (select CandidateId, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from Candidates where email like '%_@_%.__%' )
--, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID, email1, email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		--left join (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 ) */
, e1 as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
--, e3 as (select ID, email from mail4 where rn = 3)
--, e4 as (select ID, email from mail4 where rn = 4)
--, oe as (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email FROM mail4 AS a where rn > 2 GROUP BY a.ID)
--select top 100 * from e3


-- select * from Attachments
, attachment as ( 
       SELECT 
                ParentID
              , STUFF((
                     --SELECT ',' + replace(filename,',','') 
                     SELECT ',' + replace(filename,',','') as filename
                     from Attachments
                     WHERE ParentID = c.ParentID and filename is not NULL and filename <> ''
                     FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename 
       FROM Attachments as c 
       left join Candidates on Candidates.CandidateId = c.ParentId where Candidates.CandidateId is not null --<<
       GROUP BY c.ParentID )
--select count(*) from attachment
--select top 10 * from attachment
-- select ParentID,filename from CandidatesAttachments


-- Candidates_Experience_Details
, exp(LEADID, content) as (
       SELECT 
                LEADID
              , STUFF(( 
                            select char(10) + 
                            stuff(
                                   Coalesce('Occupation / Title: ' + NULLIF(cast(OccupationTitle as varchar(max)), '') + char(10), '')
                               + Coalesce('Company: ' + NULLIF(cast(Company as varchar(max)), '') + char(10), '')
                               + Coalesce('Summary: ' + NULLIF(cast(Summary as varchar(max)), '') + char(10), '')
                               + Coalesce('Work Duration From: ' + NULLIF(cast(WorkDuration_From as varchar(max)), '') + char(10), '')
                               + Coalesce('Work Duration To: ' + NULLIF(cast(WorkDuration_To as varchar(max)), '') + char(10), '')
                               + Coalesce('I currently work here: ' + NULLIF(cast(Icurrentlyworkhere as varchar(max)), '') + char(10), '')
                            , 1, 0, '') as content
                            from Candidates_Experience_Details
                            WHERE LEADID = a.LEADID  --order by WorkDuration_From desc
                            FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS content
       FROM Candidates_Experience_Details as a 
       left join Candidates on Candidates.CandidateId = a.LEADID --where Candidates.CandidateId is not null --<<
       GROUP BY a.LEADID
       )
--select * from exp where userid in (163454);
--select count(*) from attachment


-- select * from Candidates_Educational_Details
, edu(LEADID, content) as (
       SELECT 
                LEADID
              , STUFF(( 
                            select char(10) + 
                            stuff(
                                   Coalesce('Institute School: ' + NULLIF(cast(InstituteSchool as varchar(max)), '') + char(10), '')
                               + Coalesce('Major / Department: ' + NULLIF(cast(MajorDepartment as varchar(max)), '') + char(10), '')
                               + Coalesce('Degree: ' + NULLIF(cast(Degree as varchar(max)), '') + char(10), '')
                               + Coalesce('Duration From: ' + NULLIF(cast(Duration_From as varchar(max)), '') + char(10), '')
                               + Coalesce('Duration To: ' + NULLIF(cast(Duration_To as varchar(max)), '') + char(10), '')
                               + Coalesce('Currently pursuing: ' + NULLIF(cast(Currentlypursuing as varchar(max)), '') + char(10), '')
                            , 1, 0, '') as content
                            from Candidates_Educational_Details
                            WHERE LEADID = a.LEADID  --order by Duration_From
                            FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS content
       FROM Candidates_Educational_Details as a 
       left join Candidates on Candidates.CandidateId = a.LEADID --where Candidates.CandidateId is not null --<<
       GROUP BY a.LEADID
       )
--select count(*) from edu
--select * from edu
-- select ParentID,filename from CandidatesAttachments


select --top 300
         c.CandidateId As 'candidate-externalId'
--	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
--	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',C.CandidateId) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'No FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then 'No LastName' else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
       , iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.CandidateId as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email' --, c.Email As 'candidate-email'
       , c.Phone As 'candidate-phone'
       , c.AssociatedTags as 'candidate-Keyword'
       , c.CurrentEmployer As 'candidate-employer1'
       , c.CurrentJobTitle As 'candidate-jobTitle1'       
       , u.email As 'candidate-owners' -- , c.CandidateOwnerID As 'candidate-owners'
       --, c.ExpectedSalary as 'candidate-desiredsalary' , CurrentSalary_BasicandAllowances
       --, c.CurrentSalary as 'candidate-currentsalary' , ExpectedBasicSalary
       --, c.EmploymentHistory as 'candidate-workHistory'
       , c.PresentAddress as 'address'
       , case
               when C.gender in ('F','Femaie','Female','Female30') then 'FEMALE' 
               when C.gender in ('m','Male') then 'MALE' 
               else '' end as 'candidate-gender'
       , ltrim(Stuff(    
                           Coalesce('Other email: ' + NULLIF(cast(e2.email as varchar(max)), '') + char(10), '')
                        + Coalesce('Current Salary: ' + NULLIF(c.CurrentSalary_BasicandAllowances, '') + char(10), '')
                        + Coalesce('Desired Salary: ' + NULLIF(c.ExpectedBasicSalary, '') + char(10), '')
                        + Coalesce('Experience in Years: ' + NULLIF(c.ExperienceinYears, '') + char(10), '')
                        + Coalesce('Total work exp (month): ' + NULLIF(c.Totalworkexp_month, '') + char(10), '')
                        + Coalesce('Salutation: ' + NULLIF(c.Salutation, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(c.LastActivityTime, '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(c.Source, '') + char(10), '')
                        + Coalesce('Is Unqualified: ' + NULLIF(c.IsUnqualified, '') + char(10), '')
                        + Coalesce('Candidate Status: ' + NULLIF(c.CandidateStatus, '') + char(10), '')
                        + Coalesce('Secondary Contact Number: ' + NULLIF(c.SecondaryContactNumber, '') + char(10), '')
                        + Coalesce('Recruiter Notes: ' + NULLIF(c.RecruiterNotes, '') + char(10), '')
                        /*
                        + Coalesce('Highest Qualification Held: ' + NULLIF(c.HighestQualificationHeld, '') + char(10), '') 
                        + Coalesce('Created By: ' + NULLIF(concat(u1.FirstName,' ',u1.LastName,' - ',u1.email), '') + char(10), '') --c.CreatedBy
                        + Coalesce('Is Attachment Present: ' + NULLIF(c.IsAttachmentPresent, '') + char(10), '')
                        + Coalesce('Date Registered: ' + NULLIF(c.DateRegistered, '') + char(10), '')
                        + Coalesce('Consultant: ' + NULLIF(c.Consultant, '') + char(10), '')
                        + Coalesce('Brief: ' + NULLIF(c.CandidateProfile, '') + char(10), '')*/
                , 1, 0, '') ) as 'candidate-note'
/*       
       , c.Mobile As 'candidate-mobile'
       --, c.website as 'candidate-website' --<< empty
--, c.Street
, c.City as 'candidate-city'
, c.StateProvince as 'candidate-State'
, c.ZipPostalCode as 'candidate-zipCode'
--, c.Country as 'candidate-country'
        , ltrim(Stuff( Coalesce(' ' + NULLIF(c.Street, '') + char(10), '')
                        + Coalesce(', ' + NULLIF(c.City, '') + char(10), '')
                        + Coalesce(', ' + NULLIF(c.StateProvince, '') + char(10), '')
                        + Coalesce(', ' + NULLIF(c.ZipPostalCode, '') + char(10), '')
                        + Coalesce(', ' + NULLIF(c.Country, '') + char(10), '')
                , 1, 1, '') ) as 'candidate-Address'

       , c.SkillSet as 'candidate-skills'
       , c.SkypeID as 'candidate-skype' --<<
       , c.workphone as 'candidate-workphone'
       , c.Industry as 'INDUSTRY' --<<
       , c.QualificationsEducationHistory as 'candidate-education'
       , c.linkedinURL as 'candidate-linkedin'
, case
		when c.Country like 'AFGHANI%' then 'AF'
		when c.Country like 'Africa%' then 'ZA'
		when c.Country like 'ALBANIA%' then 'AL'
		when c.Country like 'ALGERIA%' then 'DZ'
		when c.Country like 'ANGOLA%' then 'AO'
		when c.Country like 'ARGENTI%' then 'AR'
		when c.Country like 'AUSTRAL%' then 'AU'
		when c.Country like 'AUSTRIA%' then 'AT'
		when c.Country like 'Azerbai%' then 'AZ'
		when c.Country like 'AZERBAI%' then 'AZ'
		when c.Country like 'BAHAMAS%' then 'BS'
		when c.Country like 'Bahrain%' then 'BH'
		when c.Country like 'BANGLAD%' then 'BD'
		when c.Country like 'BELARUS%' then 'BY'
		when c.Country like 'BELGIUM%' then 'BE'
		when c.Country like 'BRAZIL%' then 'BR'
		when c.Country like 'CANADA%' then 'CA'
		when c.Country like 'CHILE%' then 'CL'
		when c.Country like 'CHINA%' then 'CN'
		when c.Country like 'COLOMBI%' then ''
		when c.Country like 'CYPRUS%' then 'CY'
		when c.Country like 'DENMARK%' then 'DK'
		when c.Country like 'DJIBOUT%' then 'DJ'
		when c.Country like 'EGYPT%' then 'EG'
		when c.Country like 'England%' then 'GB'
		when c.Country like 'FIJI%' then 'FJ'
		when c.Country like 'FRANCE%' then 'FR'
		when c.Country like 'GERMANY%' then 'DE'
		when c.Country like 'GHANA%' then 'GH'
		when c.Country like 'GIBRALT%' then ''
		when c.Country like 'GREECE%' then 'GR'
		when c.Country like 'HONG%' then 'HK'
		when c.Country like 'HUNGARY%' then 'HU'
		when c.Country like 'India%' then 'IN'
		when c.Country like 'INDONES%' then 'ID'
		when c.Country like 'IRELAND%' then 'IE'
		when c.Country like 'ITALY%' then 'IT'
		when c.Country like 'JAPAN%' then 'JP'
		when c.Country like 'JORDAN%' then 'JO'
		when c.Country like 'KAZAKHS%' then 'KZ'
		when c.Country like 'Kenya%' then 'KE'
		when c.Country like 'KOREA%' then 'KR'
		when c.Country like 'Kuwait%' then 'KW'
		when c.Country like 'LEBANON%' then 'LB'
		when c.Country like 'Libya%' then ''
		when c.Country like 'LITHUAN%' then 'LT'
		when c.Country like 'LUXEMBO%' then 'LU'
		when c.Country like 'MACAO%' then 'HK'
		when c.Country like 'MALAYSI%' then 'MY'
		when c.Country like 'MAURITI%' then 'MU'
		when c.Country like 'MEXICO%' then 'MX'
		when c.Country like 'MONACO%' then 'MC'
		when c.Country like 'MOROCCO%' then 'MA'
		when c.Country like 'NETHERL%' then 'NL'
		when c.Country like 'NEW%' then 'NZ'
		when c.Country like 'NIGERIA%' then 'NG'
		when c.Country like 'NORWAY%' then 'NO'
		when c.Country like 'Oman%' then 'OM'
		when c.Country like 'PAKISTA%' then 'PK'
		when c.Country like 'PHILIPP%' then 'PH'
		when c.Country like 'POLAND%' then 'PL'
		when c.Country like 'PORTUGA%' then 'PT'
		when c.Country like 'QATAR%' then 'QA'
		when c.Country like 'ROMANIA%' then 'RO'
		when c.Country like 'RUSSIAN%' then 'RU'
		when c.Country like 'SAUDI%' then 'SA'
		when c.Country like 'SERBIA%' then ''
		when c.Country like 'Singapo%' then 'SG'
		when c.Country like 'SLOVAKI%' then 'SK'
		when c.Country like 'SOUTH%' then 'ZA'
		when c.Country like 'SPAIN%' then 'ES'
		when c.Country like 'SRI%' then 'LK'
		when c.Country like 'SUDAN%' then 'SD'
		when c.Country like 'SWEDEN%' then 'SE'
		when c.Country like 'SWITZER%' then 'CH'
		when c.Country like 'SYRIAN%' then ''
		when c.Country like 'THAILAN%' then 'TH'
		when c.Country like 'TUNISIA%' then 'TN'
		when c.Country like 'TURKEY%' then 'TR'
		when c.Country like 'UAE%' then 'AE'
		when c.Country like 'UKRAINE%' then 'UA'
		when c.Country like 'UK%' then 'GB'
		when c.Country like 'VENEZUE%' then 'VE'
		when c.Country like '%UNITED%ARAB%' then 'AE'
		when c.Country like '%UAE%' then 'AE'
		when c.Country like '%U.A.E%' then 'AE'
		when c.Country like '%UNITED%KINGDOM%' then 'GB'
		when c.Country like '%UNITED%STATES%' then 'US'
		when c.Country like '%US%' then 'US'
end as 'candidate-country'                
, case 
		when c.ResidentialLocation like 'Afghani%' then 'AF'
		when c.ResidentialLocation like 'Africa%' then 'ZA'
		when c.ResidentialLocation like 'Albania%' then 'AL'
		when c.ResidentialLocation like 'Algeria%' then 'DZ'
		when c.ResidentialLocation like 'Angola%' then 'AO'
		when c.ResidentialLocation like 'Argenti%' then 'AR'
		when c.ResidentialLocation like 'Austral%' then 'AU'
		when c.ResidentialLocation like 'Austria%' then 'AT'
		when c.ResidentialLocation like 'Azerbai%' then 'AZ'
		when c.ResidentialLocation like 'Bahamas%' then 'BS'
		when c.ResidentialLocation like 'Bahrain%' then 'BH'
		when c.ResidentialLocation like 'Banglad%' then 'BD'
		when c.ResidentialLocation like 'Barnsle%' then ''
		when c.ResidentialLocation like 'Belgium%' then 'BE'
		when c.ResidentialLocation like 'Bosnia%' then 'BA'
		when c.ResidentialLocation like 'Brazil%' then 'BR'
		when c.ResidentialLocation like 'bristol%' then 'GB'
		when c.ResidentialLocation like 'Bulgari%' then 'BG'
		when c.ResidentialLocation like 'Cairo%' then 'EG'
		when c.ResidentialLocation like 'Cambodi%' then 'KH'
		when c.ResidentialLocation like 'Canada%' then 'CA'
		when c.ResidentialLocation like 'Cayman%' then 'KY'
		when c.ResidentialLocation like 'Chile%' then 'CL'
		when c.ResidentialLocation like 'China%' then 'CN'
		when c.ResidentialLocation like 'Colombi%' then ''
		when c.ResidentialLocation like 'Croatia%' then 'HR'
		when c.ResidentialLocation like 'Cyprus%' then 'CY'
		when c.ResidentialLocation like 'Denmark%' then 'DK'
		when c.ResidentialLocation like 'Doha%' then ''
		when c.ResidentialLocation like 'Dubai%' then 'AE'
		when c.ResidentialLocation like 'Ecuador%' then 'EC'
		when c.ResidentialLocation like 'Egypt%' then 'EG'
		when c.ResidentialLocation like 'Finland%' then 'FI'
		when c.ResidentialLocation like 'France%' then 'FR'
		when c.ResidentialLocation like 'Germany%' then 'DE'
		when c.ResidentialLocation like 'Ghana%' then 'GH'
		when c.ResidentialLocation like 'Greece%' then 'GR'
		when c.ResidentialLocation like 'Hong%' then 'HK'
		when c.ResidentialLocation like 'Hungary%' then 'HU'
		when c.ResidentialLocation like 'India%' then 'IN'
		when c.ResidentialLocation like 'Indones%' then 'ID'
		when c.ResidentialLocation like 'Iran%' then 'IR'
		when c.ResidentialLocation like 'Iraq%' then 'IQ'
		when c.ResidentialLocation like 'Ireland%' then 'IE'
		when c.ResidentialLocation like 'Israel%' then 'IL'
		when c.ResidentialLocation like 'Italy%' then 'IT'
		when c.ResidentialLocation like 'Jamaica%' then 'JM'
		when c.ResidentialLocation like 'Japan%' then 'JP'
		when c.ResidentialLocation like 'Jordan%' then 'JO'
		when c.ResidentialLocation like 'Kazachs%' then ''
		when c.ResidentialLocation like 'kent%' then 'UZ'
		when c.ResidentialLocation like 'Kenya%' then 'KE'
		when c.ResidentialLocation like 'Korea%' then 'KR'
		when c.ResidentialLocation like 'Kuwait%' then 'KW'
		when c.ResidentialLocation like 'Lebanon%' then 'LB'
		when c.ResidentialLocation like 'Leeds%' then 'GB'
		when c.ResidentialLocation like 'Libya%' then ''
		when c.ResidentialLocation like 'london%' then 'GB'
		when c.ResidentialLocation like 'Macau%' then 'MO'
		when c.ResidentialLocation like 'Malaysi%' then 'MY'
		when c.ResidentialLocation like 'Malta%' then 'MT'
		when c.ResidentialLocation like 'Manches%' then 'GB'
		when c.ResidentialLocation like 'Merseys%' then ''
		when c.ResidentialLocation like 'Mexico%' then 'MX'
		when c.ResidentialLocation like 'Milan%' then 'IT'
		when c.ResidentialLocation like 'Moldova%' then 'MD'
		when c.ResidentialLocation like 'Montser%' then ''
		when c.ResidentialLocation like 'Morocco%' then 'MA'
		when c.ResidentialLocation like 'Nepal%' then 'NP'
		when c.ResidentialLocation like 'Netherl%' then 'NL'
		when c.ResidentialLocation like 'Nigeria%' then 'NG'
		when c.ResidentialLocation like 'Northam%' then 'GB'
		when c.ResidentialLocation like 'Norway%' then 'NO'
		when c.ResidentialLocation like 'Oman%' then 'OM'
		when c.ResidentialLocation like 'Pakista%' then 'PK'
		when c.ResidentialLocation like 'Philipp%' then 'PH'
		when c.ResidentialLocation like 'Poland%' then 'PL'
		when c.ResidentialLocation like 'Portuga%' then 'PT'
		when c.ResidentialLocation like 'Qatar%' then 'QA'
		when c.ResidentialLocation like 'Riyadh%' then 'SA'
		when c.ResidentialLocation like 'Romania%' then 'RO'
		when c.ResidentialLocation like 'Russian%' then 'RU'
		when c.ResidentialLocation like 'Saudi%' then 'SA'
		when c.ResidentialLocation like 'Senegal%' then 'SN'
		when c.ResidentialLocation like 'Singapo%' then 'SG'
		when c.ResidentialLocation like 'Spain%' then 'ES'
		when c.ResidentialLocation like 'Sri%' then 'LK'
		when c.ResidentialLocation like 'Sudan%' then 'SD'
		when c.ResidentialLocation like 'Sweden%' then 'SE'
		when c.ResidentialLocation like 'Switzer%' then 'CH'
		when c.ResidentialLocation like 'Sydney%' then 'AU'
		when c.ResidentialLocation like 'Syria%' then ''
		when c.ResidentialLocation like 'Tadjiki%' then ''
		when c.ResidentialLocation like 'TBC%' then ''
		when c.ResidentialLocation like 'Thailan%' then 'TH'
		when c.ResidentialLocation like 'Tokyo%' then 'JP'
		when c.ResidentialLocation like 'Tonga%' then ''
		when c.ResidentialLocation like 'Tunisia%' then 'TN'
		when c.ResidentialLocation like 'Turkey%' then 'TR'
		when c.ResidentialLocation like 'UAE%' then 'AE'
		when c.ResidentialLocation like 'Uganda%' then 'UG'
		when c.ResidentialLocation like 'Ukraine%' then 'UA'
		when c.ResidentialLocation like 'Uzbekis%' then 'UZ'
		when c.ResidentialLocation like 'Venezue%' then 'VE'
		when c.ResidentialLocation like 'Vietnam%' then 'VN'
		when c.ResidentialLocation like 'worcest%' then ''
		when c.ResidentialLocation like 'Yemen%' then ''
		when c.ResidentialLocation like 'Yugosla%' then ''
		when c.ResidentialLocation like 'Zealand%' then 'NZ'
		when c.ResidentialLocation like '%UNITED%ARAB%' then 'AE'
		when c.ResidentialLocation like '%UAE%' then 'AE'
		when c.ResidentialLocation like '%U.A.E%' then 'AE'
		when c.ResidentialLocation like '%UNITED%KINGDOM%' then 'GB'
		when c.ResidentialLocation like '%UNITED%STATES%' then 'US'
		when c.ResidentialLocation like '%US%' then 'US'
end as 'canddiate-citizenship'
*/
       , exp.content as 'candidate-workhistory'
       , edu.content as 'candidate-education'
       , a.filename as 'candidate-resume'
-- select * -- select distinct  C.gender
from Candidates c 
left join users u on u.userid = c.CandidateOwnerID
left join ed on ed.ID = c.candidateID
left join e2 on e2.ID = c.candidateID
left join (select userid, FirstName, LastName, email from users) u1 on u1.userid = c.CreatedBy
left join attachment a on a.ParentId = c.CandidateId
left join exp on c.candidateID = exp.LEADID
left join edu on c.candidateID = edu.LEADID
--where ed.email = 'sheik_engg786@yahoo.com'




----
----------

with comment as (
        select
                   j.ParentID --, j.CreatedTime
                 --, CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 --, CONVERT(DATETIME,replace(convert(varchar(19),j.CreatedTime),'',''), 103) as 'comment_timestamp|insert_timestamp'
                 , CONVERT(DATETIME,replace(convert(varchar(19),j.CreatedTime),'',''), 110) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Note Owner: ' + NULLIF(u1.email, '') + char(10), '')
                                + Coalesce('Note Type: ' + NULLIF(j.NoteType, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u2.email, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select top 100 * 
        from Notes J
        left join (select * from users) u1 on u1.userid = j.NoteOwnerId
        left join (select * from users) u2 on u2.userid = j.CreatedBy
        --left join Contacts c on c.ContactID = j.ParentID where c.ContactID is not null
/*UNION ALL
        select
                   j.EntityId
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Time: ' + NULLIF(j.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Subject: ' + NULLIF(j.Subject, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Emails J
        ---left join Contacts c on c.ContactID = j.EntityId where c.ContactID is not null */
UNION ALL
        select
                 --i.JobOpeningId
                   i.CandidateId
                 --, CONVERT(datetime, replace(convert(varchar(50),i.CreatedTime),'',''),103) as 'comment_timestamp|insert_timestamp'
                 , CONVERT(datetime, replace(convert(varchar(50),i.CreatedTime),'',''),110) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   'INTERVIEW NOTES:' + char(10)
                                + Coalesce('Interview Name: ' + NULLIF(i.InterviewName, '') + char(10), '')
                                + Coalesce('Location: ' + NULLIF(i.Location, '') + char(10), '')
                                + Coalesce('Company : ' + NULLIF(j1.ClientName, '') + char(10), '') --i.ClientId
                                + Coalesce('Type: ' + NULLIF(i.Type, '') + char(10), '')
                                + Coalesce('Job Name: ' + NULLIF(j2.PostingTitle, '') + char(10), '') --i.JobOpeningId
                                + Coalesce('Interviewer: ' + NULLIF(u2.email, '') + char(10), '') --i.Interviewer
                                + Coalesce('Last Activity Time: ' + NULLIF(i.LastActivityTime, '') + char(10), '')
                                + Coalesce('From: ' + NULLIF(i.From_, '') + char(10), '')
                                + Coalesce('To: ' + NULLIF(i.To_, '') + char(10), '')
                                + Coalesce('Interview Status: ' + NULLIF(i.InterviewStatus, '') + char(10), '')
                                + Coalesce('Schedule Comments: ' + NULLIF(i.ScheduleComments, '') + char(10), '')
                                + Coalesce('Is Attachment Present: ' + NULLIF(i.IsAttachmentPresent, '') + char(10), '')
                                /*
                                + Coalesce('Consultant: ' + NULLIF(u1.email, '') + char(10), '') --i.InterviewOwnerId
                                + Coalesce('Created Date Time: ' + NULLIF(i.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Date Time: ' + NULLIF(i.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u3.email, '') + char(10), '') --i.CreatedBy
                                + Coalesce('Modified By: ' + NULLIF(u4.email, '') + char(10), '') --i.ModifiedBy */
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Interviews i
        left join (select * from Users) u1 on u1.UserID = i.InterviewOwnerId
        left join (select * from Users) u2 on u2.UserID = i.InterviewName
        left join (select * from Users) u3 on u3.UserID = i.CreatedBy
        left join (select * from Users) u4 on u4.UserID = i.ModifiedBy
        left join (select ClientId,ClientName from Clients) j1 on j1.ClientId = i.ClientId --where j1.ClientId is not null
        left join (select JobOpeningId,PostingTitle from JobOpenings) j2 on j2.JobOpeningID = i.JobOpeningId --where j2.JobOpeningId is not null
        left join (select CandidateId,FullName from Candidates) j3 on j3.CandidateId = i.CandidateId where j3.CandidateId is not null
/*UNION ALL select eventid, getdate(), title from events
UNION ALL select taskid, getdate(), subject from tasks */
)
--select count(*) from comment where comment.comment is not null --8157
select
        c.CandidateId as 'externalId' , c.firstName, c.lastName
        , cast('-10' as int) as 'user_account_id'
        --, CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) 'comment_timestamp|insert_timestamp'
        , [comment_timestamp|insert_timestamp]
        , comment.comment  as 'comment_body'
from Candidates c
left join comment on comment.ParentID = c.CandidateId 
where c.CandidateId is not null and comment.comment is not null



/*
with t0 (CandidateId,firstName,Role) as (
        SELECT    CandidateId,firstName
                , Split.a.value('.', 'VARCHAR(max)') AS String
        FROM ( SELECT     CandidateId, firstName
                        , CAST ('<M>' + REPLACE(Role,';','</M><M>') + '</M>' AS XML) AS Data 
               FROM Candidates ) AS A CROSS APPLY Data.nodes ('/M') AS Split(a)
        )
select distinct Role from t0


with t as (
        select 
                c.CandidateId as 'additional_id'
                --, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
                --, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',C.CandidateId) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
                        , 'add_cand_info' as additional_type
                        , convert(int,1006) as form_id
                        , convert(int,1018) as field_id
                , c.Role
                , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(c.Role
                      ,'Domestic Couple','')
                      ,'House Manager','')
                      ,'Private House - Single Gardener','18')
                      ,'Yacht - Chief Stewardess/Purser','20')
                      ,'Private House - Single HK/Cook','17')
                      ,'Yacht - Beautician/Massuese','22')
                      ,'Private Household - Chef','15')
                      ,'Private House - Manager','13')
                      ,'Yacht - Officer/Captain','25')
                      ,'Private House - General','19')
                      ,'Private House - Couple','16')
                      ,'Private House - Butler','14')
                      ,'Yacht - Stewardess','21')
                      ,'Yacht - Cook/Stew','30')
                      ,'Yacht - Engineer','27')
                      ,'Yacht - Deckhand','23')
                      ,'Yacht - Steward','29')
                      ,'Chalet Manager','4')
                      ,'Yacht - Couple','28')
                      ,'Resort Manager','3')
                      ,'Yacht - Other','26')
                      ,'Office Based','11')
                      ,'Yacht - Chef','24')
                      ,'Housekeeper','9')
                      ,'Maintenance','8')
                      ,'Chauffeur','7')
                      ,'Childcare','10')
                      ,'Couple','6')
                      ,'Other','12')
                      ,'Cook','2')
                      ,'Host','5')
                      ,'Chef','1')
                      ,';',',') as field_value
        -- select distinct Role
        from Candidates c
        where LookingFor <> '' )
--select distinct field_value from t
select count(*) from t where field_value <> ''


select 
        c.CandidateId as 'additional_id'
	, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'FirstName' else ltrim(replace(C.firstName,'?','')) end as 'contact-firstName'
	, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('LastName-',C.CandidateId) else ltrim(replace(C.lastName,'?','')) end as 'contact-Lastname'
                , 'add_cand_info' as additional_type
                , convert(int,1006) as form_id
                , convert(int,1019) as field_id
        , c.LookingFor
        ,replace(replace(replace(replace(replace(replace(replace(replace(c.LookingFor,
               'Winter','1'
               ),'Summer','2'
               ),'Permanent','3'
               ),'Temporary','4'
               ),'Private Household','5'
               ),'Yacht','6'
               ),'Middle East','7'),';',',') as field_value
-- select distinct LookingFor
from Candidates c
where LookingFor <> ''

*/