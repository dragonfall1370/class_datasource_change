
with
-- EMAIL
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID, email1, email2) as (
		select pe.ID, email as email1, we.email2 as email2 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		--left join (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 ) */
, e1 as (select ID, email from mail4 where rn = 1)
, ed (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
, oe3 as (select ID, email from mail4 where rn = 3)
, oe4 as (select ID, email from mail4 where rn = 4)
--, oe as (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email FROM mail4 AS a where rn > 2 GROUP BY a.ID)
--select top 100 * from oe4

-- OWNER
, owner as (select distinct CA.recruiterUserID, concat(uc.firstname,' ', uc.lastname) as fullname, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

-- SkillName: split by separate rows by comma, then combine them into SkillName
, SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SkillName0 as a where a.skillID <> '' GROUP BY a.userId)

-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
, BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS URLList FROM BusinessSector0 as a where a.businessSectorID <> '' GROUP BY a.userId)
--, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID )
-- select distinct BusinessSector from BusinessSector0

-- CATEGORY - VC FE info
, CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID WHERE CateSplit.categoryid <> '' and Userid = a.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM CateSplit as a where a.categoryid <> '' GROUP BY a.Userid)
--, CName(Userid, Name) as (SELECT Userid, CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID )
-- select distinct Name from CName

-- SPECIALTY - VC SFE info
, SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate) t CROSS APPLY x.nodes('/M') as Split(a) )
, SpeName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + VS.name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '' and Userid = b.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SpecSplit as b where b.specialtyid <> '' GROUP BY b.Userid)

-- ADMISSION
, AdmissionRows(userId, CombinedText) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)

-- NOTE
, note as (
	SELECT CA.userID
		 , Stuff(Coalesce('Candidate ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')
                        + Coalesce('Other email: ' + NULLIF(cast(oe3.email as varchar(max)), '') + char(10), '')
                        + Coalesce('Notice Period: ' + NULLIF(cast(CA.customText11 as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Locations: ' + NULLIF(cast(CA.desiredLocations as varchar(max)), '') + char(10), '')
                        + Coalesce('Phone 2: ' + NULLIF(cast(CA.phone2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Limited Company Name: ' + NULLIF(cast(CA.customText3 as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company LTD Acccount #: ' + NULLIF(cast(CA.customText4 as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company LTD Phone: ' + NULLIF(cast(CA.customText5 as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company Address 1: ' + NULLIF(cast(CA.secondaryAddress1 as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company Address 2: ' + NULLIF(cast(CA.secondaryAddress2 as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company City: ' + NULLIF(cast(CA.secondaryCity as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company Post Code: ' + NULLIF(cast(CA.secondaryZip as varchar(max)), '') + char(10), '')
                        + Coalesce('LTD Company Country: ' + NULLIF(cast(t.country as varchar(max)), '') + char(10), '') --CA.secondaryCountryID
                        + Coalesce('General Comments: ' + NULLIF(cast(CA.comments as varchar(max)), '') + char(10), '')
                        /*
                        + Coalesce('Status: ' + NULLIF(cast(CA.status as varchar(max)), '') + char(10), '')                     
                        + Coalesce('Employment Preference: ' + NULLIF(cast(CA.employmentPreference as varchar(max)), '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(cast(CA.source as varchar(max)), '') + char(10), '')
                        + Coalesce('Recruiter: ' + NULLIF(cast(owner.fullname as varchar(max)), '') + char(10), '')
                        + Coalesce('Pay Rate: ' + NULLIF(cast(CA.hourlyRateLow as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Salary: ' + NULLIF(cast(CA.salary as varchar(max)), '') + char(10), '')
                        --+ Coalesce('CV Text: ' + NULLIF(cast(CA.description as varchar(max)), '') + char(10), '')                        
                        + Coalesce('Date Available: ' + NULLIF(cast(CA.dateAvailable as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Opted In - SMS Messages: ' + NULLIF(cast(CA.smsOptIn as varchar(max)), '') + char(10), '')
                        + Coalesce('Date Registered: ' + NULLIF(convert(varchar(10),CA.dateAdded,120), '') + char(10), '')
                        + Coalesce('Reffered by: ' + NULLIF(cast(CA.referredBy as varchar(max)), '') + char(10), '')
                        + Coalesce('Skills: ' + NULLIF(cast(SN.SkillName as varchar(max)), '') + char(10), '')
                        + Coalesce('Practice Area / Category: ' + NULLIF(cast(CName.Name as varchar(max)), '') + char(10), '')
                        + Coalesce('Specialty: ' + NULLIF(cast(CA.specialtyIDList as varchar(max)), '') + char(10), '')
                        + Coalesce('Business Sector: ' + NULLIF(cast(BS.BusinessSector as varchar(max)), '') + char(10), '')
                        + Coalesce('ID Number: ' + NULLIF(cast(CA.ssn as varchar(max)), '') + char(10), '')
                        + Coalesce('AA/EE: ' + NULLIF(cast(CA.customText5 as varchar(max)), '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(cast(CA.source as varchar(max)), '') + char(10), '')
                        + Coalesce('Nationality: ' + NULLIF(cast(CA.customText6 as varchar(max)), '') + char(10), '')
                        + Coalesce('Referral Type: ' + NULLIF(cast(CA.customtext2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Region: ' + NULLIF(cast(CA.customText7 as varchar(max)), '') + char(10), '')
                        + Coalesce('Currency: ' + NULLIF(cast(CA.customText12 as varchar(max)), '') + char(10), '')
                        + Coalesce('Pay Unit: ' + NULLIF(cast(CA.customText13 as varchar(max)), '') + char(10), '')
                        + Coalesce('Salary Notes: ' + NULLIF(cast(CA.customTextBlock1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Notice Period: ' + NULLIF(cast(CA.customText11 as varchar(max)), '') + char(10), '')
                        + Coalesce('Languages: ' + NULLIF(cast(CA.customComponent1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Date of Birth: ' + NULLIF(cast(CA.customFloat1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Description: ' + NULLIF(cast(CA.description as varchar(max)), '') + char(10), '')
                        + Coalesce('University (U): ' + NULLIF(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + Coalesce('If other uni (U): ' + NULLIF(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        + Coalesce('Primary Field of Study (U): ' + NULLIF(cast(CA.customText13 as varchar(max)), '') + char(10), '')
                        + Coalesce('Classification (U): ' + NULLIF(cast(CA.customText15 as varchar(max)), '') + char(10), '')
                        + Coalesce('Graduation Year (U): ' + NULLIF(cast(CA.customText16 as varchar(max)), '') + char(10), '')
                        + Coalesce('University (P): ' + NULLIF(cast(CA.customText17 as varchar(max)), '') + char(10), '')
                        + Coalesce('If other uni (P): ' + NULLIF(cast(CA.customText6 as varchar(max)), '') + char(10), '')
                        + Coalesce('Qualification Type (P): ' + NULLIF(cast(CA.customText18 as varchar(max)), '') + char(10), '')
                        + Coalesce('Primary Field of Study (P): ' + NULLIF(cast(CA.customText19 as varchar(max)), '') + char(10), '')
                        + Coalesce('Classification (P): ' + NULLIF(cast(CA.customText10 as varchar(max)), '') + char(10), '')
                        + Coalesce('Graduation Year (P): ' + NULLIF(cast(CA.customFloat3 as varchar(max)), '') + char(10), '')
                        + Coalesce('Bar admission: ' + NULLIF(cast(AD.Admission as varchar(max)), '') + char(10), '')
                        + Coalesce('General Work Function: ' + NULLIF(cast(CA.customText2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Summary: ' + NULLIF(cast(summary.summary as varchar(max)), '') + char(10), '') */
                        , 1, 0, '') as note
	-- select count(*) -- select  comments
	from bullhorn1.Candidate CA
	left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
	left join (select * from tmp_country) t on CA.secondaryCountryID = t.code
	left join SkillName SN on CA.userID = SN.userId
	left join BusinessSector BS on CA.userID = BS.userId
	left outer join admission AD on CA.userID = AD.Userid
        left outer join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
        left outer join CName on CA.userID = CName.Userid
        left outer join SpeName on CA.userID = SpeName.Userid
        --left join mail5 on CA.userID = mail5.ID
        left join oe3 on CA.userID = oe3.ID
        --left join summary on CA.userID = summary.CandidateI
        left join (select userID, name from bullhorn1.BH_UserContact) UC ON UC.userID = CA.referredByUserID
        left join owner on CA.recruiterUserID = owner.recruiterUserID
	where CA.isPrimaryOwner = 1 )
-- select count(*) from note
-- select * from note --where CA.referredByUserID is not null like '%Business Sector%'
-- select distinct CA.secondaryCountryID from bullhorn1.Candidate CA where secondaryCountryID is not null

-- COMMENT
, comment(Userid, comment) as (SELECT Userid, STUFF((SELECT char(10) + 'Date Added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max)) from [bullhorn1].[BH_UserComment] WHERE Userid = a.Userid order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM [bullhorn1].[BH_UserComment] AS a GROUP BY a.Userid )
, summary(candidateID,summary) as (SELECT candidateID, STUFF((SELECT coalesce(char(10) + 'Date Added: ' + convert(varchar,dateAdded,120) + ' || ' + 'Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), '') from bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS summary FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID)

-- DOCUMENT
, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF((SELECT ',' + name from bullhorn1.View_CandidateFile WHERE candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID)

-- Files
, files(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + concat(candidateFileID, fileExtension) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID) --where a.type = 'Resume') ==> get all candidates files

-- EDUCATION
, EducationGroup as (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID)
, Education as (select EG.userID, UE.userEducationID, UE.school, convert(varchar(10),UE.graduationDate,110) as graduationDate, UE.degree, UE.major, UE.comments from EducationGroup EG left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)



	select --top 50
                  C.userID as '#userID'
		, case C.gender when 'M' then 'MR' when 'F' then 'MISS'	else '' end as 'candidate-title'
		, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
		, C.candidateID as 'candidate-externalId'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
		, C.middleName as 'candidate-middleName'
		, CONVERT(VARCHAR(10),C.dateOfBirth,120) as 'candidate-dob'
		--, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL' OR tc.abbreviation = 'ZR') THEN '' ELSE tc.abbreviation END as 'candidate-citizenship' , ethnicity --<<
		, case
                        when C.ethnicity like 'Afghan%' then 'AF'
                        when C.ethnicity like 'African%' then 'ZA'
                        when C.ethnicity like 'Afrikaa%' then 'ZA'
                        when C.ethnicity like 'Albania%' then 'AL'
                        when C.ethnicity like 'Algeria%' then 'DZ'
                        when C.ethnicity like 'America%' then 'US'
                        when C.ethnicity like 'Andorra%' then 'AD'
                        when C.ethnicity like 'Argenti%' then 'AR'
                        when C.ethnicity like 'Armenia%' then 'AM'
                        when C.ethnicity like 'Austral%' then 'AU'
                        when C.ethnicity like 'Austria%' then 'AT'
                        when C.ethnicity like 'Azerbai%' then 'AZ'
                        when C.ethnicity like 'Azeri%' then 'AZ'
                        when C.ethnicity like 'Bahamia%' then ''
                        when C.ethnicity like 'Bahrain%' then 'BH'
                        when C.ethnicity like 'Banglad%' then 'BD'
                        when C.ethnicity like 'Barbadi%' then 'BB'
                        when C.ethnicity like 'Batswan%' then 'BW'
                        when C.ethnicity like 'Belarus%' then 'BY'
                        when C.ethnicity like 'Belgian%' then 'BE'
                        when C.ethnicity like 'Benines%' then 'BJ'
                        when C.ethnicity like 'Bolivia%' then 'BO'
                        when C.ethnicity like 'Bosnian%' then ''
                        when C.ethnicity like 'Brazili%' then 'BR'
                        when C.ethnicity like 'British%' then 'GB'
                        when C.ethnicity like 'Bulgari%' then 'BG'
                        when C.ethnicity like 'Cambodi%' then 'KH'
                        when C.ethnicity like 'Cameroo%' then 'CM'
                        when C.ethnicity like 'Canadia%' then 'CA'
                        when C.ethnicity like 'CAR%' then ''
                        when C.ethnicity like 'Chilean%' then 'CL'
                        when C.ethnicity like 'Chinese%' then 'MO'
                        when C.ethnicity like 'Colombi%' then ''
                        when C.ethnicity like 'Congole%' then ''
                        when C.ethnicity like 'Costa%' then 'CR'
                        when C.ethnicity like 'Croatia%' then 'HR'
                        when C.ethnicity like 'Cypriot%' then 'CY'
                        when C.ethnicity like 'Czech%' then 'CZ'
                        when C.ethnicity like 'Danish%' then ''
                        when C.ethnicity like 'Djibout%' then 'DJ'
                        when C.ethnicity like 'Dominic%' then ''
                        when C.ethnicity like 'Dutch%' then 'NL'
                        when C.ethnicity like 'East%' then 'ZA'
                        when C.ethnicity like 'Ecuador%' then 'EC'
                        when C.ethnicity like 'Egyptia%' then 'EG'
                        when C.ethnicity like 'Emirati%' then 'AE'
                        when C.ethnicity like 'Eritrea%' then 'ER'
                        when C.ethnicity like 'Estonia%' then 'EE'
                        when C.ethnicity like 'Ethiopi%' then 'ET'
                        when C.ethnicity like 'Fijian%' then 'FJ'
                        when C.ethnicity like 'Filipin%' then 'PH'
                        when C.ethnicity like 'Finnish%' then ''
                        when C.ethnicity like 'French%' then 'FR'
                        when C.ethnicity like 'Gabones%' then 'GA'
                        when C.ethnicity like 'German%' then 'DE'
                        when C.ethnicity like 'Ghanaia%' then 'GH'
                        when C.ethnicity like 'Greek%' then 'GR'
                        when C.ethnicity like 'Grenadi%' then 'GD'
                        when C.ethnicity like 'Guatema%' then 'GT'
                        when C.ethnicity like 'Guinea%' then 'GW'
                        when C.ethnicity like 'Hondura%' then 'HN'
                        when C.ethnicity like 'Hungari%' then 'HU'
                        when C.ethnicity like 'Iceland%' then 'IS'
                        when C.ethnicity like 'Indian%' then 'IN'
                        when C.ethnicity like 'Indones%' then 'ID'
                        when C.ethnicity like 'Iranian%' then 'IR'
                        when C.ethnicity like 'Iraqi%' then 'IQ'
                        when C.ethnicity like 'Irish%' then 'IE'
                        when C.ethnicity like 'Israeli%' then 'IL'
                        when C.ethnicity like 'Italian%' then 'IT'
                        when C.ethnicity like 'Ivoiria%' then ''
                        when C.ethnicity like 'Japanes%' then 'JP'
                        when C.ethnicity like 'Jordani%' then 'JO'
                        when C.ethnicity like 'Kazakh%' then 'KZ'
                        when C.ethnicity like 'Kenyan%' then 'KE'
                        when C.ethnicity like 'Korean%' then 'KR'
                        when C.ethnicity like 'Kuwaiti%' then 'KW'
                        when C.ethnicity like 'Kyrgyz%' then 'KG'
                        when C.ethnicity like 'Latvian%' then 'LV'
                        when C.ethnicity like 'Lebanes%' then 'LB'
                        when C.ethnicity like 'Libyan%' then ''
                        when C.ethnicity like 'Lithuan%' then 'LT'
                        when C.ethnicity like 'Luxembo%' then 'LU'
                        when C.ethnicity like 'Macedon%' then 'MK'
                        when C.ethnicity like 'Malagas%' then 'MG'
                        when C.ethnicity like 'Malaysi%' then 'MY'
                        when C.ethnicity like 'Malian%' then 'ML'
                        when C.ethnicity like 'Maltese%' then 'MT'
                        when C.ethnicity like 'Maurita%' then 'MR'
                        when C.ethnicity like 'Mauriti%' then 'MU'
                        when C.ethnicity like 'Mexican%' then 'MX'
                        when C.ethnicity like 'Moldova%' then 'MD'
                        when C.ethnicity like 'Montene%' then ''
                        when C.ethnicity like 'Morocca%' then 'MA'
                        when C.ethnicity like 'Namibia%' then 'NA'
                        when C.ethnicity like 'Nigeria%' then 'NG'
                        when C.ethnicity like 'Norwegi%' then 'NO'
                        when C.ethnicity like 'Omani%' then 'OM RO'
                        when C.ethnicity like 'Pakista%' then 'PK'
                        when C.ethnicity like 'Panaman%' then 'PA'
                        when C.ethnicity like 'Peruvia%' then 'PE'
                        when C.ethnicity like 'Polish%' then 'PL'
                        when C.ethnicity like 'Portuge%' then 'PT'
                        when C.ethnicity like 'Qatari%' then 'QA'
                        when C.ethnicity like 'Romania%' then 'RO'
                        when C.ethnicity like 'Russian%' then 'RU'
                        when C.ethnicity like 'Saudi%' then 'SA'
                        when C.ethnicity like 'Senegal%' then 'SN'
                        when C.ethnicity like 'Serb%' then ''
                        when C.ethnicity like 'Singapo%' then 'SG'
                        when C.ethnicity like 'Slovak%' then 'SK'
                        when C.ethnicity like 'Slovene%' then 'SI'
                        when C.ethnicity like 'Slovoki%' then ''
                        when C.ethnicity like 'Spanish%' then 'ES'
                        when C.ethnicity like 'Sri%' then 'LK'
                        when C.ethnicity like 'Sudanes%' then 'SD'
                        when C.ethnicity like 'Swedish%' then 'CH'
                        when C.ethnicity like 'Swiss%' then 'CH'
                        when C.ethnicity like 'Syrian%' then ''
                        when C.ethnicity like 'Taiwane%' then 'TW'
                        when C.ethnicity like 'Thai%' then 'TH'
                        when C.ethnicity like 'Tibetan%' then ''
                        when C.ethnicity like 'Togoles%' then 'TG'
                        when C.ethnicity like 'Trinida%' then 'TT'
                        when C.ethnicity like 'Tunisia%' then 'TN'
                        when C.ethnicity like 'Turkish%' then ''
                        when C.ethnicity like 'Tuvalua%' then ''
                        when C.ethnicity like 'Ukrania%' then 'UA'
                        when C.ethnicity like 'Undiscl%' then ''
                        when C.ethnicity like 'Unknown%' then ''
                        when C.ethnicity like 'Uzbek%' then 'UZ'
                        when C.ethnicity like 'Venezue%' then 'VE'
                        when C.ethnicity like 'Vietnam%' then 'VN'
                        when C.ethnicity like 'Zealand%' then 'NZ'
                        when C.ethnicity like 'Zimbabw%' then 'ZW'
                        when C.ethnicity like '%UNITED%ARAB%' then 'AE'
                        when C.ethnicity like '%UAE%' then 'AE'
                        when C.ethnicity like '%U.A.E%' then 'AE'
                        when C.ethnicity like '%UNITED%KINGDOM%' then 'GB'
                        when C.ethnicity like '%UNITED%STATES%' then 'US'
                        when C.ethnicity like '%US%' then 'US'
		end as 'candidate-citizenship'
		, iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), iif(e1.email = '' or e1.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemail.com'),e1.email) ) as 'candidate-email'
		, e2.email as 'candidate-workEmail'
		, C.mobile as 'candidate-phone'
		--, C.mobile as 'candidate-mobile'
		, C.phone as 'candidate-homePhone'	
		, C.workPhone as 'candidate-workPhone'
		, c.status as 'candidate-status' --<<
		, C.hourlyRateLow as '(Pay Rate) Contract Rate' --<< desired_contract_rate
		--, C.employmentPreference as 'candidate-jobTypes'  --, 'PERMANENT' as 'candidate-jobTypes' --<<
                /*, case C.employmentPreference
                        when 'Consultancy' then 'PROJECT CONSULTANCY'
                        when 'Consultancy,Third Party,Contract or Perm' then 'PROJECT CONSULTANCY,CONTRACT,PERMANENT'
                        when 'Contract Or Perm' then 'CONTRACT,PERMANENT'
                        when 'Contract or Perm,Contractor' then 'CONTRACT,PERMANENT'
                        when 'Contractor' then 'CONTRACT'
                        when 'Contractor,Contract or Perm' then 'CONTRACT,PERMANENT'
                        when 'Permanent' then 'PERMANENT'
                        when 'Permanent,Contractor' then 'PERMANENT,CONTRACT'
                        when 'Permanent,Temporary' then 'PERMANENT,TEMPORARY'
                        when 'Temporary' then 'TEMPORARY'
                        when 'Temporary,Permanent' then 'TEMPORARY,PERMANENT'
                        when 'Third Party' then ''
                        when 'Unknown' then '' */
                , case C.employmentPreference
                        when 'Consultancy' then '[{"desiredJobTypeId":"5"}]'
                        when 'Consultancy,Third Party,Contract or Perm' then '[{"desiredJobTypeId":"5"},{"desiredJobTypeId":"2"},{"desiredJobTypeId":"1"}]'
                        when 'Contract Or Perm' then '[{"desiredJobTypeId":"2"},{"desiredJobTypeId":"1"}]'
                        when 'Contract or Perm,Contractor' then '[{"desiredJobTypeId":"2"},{"desiredJobTypeId":"1"}]'
                        when 'Contractor' then '[{"desiredJobTypeId":"2"}]'
                        when 'Contractor,Contract or Perm' then '[{"desiredJobTypeId":"2"},{"desiredJobTypeId":"1"}]'
                        when 'Permanent' then '[{"desiredJobTypeId":"1"}]'
                        when 'Permanent,Contractor' then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
                        when 'Permanent,Temporary' then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"4"}]'
                        when 'Temporary' then '[{"desiredJobTypeId":"4"}]'
                        when 'Temporary,Permanent' then '[{"desiredJobTypeId":"4"},{"desiredJobTypeId":"1"}]'
                        when 'Third Party' then '[{"desiredJobTypeId":"5"}]'
                        when 'Unknown' then ''
		      end as 'candidate-jobTypes'                        
		, C.source as 'CANDIDATE-SOURCE' --<<
		, C.address1 as 'candidate-address'
		, C.city as 'candidate-city'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN '' ELSE tc.abbreviation END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		--, C.state as 'candiadte-state'
		, cast(C.salaryLow as int) as 'candidate-currentSalary'
		, cast(C.salary as int) as 'candidate-desiredSalary'
		, C.customText19 as 'Currency'
		--, Education.school as 'candidate-schoolName' 
		--, Education.graduationDate as 'candidate-graduationDate'
		--, Education.degree as 'candidate-degreeName'
		--, Education.major as '#candidate-major'
		--, SN.SkillName as 'candidate-skills'
		, C.companyName as 'candidate-company1'
		, C.occupation as 'candidate-jobTitle1'
		, C.companyName as 'candidate-employer1'
		--, C.recruiterUserID as '#recruiterUserID'
		, owner.email as 'candidate-owners'
		--, t4.finame as '#Candidate File'
		, files.ResumeId as 'candidate-resume'
		, note.note as 'candidate-note'
		--, c.referredByUserID, c.referredBy
		--, left(comment.comment,32760) as 'candidate-comments'
	-- select count (*) -- select top 50 hourlyRateLow -- select distinct status --ethnicity -- dateAvailable
	from bullhorn1.Candidate C --where c.userID in (76938, 100453, 120112) --where C.isPrimaryOwner = 1
	left join SkillName SN on C.userID = SN.userId
	left join tmp_country tc ON c.countryID = tc.code
	--left join tmp_country tc1 ON c.ethnicity = tc1.code -- 
	left join owner on C.recruiterUserID = owner.recruiterUserID
	left join e1 on C.userID = e1.ID
	left join e2 on C.userID = e2.ID
	left join ed on C.userID = ed.ID -- candidate-email-DUPLICATION
	left join Education on C.userID = Education.userID
	left join t4 on t4.candidateUserID = C.userID
	left join files on C.userID = files.candidateUserID
	--left join comment on C.userID = comment.Userid
	left join note on C.userID = note.Userid
	left join bullhorn1.BH_ClientCorporationBlacklist bl on bl.Userid = C.userID
	where C.isPrimaryOwner = 1
	--and c.candidateID = 300
	--and C.FirstName = 'Boian'
	--and e1.email = '' or e1.email is null --e1.email <> ''
	--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
	--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID


/*
select          C.candidateID, concat(C.FirstName,' ',C.LastName) as fullname
		--, C.hourlyRateLow as '(Pay Rate) Contract Rate' --<<Desired contract rate
		--, C.employmentPreference as 'candidate-jobTypes'  --, 'PERMANENT' as 'candidate-jobTypes' --<<
                , C.status as 'candidate-status' --<<
                , case C.status
                        when 'Active' then 1
                        when 'Archive' then 1
                        when 'DNC' then 1
                        when 'Former Contact' then 1
                        when 'Imported' then 1
                        when 'Inactive' then 1
                        when 'new Lead' then 1
                        when 'Passive' then 0
                        when 'Placed' then 1
                        when 'Private' then 1
                        when 'Unavailable' then 1
		   end as 'active'                 
		, C.source as 'candidate-source' --<<
		, case C.source
                        when 'Advertisement' then 29100
                        when 'Alsacreation' then 29101
                        when 'APEC' then 29102
                        when 'Auto Parsed' then 29095
                        when 'Broadbean' then 29089
                        when 'Candidate Email Parser' then 29103
                        when 'CarrerBuilder' then 29104
                        when 'Coder.com' then 29105
                        when 'Company ' then 29106
                        when 'Company Website' then 29107
                        when 'Consultant Website' then 29108
                        when 'CV Library' then 29109
                        when 'CWJobs' then 29110
                        when 'Data Import' then 29093
                        when 'Dice.com' then 29111
                        when 'Dova' then 29086
                        when 'DoYouBuzz' then 29112
                        when 'eFinancialCareers' then 29113
                        when 'Facebook' then 29114
                        when 'Freelance.de' then 29115
                        when 'FreelanceInfo' then 29116
                        when 'Freelancerde' then 29117
                        when 'Freelancermap' then 29118
                        when 'freelancermap.de' then 29119
                        when 'FreelancerRepublik' then 29120
                        when 'Github' then 29098
                        when 'GULP' then 29121
                        when 'Headhunt' then 29091
                        when 'HH' then 29122
                        when 'Indeed' then 29085
                        when 'Instant Job Board' then 29087
                        when 'It JobBoard' then 29123
                        when 'Jobs.ch' then 29124
                        when 'Jobserve' then 29125
                        when 'Jobserve Database' then 29126
                        when 'Jobsite' then 29127
                        when 'jobupch' then 29128
                        when 'LinkedIn' then 29096
                        when 'Linkedrecruiter' then 29129
                        when 'LogicMelon' then 29094
                        when 'Monster' then 29130
                        when 'Monster.com' then 29131
                        when 'Paper' then 29132
                        when 'Qapa' then 29133
                        when 'QX' then 29134
                        when 'Referral' then 29092
                        when 'Resume Parser' then 29135
                        when 'SourceBreaker' then 29136
                        when 'Stack Overflow' then 29137
                        when 'TalentSpa' then 29138
                        when 'TheITJobBoard' then 29139
                        when 'Twitter' then 29140
                        when 'Viadeo' then 29141
                        when 'Volcanic' then 29084
                        when 'Website' then 29142
                        when 'WeJob' then 29143
                        when 'Workday Gigs' then 29144
                        when 'Xing' then 29097
		end as 'source'                 
		, C.dateAvailable as 'Availability for work'
		--, coalesce(NULLIF(convert(varchar(19), dateAvailable, 120),''), CURRENT_TIMESTAMP) as 'dateAvailable'
		, coalesce(NULLIF(convert(varchar(19), dateAvailable, 120),''), null) as 'dateAvailable'
-- select distinct status ---- select distinct source ---- select distinct dateAvailable --
from bullhorn1.Candidate C where C.isPrimaryOwner = 1
