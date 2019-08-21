
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
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 as (select ID, email from mail4 where rn = 2)
, oe3 as (select ID, email from mail4 where rn = 3)
, oe4 as (select ID, email from mail4 where rn = 4)
--, oe as (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email FROM mail4 AS a where rn > 2 GROUP BY a.ID)
--select top 100 * from ed where email like '%waiyin@alphasearch%'

-- OWNER
, owner as (select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

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
		 , Stuff( Coalesce('Candidate ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')
--+ Coalesce('Preferred Name: ' + NULLIF(cast(CA.nickname as varchar(max)), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(cast(CA.status as varchar(max)), '') + char(10), '')
+ Coalesce('Ideal Role: ' + NULLIF(cast(CA.customText5 as varchar(max)), '') + char(10), '')
+ Coalesce('Source: ' + NULLIF(cast(CA.Source as varchar(max)), '') + char(10), '')
+ Coalesce('Referred By: ' + NULLIF(cast(CA.name as varchar(max)), '') + char(10), '')
+ Coalesce('Referred By (Other): ' + NULLIF(cast(CA.referredBy as varchar(max)), '') + char(10), '')
+ Coalesce('Other Phone: ' + NULLIF(cast(CA.phone2 as varchar(max)), '') + char(10), '')
--+ Coalesce('Fax: ' + NULLIF(cast(CA.Fax as varchar(max)), '') + char(10), '')
+ Coalesce('Years Experience: ' + NULLIF(cast(CA.experience as varchar(max)), '') + char(10), '')
+ Coalesce('Employment Preference: ' + NULLIF(cast(CA.employmentPreference as varchar(max)), '') + char(10), '')
+ Coalesce('Desired Salary: ' + NULLIF(cast(CA.salary as varchar(max)), '') + char(10), '')
+ Coalesce('Currency: ' + NULLIF(cast(CA.customText2 as varchar(max)), '') + char(10), '')
+ Coalesce('Gross / Net: ' + NULLIF(cast(CA.customText4 as varchar(max)), '') + char(10), '')
+ Coalesce('Current Contract Rate: ' + NULLIF(cast(CA.hourlyRateLow as varchar(max)), '') + char(10), '')
+ Coalesce('Desired Contract Rate: ' + NULLIF(cast(CA.hourlyRate as varchar(max)), '') + char(10), '')
+ Coalesce('Contract Rate Type: ' + NULLIF(cast(CA.customText10 as varchar(max)), '') + char(10), '')
+ Coalesce('Notice Period: ' + NULLIF(cast(CA.customText8 as varchar(max)), '') + char(10), '')
+ Coalesce('Date Available: ' + NULLIF(cast(CA.dateAvailable as varchar(max)), '') + char(10), '')
+ Coalesce('Preferred Work Environment: ' + NULLIF(cast(CA.customText11 as varchar(max)), '') + char(10), '')
+ Coalesce('Preferred Locations: ' + NULLIF(cast(CA.desiredLocations as varchar(max)), '') + char(10), '')
+ Coalesce('Willing to Relocate: ' + NULLIF(cast(CA.willRelocate as varchar(max)), '') + char(10), '')
+ Coalesce('Travel Preferences: ' + NULLIF(cast(CA.customText3 as varchar(max)), '') + char(10), '')
+ Coalesce('Family Situation: ' + NULLIF(cast(CA.customTextBlock2 as varchar(max)), '') + char(10), '')
+ Coalesce('Citizenships / Visas: ' + NULLIF(cast(CA.customText9 as varchar(max)), '') + char(10), '')
+ Coalesce('Authorized to work in Australia: ' + NULLIF(cast(CA.workAuthorized as varchar(max)), '') + char(10), '')
+ Coalesce('General Candidate Comments: ' + NULLIF(cast(CA.comments as varchar(max)), '') + char(10), '')
                        /*  Coalesce('Other email: ' + NULLIF(cast(oe3.email as varchar(max)), '') + char(10), '')
                        + Coalesce('Singapore / PR Yes/No: ' + NULLIF(cast(ca.EmployeeType as varchar(max)), '') + char(10), '')
                        + Coalesce('Skype: ' + NULLIF(cast(CA.phone2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Current Salary: ' + NULLIF(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        + Coalesce('Total Annual Salary: ' + NULLIF(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Salary: ' + NULLIF(cast(CA.customTextBlock2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Willing to Relocate: ' + NULLIF(cast(CA.willRelocate as varchar(max)), '') + char(10), '')
                        + Coalesce('Practice Area / Category: ' + NULLIF(cast(CName.Name as varchar(max)), '') + char(10), '')
                        + Coalesce('Skills: ' + NULLIF(cast(SN.SkillName as varchar(max)), '') + char(10), '')
                        + Coalesce('BullHorn Candidate ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')
                        + Coalesce('Date Registered: ' + NULLIF(convert(varchar(10),CA.dateAdded,120), '') + char(10), '')
                        + Coalesce('Reffered by: ' + NULLIF(cast(CA.referredBy as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Locations: ' + NULLIF(cast(CA.desiredLocations as varchar(max)), '') + char(10), '')
                        + Coalesce('Source: ' + NULLIF(cast(CA.source as varchar(max)), '') + char(10), '')
                        + Coalesce('Employment Preference: ' + NULLIF(cast(CA.employmentPreference as varchar(max)), '') + char(10), '')
                        + Coalesce('General Comment: ' + NULLIF(cast(CA.comments as varchar(max)), '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(cast(CA.status as varchar(max)), '') + char(10), '')
                        + Coalesce('Reffered by UserID: ' + NULLIF(cast(CA.referredByUserID as varchar(max)), '') + char(10), '')                    
                        + Coalesce('Specialty: ' + NULLIF(cast(CA.specialtyIDList as varchar(max)), '') + char(10), '')
                        + Coalesce('Business Sector: ' + NULLIF(cast(BS.BusinessSector as varchar(max)), '') + char(10), '')
                        --+ Coalesce('ID Number: ' + NULLIF(cast(CA.ssn as varchar(max)), '') + char(10), '')
                        --+ Coalesce('AA/EE: ' + NULLIF(cast(CA.customText5 as varchar(max)), '') + char(10), '')
                        + Coalesce('Nationality: ' + NULLIF(cast(CA.customText6 as varchar(max)), '') + char(10), '')
                        + Coalesce('Referral Type: ' + NULLIF(cast(CA.customtext2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Region: ' + NULLIF(cast(CA.customText7 as varchar(max)), '') + char(10), '')
                        + Coalesce('Currency: ' + NULLIF(cast(CA.customText12 as varchar(max)), '') + char(10), '')
                        + Coalesce('Pay Unit: ' + NULLIF(cast(CA.customText13 as varchar(max)), '') + char(10), '')
                        + Coalesce('Salary Notes: ' + NULLIF(cast(CA.customTextBlock1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Notice Period: ' + NULLIF(cast(CA.customText11 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Languages: ' + NULLIF(cast(CA.customComponent1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Date of Birth: ' + NULLIF(cast(CA.customFloat1 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Description: ' + NULLIF(cast(CA.description as varchar(max)), '') + char(10), '')
                        + Coalesce('University (U): ' + NULLIF(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + Coalesce('If other uni (U): ' + NULLIF(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        + Coalesce('Primary Field of Study (U): ' + NULLIF(cast(CA.customText13 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Classification (U): ' + NULLIF(cast(CA.customText15 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Graduation Year (U): ' + NULLIF(cast(CA.customText16 as varchar(max)), '') + char(10), '')
                        + Coalesce('University (P): ' + NULLIF(cast(CA.customText17 as varchar(max)), '') + char(10), '')
                        + Coalesce('If other uni (P): ' + NULLIF(cast(CA.customText6 as varchar(max)), '') + char(10), '')
                        + Coalesce('Qualification Type (P): ' + NULLIF(cast(CA.customText18 as varchar(max)), '') + char(10), '')
                        + Coalesce('Primary Field of Study (P): ' + NULLIF(cast(CA.customText19 as varchar(max)), '') + char(10), '')
                        + Coalesce('Classification (P): ' + NULLIF(cast(CA.customText10 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Graduation Year (P): ' + NULLIF(cast(CA.customFloat3 as varchar(max)), '') + char(10), '')
                        --
                        --+ Coalesce('Bar admission: ' + NULLIF(cast(AD.Admission as varchar(max)), '') + char(10), '')
                        --+ Coalesce('General Work Function: ' + NULLIF(cast(CA.customText2 as varchar(max)), '') + char(10), '') */
                        --+ Coalesce('Summary: ' + NULLIF(cast(summary.summary as varchar(max)), '') + char(10), '') 
                        , 1, 0, '') as note
	-- select top 10 *
	from bullhorn1.Candidate CA
	left join SkillName SN on CA.userID = SN.userId
	left join BusinessSector BS on CA.userID = BS.userId
	left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
        left outer join admission AD on CA.userID = AD.Userid
        left outer join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
        left outer join CName on CA.userID = CName.Userid
        left outer join SpeName on CA.userID = SpeName.Userid
         left join (select bh_usercontact.userid , bh_usercontact.name from bullhorn1.BH_UserContact) UC on CA.userid = UC.userID
        --left join mail5 on CA.userID = mail5.ID
        left join oe3 on CA.userID = oe3.ID
        --left join summary on CA.userID = summary.CandidateID
	where CA.isPrimaryOwner = 1 )
--select count(*) from note
--select * from note --where AddedNote like '%Business Sector%'
-- select top 10 * from bullhorn1.Candidate CA

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



	select --top 200
                  C.candidateID as 'candidate-externalId'
                --C.userID as '#userID'
		, case C.gender when 'M' then 'MR' when 'F' then 'MISS'	else '' end as 'candidate-title'
		, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
		, C.middleName as 'candidate-middleName'
		, C.Nickname as 'Preferred Name' --<<
		, CONVERT(VARCHAR(10),C.dateOfBirth,120) as 'candidate-dob'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL' OR tc.abbreviation in ('ZR','YU')) THEN '' ELSE tc.abbreviation END as 'candidate-citizenship'
		, iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), iif(e1.email = '' or e1.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemail.com'),e1.email) ) as 'candidate-email'
		, e2.email as 'candidate-workEmail'
		, C.mobile as 'candidate-phone'
		, C.mobile as 'candidate-mobile'
		, C.phone as 'candidate-homePhone'	
		, C.workPhone as 'candidate-workPhone'
		--, C.address1 as 'candidate-address'
                , ltrim(Stuff( Coalesce(' ' + NULLIF(C.address1, ''), '')
                                + Coalesce(', ' + NULLIF(C.address2, ''), '')
                                + Coalesce(', ' + NULLIF(C.city, ''), '')
                                + Coalesce(', ' + NULLIF(C.state, ''), '')
                                + Coalesce(', ' + NULLIF(C.zip, ''), '')
                                + Coalesce(', ' + NULLIF(tc.country, ''), '')
                                + Coalesce(', Skype: ' + NULLIF(c.customtext12, ''), '')
                                , 1, 1, '') ) as 'candidate-Address'	
                , c.customtext12 as 'candidate-skype' --<<
		, 'PERMANENT' as 'candidate-jobTypes'
		, c.source as 'candidate-source' --<<	
		, C.city as 'candidate-city'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN '' ELSE tc.abbreviation END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		, C.state as 'candidate-state'
		, cast(C.salaryLow as int) as 'candidate-currentSalary' --,C.customTextBlock3
		, cast(C.salary as int) as 'candidate-desiredSalary'
		--, C.customText2 as 'candidate-currency'
		--, Education.school as 'candidate-schoolName'
		--, Education.graduationDate as 'candidate-graduationDate'
		--, Education.degree as 'candidate-degreeName'
		--, Education.major as '#candidate-major'
		--, SN.SkillName as 'candidate-skills'
                , ltrim(Stuff( Coalesce(NULLIF(SN.SkillName, '') + char(10), '')
                        + Coalesce(NULLIF(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
                        + Coalesce(NULLIF(convert(varchar(max),C.skillset), ''), '')
                        , 1, 0, '') ) as 'candidate-skills'
		, C.companyName as 'candidate-company1'
		, C.occupation as 'candidate-jobTitle1'
		, C.companyName as 'candidate-employer1'
                /*, ltrim(Stuff( Coalesce('Job Title: ' + NULLIF(C.occupation, '') + char(10), '')
                                + Coalesce('Employer: ' + NULLIF(C.companyName, '') + char(10), '')
                                , 1, 0, '') ) as 'candidate-workhistory' */
		--, C.recruiterUserID as '#recruiterUserID'
		, owner.email as 'candidate-owners'
		--, t4.finame as '#Candidate File'
		, files.ResumeId as 'candidate-resume'
		, note.note as 'candidate-note'
		--, left(comment.comment,32760) as 'candidate-comments'
	-- select count (*) --7758-- select distinct employmentPreference-- select gender -- select customTextBlock1, convert(varchar(max),C.skillset)-- select distinct c.customText2
	from bullhorn1.Candidate C  --where C.isPrimaryOwner = 1 --29386
	left join SkillName SN on C.userID = SN.userId
	left join tmp_country tc ON c.countryID = tc.code
	left join owner on C.recruiterUserID = owner.recruiterUserID
	left join e1 on C.userID = e1.ID
	left join e2 on C.userID = e2.ID
	left join ed on C.userID = ed.ID -- candidate-email-DUPLICATION
	left join Education on C.userID = Education.userID
	left join t4 on t4.candidateUserID = C.userID
	left join files on C.userID = files.candidateUserID
	--left join comment on C.userID = comment.Userid
	left join note on C.userID = note.Userid
	where C.isPrimaryOwner = 1
	--and C.userid in (37380,5)
	--and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
	--and concat (C.FirstName,' ',C.LastName) like '%Partha%'
	--and e1.email = '' or e1.email is null --e1.email <> ''
	--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
	--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID

/*
select    C.candidateID as 'externalId'
	, C.Nickname as 'PreferredName'
        , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
from bullhorn1.Candidate C
where Nickname <> '' and Nickname is not null


with t as (
select    C.candidateID as 'externalId'
        , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
        , C.source as original
        , case 
                when c.source like '%Inde%' then 29085
                when c.source like '%Volc%' then 29084
                when c.source like '%Dova%' then 29086
                when c.source like '%Broa%' then 29089
                when c.source like '%Head%' then 29091
                when c.source like '%Refe%' then 29092
                when c.source like '%Data%' then 29093
                when c.source like '%Inst%' then 29087
                when c.source like '%Logi%' then 29094
                when c.source like '%Auto%' then 29095
                when c.source like '%Inde%' then 29098
                when c.source like '%Alph%' then 29096
                when c.source like '%Face%' then 29097
                when c.source like '%Jobstreet%' then 29099
                when c.source like '%Lee %' then 29101
                when c.source like '%Link%' then 29090
                when c.source like '%Link%Job%' then 29102
                when c.source like '%Regi%' then 29103
                when c.source like '%JobsDB%' then 29100
        else '' end as 'candidate_source_id'
from bullhorn1.Candidate C
where source <> '' and source is not null )
--select count(*) from t where source is not null
select * from t where candidate_source_id <> 0 and candidate_source_id <> '29093'
--where externalid = 13144

*/