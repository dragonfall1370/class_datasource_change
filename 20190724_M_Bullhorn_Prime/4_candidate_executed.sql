with
-- EMAIL
  mail1 (ID,email) as (select C.candidateID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail 
	from bullhorn1.BH_UserContact UC 
	left join bullhorn1.Candidate C on C.userID = UC.UserID
	where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and (C.isdeleted <> 1 and C.status <> 'Archive') /*and C.isPrimaryOwner = 1*/ )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed (ID,email,rn) as (SELECT ID,email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)

-- BusinessSector >>> INDUSTRY
, INDUSTRY0 as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID 
	FROM (SELECT userid, CAST('<M>' + REPLACE(cast( ltrim(rtrim( convert(nvarchar(max),businessSectorIDList) )) as nvarchar(max)),',','</M><M>') + '</M>' AS XML) AS x 
		FROM bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') t CROSS APPLY x.nodes('/M') AS Split(a) 
	)

, INDUSTRY as (SELECT userid, STRING_AGG( ltrim(rtrim( convert(nvarchar(max),BSL.name) )),', ' ) WITHIN GROUP (ORDER BY BSL.name) name 
	from INDUSTRY0 left join bullhorn1.BH_BusinessSectorList BSL ON INDUSTRY0.businessSectorID = BSL.businessSectorID
	WHERE INDUSTRY0.businessSectorID <> '' GROUP BY userid) -- no records

-- CATEGORY >>> FUNCTIONAL EXPERTISE
, CATEGORY(candidateID, userid, categoryIDList, categoryID, name) as (
       select C.candidateID, C.userid, C.categoryIDList, C.categoryID, UC.name
       FROM bullhorn1.Candidate C
       left join ( select UC.userID, UC.categoryID, cl.occupation as name
			--, replace(replace(replace(replace(replace(name,'4294:',''),'4295:',''),'4296:',''),'4297:',''),'850:','') as name 
			from BULLHORN1.BH_UserCategory UC 
			left join bullhorn1.BH_CategoryList CL ON UC.categoryid = CL.categoryID ) UC on UC.userid = C.userID
		where UC.name is not null and UC.name <> '' and C.isdeleted <> 1 and C.status <> 'Archive'
       )
--select distinct name from CATEGORY
--select * from CATEGORY
--select * from BULLHORN1.BH_UserCategory

--SPECIALTY >>> SUB FUNCTIONAL EXPERTISE
, SPECIALTY(candidateID, userid, specialtyIDList, SpecialtyID, name) as (
       select C.candidateID, C.userid, C.specialtyIDList, UC.SpecialtyID, UC.name
       FROM bullhorn1.Candidate C
       left join (select US.userID, US.SpecialtyID, VS.name as name
					from BULLHORN1.View_UserSpecialty US
					left join bullhorn1.View_Specialty VS ON US.SpecialtyID = VS.specialtyID ) UC on UC.userid = C.userID
       where UC.name is not null and UC.name <> '' and C.isdeleted <> 1 and C.status <> 'Archive' -- no records
       )
--select distinct name from SPECIALTY
--select * from SPECIALTY where userid in (155843,165184 ,165199, 161585)

--SKILL
, skill (candidateID,userid, SkillName) as ( 
       SELECT candidateID, userid, STRING_AGG(name,', ' ) WITHIN GROUP (ORDER BY name) files 
       from (
              SELECT [BH_Candidate].candidateID, [BH_Candidate].userID, [BH_UserSkill].skillID, [BH_SkillList].name 
              FROM [bullhorn1].[BH_Candidate]
              LEFT JOIN [bullhorn1].BH_User on [BH_User].userID = [BH_Candidate].userID
              LEFT JOIN [bullhorn1].BH_UserSkill on [BH_Candidate].userID = [BH_UserSkill].userID
              LEFT JOIN [bullhorn1].BH_SkillList on [BH_UserSkill].skillID = [BH_SkillList].skillID
              WHERE [BH_SkillList].name is not null and [BH_SkillList].name <> '' and [BH_Candidate].isdeleted <> 1 and [BH_Candidate].status <> 'Archive' ) s
       GROUP BY candidateID,userid )
--select top 10 * from SKILL where userid in (165199)

-- ADMISSION
, admissionRows (userId, CombinedText, dateadded) as 
	(select UCOI.userID, concat(text1,' ',text2) as CombinedText, COI.dateadded
		from bullhorn1.BH_UserCustomObjectInstance UCOI
		inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)

, admission(Userid, Admission) as (SELECT userid, STRING_AGG( CombinedText,'|| ' ) WITHIN GROUP (ORDER BY dateadded) name
		from admissionRows
		where CombinedText <> '' GROUP BY userid )
--select top 10 * from admission

-- NEWEST EDUCATION
-- select * from bullhorn1.BH_UserEducation 
, EducationGroup as (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID)

, Education as (
       select EG.userID
              , UE.certification
              , UE.city
              , UE.comments
              --, UE.customText1
              , UE.dateAdded
              , UE.degree
              , UE.endDate
              , UE.expirationDate
              , UE.gpa
              , convert(varchar(10),UE.graduationDate,120) as graduationDate
              , UE.major
              , UE.school
              , UE.startDate
              , UE.state
              , UE.userEducationID       
       from EducationGroup EG left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)

--WEB RESPONSES
, wr as (
        select jr.userid, jp.title,jr.status
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isdeleted <> 1 and CA.status <> 'Archive') CAI on JR.userID = CAI.CandidateUserID
        left join bullhorn1.BH_JobPosting  jp on jp.jobPostingID = jr.jobPostingID )
, wr1 as (SELECT userID, STRING_AGG( concat('Title: ',title,' - Status: ',status) ,char(10)) WITHIN GROUP (ORDER BY title) name from wr GROUP BY userID )
--select top 10 * from wr1

--LATEST COMMENT
, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc)
		FROM bullhorn1.BH_UserComment )

--CANDIDATE CERTIFICATE (no value)

--OWNER
, owner as ( select ca.candidateid, ca.recruiterUserID, ca.owneruseridlist, concat(ca.recruiterUserID,',', ca.ownerUserIDList) as owners 
		from bullhorn1.Candidate CA
		left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID
		where CA.isdeleted <> 1 and CA.status <> 'Archive')
--select ca.owneruseridlist from bullhorn1.Candidate CA where owneruseridlist like '%,%'
, owner2a as (SELECT candidateid, Split.a.value('.', 'VARCHAR(100)') AS String
		FROM (SELECT candidateid, CAST ('<M>' + REPLACE(convert(nvarchar(max),owners),',','</M><M>') + '</M>' AS XML) AS Data FROM owner
					where owners is not null and owners <> '') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
--, owner2b as (select distinct owner2a.candidateid, UC.email, UC.name from owner2a left join (select ca.candidateid, uc.email, uc.name from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.UserID = UC.userID ) UC on UC.candidateid = owner2a.String)
, owner2b as ( select distinct owner2a.candidateid, UC.email, UC.name
		from owner2a
		left join bullhorn1.BH_UserContact UC on UC.userid = owner2a.String)
--select top 10 * from owner2b where candidateid in (31595) and email <> '' and email is not null
, owner2c as (SELECT candidateid, STRING_AGG( email,',') WITHIN GROUP (ORDER BY email) email
		from owner2b
		where email like '%_@_%.__%' GROUP BY candidateid )
--select top 10 * from owner2c where candidateid in (31595) and email like '%,%' --candidateid in (39194)

-- DOCUMENT
, files(candidateUserID, files) as (SELECT candidateUserID, STRING_AGG( concat(candidateFileID, fileExtension),',' ) WITHIN GROUP (ORDER BY candidateFileID) files
		from bullhorn1.View_CandidateFile GROUP BY candidateUserID)
--select top 10 * from files
, placementfiles(userID, files) as (SELECT userID, STRING_AGG( concat(placementFileID, fileExtension),',' ) WITHIN GROUP (ORDER BY placementFileID) files
		from bullhorn1.View_PlacementFile GROUP BY userID)
--select top 10 * from placementfiles
, doc(Userid, files) as ( select f.candidateUserID, STRING_AGG( f.files,',' ) WITHIN GROUP (ORDER BY f.files) files
		from (SELECT * from files 
				UNION ALL SELECT * from placementfiles) f
		GROUP BY f.candidateUserID )
--select top 10 * from doc

-- NOTE
, note as (
	SELECT CA.candidateid
		 , stuff( coalesce('BH Candidate ID: ' + nullif(cast(CA.userID as varchar(max)), '') + char(10), '')
						+ coalesce('Date available: ' + nullif(convert(nvarchar(10),ca.dateAvailable,120), '') + char(10), '')
                        --+ coalesce('Other email: ' + nullif(cast(e3.email as varchar(max)), '') + char(10), '')
                        --+ coalesce('Desired Locations: ' + nullif(cast(CA.desiredLocations as varchar(max)), '') + char(10), '')
                        + coalesce('Employment Preference: ' + nullif(cast(CA.employmentPreference as varchar(max)), '') + char(10), '')
                        + coalesce('Referred By (Other): ' + nullif(convert(varchar(max),CA.referredBy), '') + char(10), '')
                        + coalesce('Referred By: ' + nullif(convert(varchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
                        + coalesce('Status: ' + nullif(convert(varchar(max),CA.Status), '') + char(10), '')
                        --+ coalesce('Willing to Relocate: ' + nullif(cast(CA.willRelocate as varchar(max)), '') + char(10), '')                       
						--+ coalesce('HKID#: ' + nullif(convert(varchar(max),CA.customText1), '') + char(10), '')
						--+ coalesce('Other Phone: ' + nullif(convert(varchar(max),CA.phone2), '') + char(10), '')
                        + coalesce('CV: ' + nullif(UC1.description, '') + char(10), '')
                        + coalesce('General Comments: ' + nullif(convert(varchar(max),CA.comments), '') + char(10), '')
						--+ coalesce('CV: ' + nullif(UW.description, '') + char(10), '')
						+ coalesce('Latest Comment: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](lc.comments), '') + char(10), '')
                        /*+ coalesce('Placements: ' + nullif(convert(varchar(max),pm.status), '') + char(10), '') --CA.activePlacements
                        + coalesce('Secondary Owners: ' + nullif(convert(varchar(max),owner2c.name), '') + char(10), '') --CA.secondaryOwners
                        + coalesce('Web Responses: ' + nullif(convert(varchar(max),wr1.name), '') + char(10), '') --CA.jobResponseJobPostingID                        
                        + coalesce('Singapore / PR Yes/No: ' + nullif(cast(ca.EmployeeType as varchar(max)), '') + char(10), '')
                        + coalesce('Skype: ' + nullif(cast(CA.phone2 as varchar(max)), '') + char(10), '')
                        + coalesce('Current Salary: ' + nullif(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        + coalesce('Total Annual Salary: ' + nullif(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + coalesce('Desired Salary: ' + nullif(cast(CA.customTextBlock2 as varchar(max)), '') + char(10), '')
                        + coalesce('Practice Area / Category: ' + nullif(cast(CName.Name as varchar(max)), '') + char(10), '')
                        + coalesce('Skills: ' + nullif(cast(SN.SkillName as varchar(max)), '') + char(10), '')
                        + coalesce('Date Registered: ' + nullif(convert(varchar(10),CA.dateAdded,120), '') + char(10), '')
                        + coalesce('Reffered by: ' + nullif(cast(CA.referredBy as varchar(max)), '') + char(10), '')
                        + coalesce('Source: ' + nullif(cast(CA.source as varchar(max)), '') + char(10), '')
                        + coalesce('General Comment: ' + nullif(cast(CA.comments as varchar(max)), '') + char(10), '')
                        + coalesce('Status: ' + nullif(cast(CA.status as varchar(max)), '') + char(10), '')
                        + coalesce('Reffered by UserID: ' + nullif(cast(CA.referredByUserID as varchar(max)), '') + char(10), '')                    
                        + coalesce('Specialty: ' + nullif(cast(CA.specialtyIDList as varchar(max)), '') + char(10), '')
                        + coalesce('Business Sector: ' + nullif(cast(BS.BusinessSector as varchar(max)), '') + char(10), '')
                        + coalesce('ID Number: ' + nullif(cast(CA.ssn as varchar(max)), '') + char(10), '')
                        + coalesce('AA/EE: ' + nullif(cast(CA.customText5 as varchar(max)), '') + char(10), '')
                        + coalesce('Nationality: ' + nullif(cast(CA.customText6 as varchar(max)), '') + char(10), '')
                        + coalesce('Referral Type: ' + nullif(cast(CA.customtext2 as varchar(max)), '') + char(10), '')
                        + coalesce('Region: ' + nullif(cast(CA.customText7 as varchar(max)), '') + char(10), '')
                        + coalesce('Currency: ' + nullif(cast(CA.customText12 as varchar(max)), '') + char(10), '')
                        + coalesce('Pay Unit: ' + nullif(cast(CA.customText13 as varchar(max)), '') + char(10), '')
                        + coalesce('Salary Notes: ' + nullif(cast(CA.customTextBlock1 as varchar(max)), '') + char(10), '')
                        + coalesce('Notice Period: ' + nullif(cast(CA.customText11 as varchar(max)), '') + char(10), '')
                        + coalesce('Languages: ' + nullif(cast(CA.customComponent1 as varchar(max)), '') + char(10), '')
                        + coalesce('Date of Birth: ' + nullif(cast(CA.customFloat1 as varchar(max)), '') + char(10), '')
                        + coalesce('Description: ' + nullif(cast(CA.description as varchar(max)), '') + char(10), '')
                        + coalesce('University (U): ' + nullif(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + coalesce('If other uni (U): ' + nullif(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        + coalesce('Primary Field of Study (U): ' + nullif(cast(CA.customText13 as varchar(max)), '') + char(10), '')
                        + coalesce('Classification (U): ' + nullif(cast(CA.customText15 as varchar(max)), '') + char(10), '')
                        + coalesce('Graduation Year (U): ' + nullif(cast(CA.customText16 as varchar(max)), '') + char(10), '')
                        + coalesce('University (P): ' + nullif(cast(CA.customText17 as varchar(max)), '') + char(10), '')
                        + coalesce('If other uni (P): ' + nullif(cast(CA.customText6 as varchar(max)), '') + char(10), '')
                        + coalesce('Qualification Type (P): ' + nullif(cast(CA.customText18 as varchar(max)), '') + char(10), '')
                        + coalesce('Primary Field of Study (P): ' + nullif(cast(CA.customText19 as varchar(max)), '') + char(10), '')
                        + coalesce('Classification (P): ' + nullif(cast(CA.customText10 as varchar(max)), '') + char(10), '')
                        + coalesce('Graduation Year (P): ' + nullif(cast(CA.customFloat3 as varchar(max)), '') + char(10), '')
                        + coalesce('Bar admission: ' + nullif(cast(AD.Admission as varchar(max)), '') + char(10), '')
                        + coalesce('General Work Function: ' + nullif(cast(CA.customText2 as varchar(max)), '') + char(10), '') 
                        + coalesce('Summary: ' + nullif(cast(summary.summary as varchar(max)), '') + char(10), '') */
                        , 1, 0, '') as note
		from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
		left join e3 on CA.userID = e3.ID
		left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
			/*left join (SELECT userid, STUFF((
                        SELECT char(10) + nullif(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid*/	
		left join (select userid, trim([bullhorn1].[fn_ConvertHTMLToText](description)) as description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
		--left join lc on lc.userid = ca.userID --last comment
		--left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
		--left join SkillName SN on CA.userID = SN.userId
		--left join BusinessSector BS on CA.userID = BS.userId
		--left join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
		--left join admission AD on CA.userID = AD.Userid
		--left join CName on CA.userID = CName.Userid
		--left join SpeName on CA.userID = SpeName.Userid
		--left join mail5 on CA.userID = mail5.ID
		--left join summary on CA.userID = summary.CandidateID
		--left join (select userid, status from bullhorn1.BH_Placement ) pm on pm.userid = ca.userid
		--left join owner2c on owner2c.userid = CA.userid
		--left join wr1 on wr1.userid = CA.userid
		left join (select * from lc where rn = 1) lc on lc.userid = CA.userid --last comment
		where isdeleted <> 1 and status <> 'Archive' )
--select count(*) from note --47954
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note

--MAIN SCRIPT
select
         concat('PR', c.candidateID) as 'candidate-externalId'
	, case 
              when C.nameprefix in ('Dr','Dr.') then 'DR' 
              when C.nameprefix in ('Mr','Mr.') then 'MR' 
              when C.nameprefix in ('Miss','Miss.','Ms','Ms.') then 'MISS' 
              when C.nameprefix in ('Mrs.') then 'MRS' 
              else '' end as 'candidate-title'
	, case 
              when C.nameprefix in ('Mr','Mr.') then 'MALE' 
              when C.nameprefix in ('Miss','Ms.','Mrs.') then 'FEMALE' 
              else '' end as 'candidate-gender'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname - ',C.userID)) as 'candidate-lastName'
	, C.middleName as 'candidate-middleName'
	--, C.Nickname as 'Preferred Name' --CUSTOM SCRIPT
	--, convert(varchar(10),C.dateOfBirth,120) as 'candidate-dob'
	, iif(ed.rn > 1,concat(ed.rn, '_', ed.email), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
	, C.mobile as 'candidate-phone' --edited
	, concat_ws(',',  nullif(C.mobile, ''), nullif(C.phone2, '')) as 'candidate-mobile'
	, C.phone as 'candidate-homePhone'	
	, C.workPhone as 'candidate-workPhone'
	, trim(stuff( coalesce(' ' + nullif(c.address1, ''), '') + coalesce(', ' + nullif(c.address2, ''), '') 
			+ coalesce(', ' + nullif(c.city, ''), '') + coalesce(', ' + nullif(c.state, ''), '') 
			+ coalesce(', ' + nullif(c.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), '') , 1, 1, '') ) as 'candidate-address'
	, trim(stuff( coalesce(' ' + nullif(C.city, ''), '') + coalesce(', ' + nullif(C.state, ''), '') 
			+ coalesce(', ' + nullif(C.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '') ) as 'candidate-locationName' 
	, C.city as 'candidate-city'
	, C.state as 'candidate-state'
  , C.zip as 'candidate-zipCode'
	, tc.abbreviation as 'candidate-country'	
	, cast(C.salaryLow as int) as 'candidate-currentSalary'
	, cast(C.salary as int) as 'candidate-desiredSalary'	
	, 'PERMANENT' as 'candidate-jobTypes'
	, c.companyName as 'candidate-company1'
	, c.occupation as 'candidate-jobTitle1'
	, c.companyName as 'candidate-employer1'
	, owner2c.email as 'candidate-owners' --, C.recruiterUserID
	, doc.files as 'candidate-resume'
	--, note.note as 'candidate-note' --*** Injected later
	--, es.es as 'candidate-education' --*** Injected later
	--, eh.eh as 'candidate-workHistory' --*** Injected later
	, e2.email as 'candidate-workEmail'
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
left join owner2c on owner2c.candidateid = C.candidateid
left join ed on ed.ID = C.candidateID --candidate-email-deduplication
left join e2 on e2.ID = C.candidateID --personal email
left join e3 on e3.ID = c.candidateID --work email
left join tmp_country tc ON tc.code =  c.countryID
left join doc on doc.UserID = C.userID
where C.isdeleted <> 1 and C.status <> 'Archive' --where C.isPrimaryOwner = 1
order by c.candidateID
--27366 rows