
with
-- EMAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(email,',',email2,',',email3)
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact where email like '%_@_%.__%' or email2 like '%_@_%.__%' or email3 like '%_@_%.__%' ) -- from bullhorn1.Candidate
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
/*, mail5 (ID, email1, email2, email3) as (
		select pe.ID, email as email1, we.email2, oe.email3 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		left join (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 ) */
, mail5 (ID, email1, email2) as ( select pe.ID, email as email1, we.email2 from mail4 pe
		                  left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		                  where pe.rn = 1 )
, oe1 as (select ID, email from mail4 where rn = 3)

-- OWNER
, owner as (select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

-- SkillName: split by separate rows by comma, then combine them into SkillName
, SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SkillName0 as a where a.skillID <> '' GROUP BY a.userId)

-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
, BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS URLList FROM BusinessSector0 as a where a.businessSectorID <> '' GROUP BY a.userId)
--select distinct BusinessSector from BusinessSector

-- CATEGORY - VC FE info
, CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID WHERE CateSplit.categoryid <> '' and Userid = a.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM CateSplit as a where a.categoryid <> '' GROUP BY a.Userid)

-- SPECIALTY - VC SFE info
, SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate) t CROSS APPLY x.nodes('/M') as Split(a) )
, SpeName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + VS.name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '' and Userid = b.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SpecSplit as b where b.specialtyid <> '' GROUP BY b.Userid)

-- ADMISSION
, AdmissionRows(userId, CombinedText) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)

-- NOTE
, note as (
	SELECT CA.userID
		 , Stuff( Coalesce('Bullhorn Candidate ID: ' + NULLIF(cast(CA.candidateID as varchar(max)), '') + char(10), '')
		        + Coalesce('Other email: ' + NULLIF(cast(oe1.email as varchar(max)), '') + char(10), '')
                        + Coalesce('Date Registered: ' + NULLIF(convert(varchar(10),CA.dateAdded,120), '') + char(10), '')
                        --+ Coalesce('Status: ' + NULLIF(cast(CA.status as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Reffered by UserID: ' + NULLIF(cast(CA.referredByUserID as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Reffered by: ' + NULLIF(cast(CA.referredBy as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Phone 2: ' + NULLIF(cast(CA.phone2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Locations: ' + NULLIF(cast(CA.desiredLocations as varchar(max)), '') + char(10), '')
                        + Coalesce('Skills: ' + NULLIF(cast(SN.SkillName as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Practice Area / Category: ' + NULLIF(cast(CName.Name as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Specialty: ' + NULLIF(cast(CA.specialtyIDList as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Business Sector: ' + NULLIF(cast(BS.BusinessSector as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Source: ' + NULLIF(cast(CA.source as varchar(max)), '') + char(10), '')
                        --
                        + Coalesce('How did you hear about us?: ' + NULLIF(cast(CA.customText2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Application Date (Zoho): ' + NULLIF(cast(CA.customDate1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Do you have the legal right to work in the UK? : ' + NULLIF(cast(CA.customText12 as varchar(max)), '') + char(10), '')
                       -- + Coalesce('Sectors of Interest: ' + NULLIF(cast(CA.customTextBlock5 as varchar(max)), '') + char(10), '')
                        + Coalesce('Add more details on sector of interest: ' + NULLIF(cast(CA.customTextBlock1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Primary Sector we''d Recommend for: ' + NULLIF(cast(CA.customText3 as varchar(max)), '') + char(10), '')
                        + Coalesce('Secondary Sector we''d Recommend for: ' + NULLIF(cast(CA.customText4 as varchar(max)), '') + char(10), '')
                        + Coalesce('Tertiary Sector we''d Recommend for: ' + NULLIF(cast(CA.customText5 as varchar(max)), '') + char(10), '')
                        + Coalesce('Sectors we''d recommend for: ' + NULLIF(cast(CA.categoryID as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Description: ' + NULLIF(cast(CA.description as varchar(max)), '') + char(10), '')
                        --+ Coalesce('University (U): ' + NULLIF(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + Coalesce('If other uni (U): ' + NULLIF(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Primary Field of Study (U): ' + NULLIF(cast(CA.customText13 as varchar(max)), '') + char(10), '') --ENJECTION TO COURSE
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
                        --+ Coalesce('General Work Function: ' + NULLIF(cast(CA.customText2 as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Summary: ' + NULLIF(cast(summary.summary as varchar(max)), '') + char(10), '')
                        , 1, 0, '') as note
	from bullhorn1.Candidate CA
	left join SkillName SN on CA.userID = SN.userId
	left join BusinessSector BS on CA.userID = BS.userId
	left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
        left outer join admission AD on CA.userID = AD.Userid
        left outer join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
        left outer join CName on CA.userID = CName.Userid
        left outer join SpeName on CA.userID = SpeName.Userid
        left join oe1 on CA.userID = oe1.ID
        --left join summary on CA.userID = summary.CandidateID
	where CA.isPrimaryOwner = 1 )
--select count(*) from note
--select top 100 * from note --where AddedNote like '%Business Sector%'

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


	select
                C.userID as '#userID'
		, case C.gender when 'M' then 'MR' when 'F' then 'MISS'	else '' end as 'candidate-title'
		, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
		, C.candidateID as 'candidate-externalId'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
		--, C.middleName as 'candidate-middleName'
		, CONVERT(VARCHAR(10),C.dateOfBirth,120) as 'candidate-dob'
		, Coalesce(NULLIF(mail5.email1,''), concat('candidate-',cast(C.userID as varchar(max)),'@noemail.com') ) as 'candidate-email'
		, mail5.email2 as 'candidate-workEmail'
		, C.mobile as 'candidate-phone'
		--, C.phone2 as 'candidate-homePhone'	
		, C.mobile as 'candidate-mobile'
		--, C.workPhone as 'candidate-workPhone'
		, 'PERMANENT' as 'candidate-jobTypes'
		, C.address1 as 'candidate-address'
		, C.city as 'candidate-city'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation	END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		--, C.state as 'candiadte-state'
		--, cast(C.salaryLow as int) as 'candidate-currentSalary'
		, cast(C.salary as int) as 'candidate-desiredSalary'
		
		--, Education.school as 'candidate-schoolName'
		, C.customText1 as 'candidate-schoolName'
		
		--, Education.graduationDate as 'candidate-graduationDate'
		, C.customText16 as 'candidate-graduationDate'
		
		--, Education.degree as 'candidate-degreeName'
		, C.customText15 as 'candidate-degreeName'
		
		--, Education.major as '#candidate-major'
		--, SN.SkillName as 'candidate-skills'
		, C.companyName as 'candidate-company1'
		, C.occupation as 'candidate-jobTitle1'
		, C.companyName as 'candidate-employer1'
		--, C.recruiterUserID as '#recruiterUserID'
		, owner.email as 'candidate-owners'
		, t4.finame as '#Candidate File'
		, files.ResumeId as 'candidate-resume'
		, note.note as 'candidate-note'
		--, left(comment.comment,32760) as 'candidate-comments'
	-- select count (*)
	from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
	left join SkillName SN on C.userID = SN.userId
	left join tmp_country tc ON c.countryID = tc.code
	left join owner on C.recruiterUserID = owner.recruiterUserID
	left join mail5 on C.userID = mail5.ID
	left join Education on C.userID = Education.userID
	left join t4 on t4.candidateUserID = C.userID
	left join files on C.userID = files.candidateUserID
	--left join comment on C.userID = comment.Userid
	left join note on C.userID = note.Userid
	where C.isPrimaryOwner = 1
	--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
	--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
        --where [contact-firstName] like '%Sam%'


/*
-- COMMENT
with comment(candidateID, comment) as (
        SELECT C.candidateID --,UC.Userid
                , STUFF( Coalesce('Date Added: ' + NULLIF(convert(varchar(10), UC.dateAdded, 120), '') + char(10), '')
                        + Coalesce('Action: ' + NULLIF(cast(UC.comments as varchar(max)), '') + char(10), '')
                        , 1, 0, '') as note
        from bullhorn1.BH_UserComment UC
        left join bullhorn1.Candidate C on C.userID = UC.Userid
        )
--select count(*) from comment --35.508 37.583
--select top 1000 * from comment
                             
, summary(candidateID,summary) as (
        SELECT candidateID
                , STUFF( Coalesce('Date Added: ' + NULLIF(convert(varchar(10), dateAdded, 120), '') + char(10), '')
                        + coalesce('Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), '') 
                , 1, 0, '' ) as summary
        from bullhorn1.BH_CandidateHistory )
--select count(*) from summary --105.793
--select top 1000 * from summary

select count(*) from comment c,summary s where c.candidateID = s.candidateID

--
with comments as (
        SELECT --top 1000
                C.candidateID --,UC.Userid
                , UC.dateAdded
                , coalesce('Date Added: ' + convert(varchar(10), UC.dateAdded, 120) + char(10) + 'Action: ' + NULLIF(cast(UC.comments as varchar(max)), ''), '') as note
                --, STUFF( Coalesce('Date Added: ' + NULLIF(convert(varchar(10), UC.dateAdded, 120), '') + char(10), '') + Coalesce('Action: ' + NULLIF(cast(UC.comments as varchar(max)), '') + char(10), ''), 1, 0, '') as note
        --select top 1000 UC.comments 
        from bullhorn1.BH_UserComment UC
        left join bullhorn1.Candidate C on C.userID = UC.Userid where C.candidateID is not null and cast(UC.comments as varchar(max)) <> ' '
UNION ALL
        SELECT --top 1000
                candidateID
                , dateAdded
                , coalesce('Date Added: ' + convert(varchar(10), dateAdded, 120) + char(10) + 'Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), '') as summary
                --, STUFF( Coalesce('Date Added: ' + NULLIF(convert(varchar(10), dateAdded, 120), '') + char(10), '') + coalesce('Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), ''), 1, 0, '' ) as summary
        -- select top 1000 comments
        from bullhorn1.BH_CandidateHistory )

-- select count(*) from comments where note <> '' --75220
select top 100
                   candidateID
                  , cast('-10' as int) as userid
                  , cast('4' as int) as contact_method
                  , cast('1' as int) as related_status
                  , dateAdded
                  , note
from comments where note <> ''
*/