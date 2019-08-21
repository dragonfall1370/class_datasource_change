
with
-- SkillName: split by separate rows by comma, then combine them into SkillName
  SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, SkillName(userId, SkillName) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SkillName0 as a where a.skillID <> '' GROUP BY a.userId)

-- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
, BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS URLList FROM BusinessSector0 as a where a.businessSectorID <> '' GROUP BY a.userId)
--select distinct BusinessSector from BusinessSector
--OLD bs1(userid, businessSectorID) as (SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS businessSectorID FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/XMLRoot/RowData')m(n) )
--OLD, BusinessSector(userId, BusinessSector) as (SELECT userid,STUFF((SELECT DISTINCT ', ' + BSL.name from bs1 inner join bullhorn1.BH_BusinessSectorList BSL ON bs1.businessSectorID = BSL.businessSectorID WHERE bs1.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM bs1 as a where a.businessSectorID <> '' GROUP BY a.userId)

-- CATEGORY - VC FE info
, CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
, CName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID WHERE CateSplit.categoryid <> '' and Userid = a.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM CateSplit as a where a.categoryid <> '' GROUP BY a.Userid)

-- SPECIALTY - VC SFE info
, SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate) t CROSS APPLY x.nodes('/M') as Split(a) )
, SpeName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + VS.name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '' and Userid = b.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SpecSplit as b where b.specialtyid <> '' GROUP BY b.Userid)

-- ADMISSION
, AdmissionRows(userId, CombinedText) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)

-- Category, Specialty, Bar Admission for Zensho
, ZenshoInfo as (
	select CA.userID
		, CA.candidateID
		, Concat(iif(AD.Admission = '' or AD.Admission is NULL,'',concat('Bar admission: ',AD.Admission,char(10)))
		, iif(CA.customText2 = '' or CA.customText2 is NULL,'',concat('General Work Function: ',CA.customText2,char(10)))
		, concat('Practice Area / Category: ',CName.Name,char(10))
		, iif(cast(CA.specialtyIDList as varchar(max)) = '' or CA.specialtyIDList is NULL,'',concat('Specialty: ',SpeName.name))) as MoreCandInfo
	from bullhorn1.Candidate CA
	left outer join admission AD on CA.userID = AD.Userid
	left outer join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
	left outer join CName on CA.userID = CName.Userid
	left outer join SpeName on CA.userID = SpeName.Userid
	where ca.isPrimaryOwner = 1)

-- DOCUMENT
, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF((SELECT ',' + name from bullhorn1.View_CandidateFile WHERE candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID)

-- Get candidates files
, tmp_6(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + concat(candidateFileID, fileExtension) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID) --where a.type = 'Resume') ==> get all candidates files

-- EMAIL
, mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(email,',',email2,',',email3)
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact where email like '%_@_%.__%' or email2 like '%_@_%.__%' or email3 like '%_@_%.__%' ) -- from bullhorn1.Candidate
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, mail5 (ID, email1, email2, email3) as (
		select pe.ID, email as email1, we.email2, oe.email3 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		left join (SELECT ID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 )

-- OWNER
, tmp_email_3 as (select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

-- EDUCATION
, EducationGroup as (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID)
, Education as (select EG.userID, UE.userEducationID, UE.school, convert(varchar(10),UE.graduationDate,110) as graduationDate, UE.degree, UE.major, UE.comments from EducationGroup EG left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)

-- NOTE
, tmp_note(Userid, Notes) as (SELECT Userid, STUFF((SELECT char(10) + 'Date Added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max)) from [bullhorn1].[BH_UserComment] WHERE Userid = a.Userid order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM [bullhorn1].[BH_UserComment] AS a GROUP BY a.Userid )

/* candidate-note */
, tmp_addednote as (
	SELECT CA.userID
		 ,concat('Date Registered: ',convert(varchar(10),dateAdded,120),char(10)
		 ,'Status: ',status,char(10)
		 ,iif(referredByUserID = '' or referredByUserID is NULL,'',concat('Reffered by UserID: ',referredByUserID,char(10)))
		 ,iif(referredBy = '' or referredBy is NULL,'',concat('Reffered by: ',referredBy,char(10)))
		 ,iif(phone2 = '' or phone2 is NULL,'',concat('Phone 2: ',phone2,char(10)))
		 ,iif(cast(desiredLocations as varchar(2)) = '' or desiredLocations is NULL,'',concat('Desired Locations: ',tmp_country.COUNTRY,char(10)))
		 ,iif(SN.SkillName = '' or SN.SkillName is NULL,'',concat('Skills: ',SN.SkillName,char(10)))
		 --,iif(BS.BusinessSector='' or BS.BusinessSector is NULL,'',concat('Business Sector: ',BS.BusinessSector,char(10)))
		 ,ZenshoInfo.MoreCandInfo) as AddedNote
		,CA.desiredLocations
		,tmp_country.ABBREVIATION
		,tmp_country.COUNTRY
	from bullhorn1.Candidate CA
	left join SkillName SN on CA.userID = SN.userId
	left join BusinessSector BS on CA.userID = BS.userId
	left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
	left join ZenshoInfo on CA.userID = ZenshoInfo.userID
	where CA.isPrimaryOwner = 1 )
--select top 100 * from tmp_addednote where AddedNote like '%Business Sector%'


, t1 as (
	select
                C.userID as '(userID)'
		, case C.gender when 'M' then 'MR' when 'F' then 'MISS'	else '' end as 'candidate-title'
		, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
		, C.candidateID as 'candidate-externalId'
		--, case when (ltrim(replace(C.firstName,'?','')) = '' or  C.firstName is null) then 'Firstname' else ltrim(replace(C.firstName,'?','')) end as 'candidate-firstName'
		--, case when (ltrim(replace(C.lastName,'?','')) = '' or  C.lastName is null) then concat('Lastname-',C.candidateID) else ltrim(replace(C.lastName,'?','')) end as 'candidate-Lastname'
                , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
		, C.middleName as 'candidate-middleName'
		, CONVERT(VARCHAR(10),C.dateOfBirth,120) as 'candidate-dob'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation	END as 'candidate-citizenship'
		, mail5.email1 as 'candidate-email' --, iif(C.email like '%_@_%.__%',C.email,concat('candidate-',C.userID,'@noemail.com'))
		, mail5.email2 as 'candidate-workEmail' --coalesce(C.email2,'') 
		, C.mobile as 'candidate-phone'
		, C.phone2 as 'candidate-homePhone'	
		, C.mobile as 'candidate-mobile'
		, C.workPhone as 'candidate-workPhone'
		, 'PERMANENT' as 'candidate-jobTypes'
		, C.address1 as 'candidate-address'
		, C.city as 'candidate-city'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation	END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		, C.state as 'candiadte-state'
		, cast(C.salaryLow as int) as 'candidate-currentSalary'
		, cast(C.salary as int) as 'candidate-desiredSalary'
		, Education.school as 'candidate-schoolName'
		, Education.graduationDate as 'candidate-graduationDate'
		, Education.degree as 'candidate-degreeName'
		, Education.major as '(candidate-major)'
		, SN.SkillName as 'candidate-skills'
		, C.companyName as 'candidate-company1'
		, C.occupation as 'candidate-jobTitle1'
		, C.companyName as 'candidate-employer1'
		, C.recruiterUserID as '(recruiterUserID)'
		, tmp_email_3.email as 'candidate-owners'
		, t4.finame as '(Candidate File)'
		, tmp_6.ResumeId as 'candidate-resume'
		, left(concat('Bullhorn Candidate ID: ',C.candidateID,char(10)
			,iif(mail5.email3 is null or mail5.email3 = '','',concat('Other email: ',mail5.email3,char(10)))
			,concat('Note: ',AN.AddedNote)
			--,concat('Note: ',replace(AN.AddedNote,'&amp;','&'))
			--,iif(sum.sum = '' or sum.sum is NULL,'',concat(replace(replace(replace(sum.URLList,'&amp;','&'),'&#x0D;',''),'x0A;',''),char(10)))
			,iif(sum.sum = '' or sum.sum is NULL,'',concat(sum.sum,char(10)))
			),32760) as 'candidate-note' -- removed C.comments because of duplication with bullhorn1.BH_CandidateHistory
		--, replace(left(cast(tmp_note.Notes as varchar(max)),32000),'&#x0D;','') as 'candidate-comments'
		, left(tmp_note.Notes,32760) as 'candidate-comments'
	from bullhorn1.Candidate C
	left join SkillName SN on C.userID = SN.userId
	left join tmp_country tc ON c.countryID = tc.code
	left join tmp_email_3 on C.recruiterUserID = tmp_email_3.recruiterUserID
	left join mail5 on C.userID = mail5.ID
	left join Education on C.userID = Education.userID
	left join t4 on t4.candidateUserID = C.userID
	left join tmp_6 on C.userID = tmp_6.candidateUserID
	left join tmp_note on C.userID = tmp_note.Userid
	left join tmp_addednote AN on C.userID = AN.Userid
	--left join (SELECT candidateID, STUFF((SELECT '  Summary: ' + convert(varchar(max),comments) + char(10) from  bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID FOR XML PATH ('')), 1, 2, '')  AS URLList FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID) sum on C.candidateID = sum.candidateID
	left join (SELECT candidateID, STUFF((SELECT case when (convert(varchar(max),comments) = '' or comments is null) then '' else char(10) + 'Date Added: ' + convert(varchar,dateAdded,120) + ' || ' + 'Candidate History: ' end + convert(varchar(max),comments) 
                        from bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS sum 
                        FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID) sum on C.candidateID = sum.candidateID
	where C.isPrimaryOwner = 1
	--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
	--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
	)

select * from t1 --65930
where [contact-firstName] like '%Sam%'