
with 
----- Resume's filename -----
--t3(candidateUserID, name) as (select a.candidateUserID, a.name from bullhorn1.View_CandidateFile a where a.type = 'Resume') -- 2473 lines
t3(candidateUserID, name) as (select a.candidateUserID, a.name from bullhorn1.View_CandidateFile a where a.type in ('CV','Resume'))
--select * from t3 where candidateUserID = 810 -- 3675 lines
, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF( (SELECT ',' + name from t3 WHERE candidateUserID = a.candidateUserID FOR XML PATH ('')), 1, 1, '' ) AS URLList FROM t3 AS a GROUP BY a.candidateUserID)
--select * from t4

------ Email -----
, tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact)
--select * from tmp_1
--select userID, email, CHARINDEX(email,',',0) from tmp_1
, tmp_2(userID, email) as ( select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1) ELSE email END as email from tmp_1)
, tmp_3(userID, email) as ( select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)	ELSE email END as email from tmp_2 )
--select * from tmp_3

----- Resume's ID name -----
--, tmp_5 (candidateUserID, name) as (select a.candidateUserID, concat(a.candidateFileID, a.fileExtension) from bullhorn1.View_CandidateFile a where a.type = 'Resume')
, tmp_5 (candidateUserID, name) as (select a.candidateUserID, concat(a.candidateFileID, a.fileExtension) from bullhorn1.View_CandidateFile a where a.type in ('CV', 'Resume'))
--select * from tmp_5 where candidateUserID = 810

, tmp_6(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF( (SELECT DISTINCT ',' + name from tmp_5 WHERE candidateUserID = a.candidateUserID FOR XML PATH ('')), 1, 1, '') AS URLList FROM tmp_5 AS a GROUP BY a.candidateUserID)
--select * from tmp_6 order by candidateUserID

----- Email by CandidateID -----
, tmp_email_1 as (select c.candidateID, case 
	when c.email is not null and c.email <> '' then c.email
	when c.email2 is not null and c.email2 <> '' then c.email2
	when c.email3 is not null and c.email3 <> '' then c.email3
	else ''	end as email from bullhorn1.Candidate c)
--select * from tmp_email_1
--select email, count(*) from tmp_email_1 group by email having count(*) > 1
, tmp_email_2 as (select email, min(candidateID) as candidateID from tmp_email_1 group by email)
--select * from tmp_email_2

----- Notes -----
, tmp_note(Userid, Notes) as (SELECT Userid,STUFF( (SELECT DISTINCT ' || ' + convert(varchar(10), dateAdded, 120) + ': ' + cast(comments as varchar(max)) from  [bullhorn1].[BH_UserComment] WHERE Userid = a.Userid FOR XML PATH ('')), 1, 4, '') 
AS URLList FROM  [bullhorn1].[BH_UserComment] AS a GROUP BY a.Userid)
--select * from tmp_note


-----Skill-------
, skill0 (userid, skillIDList) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS skillIDList 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(skillIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.BH_UserContact CA where CA.status not like '%Archive%' and CA.status <> '' )t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
--select * from skill0
--select cast(skillIDList as int) from skill0

, skill1(userid, skillIDList) as (select distinct userid, cast(skillIDList as varchar(max)) from (select userid, skillIDList from skill0) a)
--select * from skill1

, skill2(userid, name) as (select skill1.userid, cast(BS.name as varchar(max)) from skill1 inner join bullhorn1.BH_SkillList BS ON skill1.skillIDList = BS.skillID)
--select * from t --where userid = 3144 --order by userid 

, skill3(userid, name) as (SELECT userid, name = 
    STUFF((SELECT DISTINCT ', ' + name
           FROM skill2 b 
           WHERE b.userid = a.userid 
          FOR XML PATH('')), 1, 2, '')
FROM skill2 a
GROUP BY userid
)

--select * from skill3 --where userid = 893 order by userid
----------------

------------candidate-employeeType---------------
, employmentPreference0 (userid,employmentPreference) as (select userid,employmentPreference from bullhorn1.BH_UserContact)
 --select * from employeetype0
, employmentPreference1 (userid, employmentPreference) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS employmentPreference 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(employmentPreference as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.BH_UserContact CA where CA.status not like '%Archive%' and CA.status <> '' )t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n))
--select * from employmentPreference1 
, employmentPreference2 as (select userid, employmentPreference, ROW_NUMBER () OVER ( PARTITION BY userid order by userid ) as ord from employmentPreference1)
--select * from employmentPreference2 where ord = 1

------------businessSector---------------
, category (categoryID,name) as (select categoryID,name from bullhorn1.BH_Category)
, tmp_businessSectorIDList1 (userid, BussinessID) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS BussinessID 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.BH_UserContact CA where CA.status not like '%Archive%' and CA.status <> '' )t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
--select cast(BussinessID as int) from tmp_businessSectorIDList1

, tmp_businessSectorIDList2(userid, bussinesid) as (select distinct userid, cast(BussinessID as varchar(max)) from (select userid, BussinessID from tmp_businessSectorIDList1) a)
--select * from tmp_businessSectorIDList2
--, t1(userId, IndustryName) as (select tmp_1.userid, BS.name from tmp_1 inner join bullhorn1.BH_BusinessSector BS ON tmp_1.bussinesid = BS.businessSectorID)
--, t1(userid, name, bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--, t1(userid,name,bussinesid) as (select tmp_businessSectorIDList2.userid, BS.name, tmp_businessSectorIDList2.bussinesid from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--select * from t1 order by userid
, t(userid, name) as (select tmp_businessSectorIDList2.userid, cast(BS.name as varchar(max)) from tmp_businessSectorIDList2 inner join bullhorn1.BH_BusinessSector BS ON tmp_businessSectorIDList2.bussinesid = BS.businessSectorID)
--select * from t where userid = 3144 --order by userid 

, tmp_businessSectorIDList3(userid, name) as (SELECT userid, name = 
    STUFF((SELECT DISTINCT ', ' + name
           FROM t b 
           WHERE b.userid = a.userid 
          FOR XML PATH('')), 1, 2, '')
FROM t a
GROUP BY userid
)
--select * from tmp_businessSectorIDList3 where userid = 893
--select * from tmp_businessSectorIDList3 order by userid

----------------------------
, note as (select 
ca.candidateid 
--, case when (ca.comments = '' OR ca.comments is NULL) THEN '' ELSE concat('Notes: ',ca.comments,char(10)) END  as 'comments'
, case when (ISNULL(REPLACE(cast(ca.comments as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Notes: ',ca.comments) END as 'comments'

, case when (ca.employeeType = '' OR ca.employeeType is NULL) THEN '' ELSE concat('Employment Type / Job Type: ',ca.employeeType,char(10)) END as 'employeeType'
, case when (cast(tmp_businessSectorIDList3.name as varchar(max)) = '' OR tmp_businessSectorIDList3.name is NULL) THEN '' ELSE REPLACE(REPLACE(concat('Industry: ',tmp_businessSectorIDList3.name,char(10)), '&amp;', '&'), 'amp;', '') END as 'Industry'
, case when (t1.name = '' OR t1.name is NULL) THEN '' ELSE concat('Functional Expertise: ',t1.name,char(10)) END as 'categoryname'
--, case when (uc.specialtyIDList = '' OR uc.specialtyIDList is NULL) THEN '' ELSE concat('Sub functional expertise: ',uc.specialtyIDList,char(10)) END as 'uc.specialtyIDList'
, case when (ISNULL(REPLACE(cast(uc.specialtyIDList as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Sub functional expertise: ',uc.specialtyIDList) END as 'specialtyIDList'

--, case when (uc.skillIDList = '' OR uc.skillIDList is NULL) THEN '' ELSE concat('Skills: ',uc.skillIDList,char(10)) END as 'uc.skillIDList'
--, case when (ISNULL(REPLACE(cast(uc.skillIDList as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Skills: ',uc.skillIDList) END as 'skillIDList'

, case when (uc.employmentPreference = '' OR uc.employmentPreference is NULL) THEN '' ELSE concat('Desired Industry: ',uc.employmentPreference,char(10)) END as 'employmentPreference'

--, case when (uc.customTextBlock1 = '' OR uc.customTextBlock1 is NULL) THEN '' ELSE concat('Skills: ',uc.customTextBlock1,char(10)) END as 'uc.customTextBlock1'
, case when (ISNULL(REPLACE(cast(uc.customTextBlock1 as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Skills: ',uc.customTextBlock1) END as 'customTextBlock1'

--, case when (uc.skillSet = '' OR uc.skillSet is NULL) THEN '' ELSE concat('Skills: ',uc.skillSet,char(10)) END as 'uc.skillSet'
, case when (ISNULL(REPLACE(cast(uc.skillSet as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Skills: ',uc.skillSet) END as 'skillSet'

, case when (uc.customText4 = '' OR uc.customText4 is NULL) THEN '' ELSE concat('Candidate picker: ',uc.customText4,char(10)) END as 'customText4'
, case when (uc.customtext1 = '' OR uc.customtext1 is NULL) THEN '' ELSE concat('Customtext1: ',uc.customtext1,char(10)) END as 'customtext1'
, case when (uc.fax = '' OR uc.fax is NULL) THEN '' ELSE concat('Skype ID: ',uc.fax,char(10)) END as 'fax'
, case when (uc.customtext11 = '' OR uc.customtext11 is NULL) THEN '' ELSE concat('We ve Met: ',uc.customtext11,char(10)) END as 'customtext11'
, case when (ca.type = '' OR ca.type is NULL) THEN '' ELSE concat('Representation Commitment: ',ca.type,char(10)) END as 'type'
, case when (ca.status = '' OR ca.status is NULL) THEN '' ELSE concat('Active/Passive and general VC Pipeline: ',ca.status,char(10)) END as 'status'
, case when (ca.candidateSourceID = '' OR ca.candidateSourceID is NULL) THEN '' ELSE concat('Subjective Assessment: ',ca.candidateSourceID,char(10)) END as 'candidateSourceID'
, case when (uc.workAuthorized = '' OR uc.workAuthorized is NULL) THEN '' ELSE concat('Valid work permit: ',uc.workAuthorized,char(10)) END as 'workAuthorized'
, case when (uc.customtext10 = '' OR uc.customtext10 is NULL) THEN '' ELSE concat('Will relocate: ',uc.customtext10,char(10)) END as 'customtext10'
, case when (ISNULL(REPLACE(cast(uc.desiredLocations as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Work Aspirations Desired locations: ',uc.desiredLocations) END as 'desiredLocations'
, case when (uc.hourlyRateLow = '' OR uc.hourlyRateLow is NULL) THEN '' ELSE concat('Contract rate: ',uc.hourlyRateLow,char(10)) END as 'hourlyRateLow'
, case when (uc.hourlyRate = '' OR uc.hourlyRate is NULL) THEN '' ELSE concat('Desired contract rate: ',uc.hourlyRate,char(10)) END as 'hourlyRate'
--, case when (uc.description = '' OR uc.description is NULL) THEN '' ELSE concat('Original CV: ',uc.description,char(10)) END as 'uc.description'
, case when (ISNULL(REPLACE(cast(uc.description as nvarchar(max)),CHAR(13),''), '') = '') THEN '' ELSE concat('Original CV: ',uc.description) END as 'description'
, case when (uc.customtext2 = '' OR uc.customtext2 is NULL) THEN '' ELSE concat('School name: ',uc.customtext2,char(10)) END as 'customtext2'
, case when (uc.referredByUserID = '' OR uc.referredByUserID is NULL) THEN '' ELSE concat('Referral Source: ',uc.referredByUserID,char(10)) END as 'referredByUserID'
	from bullhorn1.BH_Candidate ca
	left join bullhorn1.BH_UserContact UC on ca.recruiterUserID = uc.userID
	left join tmp_businessSectorIDList3 ON uc.userid = tmp_businessSectorIDList3.userid
	left join category t1 ON UC.categoryID = t1.categoryID
  )

--select * from note
--select candidateid,count(*) from notes group by candidateid having count(*) > 1
, notes as (select candidateid, concat(comments,employeeType,Industry,categoryname,specialtyIDList,employmentPreference,customTextBlock1,skillSet,customText4,customtext1,fax,customtext11,type,status,candidateSourceID,workAuthorized,customtext10,desiredLocations,hourlyRateLow,hourlyRate,description,customtext2,referredByUserID) as 'candidate-note' from note)
--select * from notes
--select jobPostingID, count(*) from tmp_6 group by jobPostingID having count(*) > 1

------------------
, t1 as (select C.userID --NO CandidateID = UserContact ID
	, case C.gender when 'M' then 'MR' when 'F' then 'MS' else '' end as 'candidate-title'
	, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
	, C.candidateID as 'candidate-externalId' --NO
	, C.firstName as 'candidate-firstName'
	, C.[lastName] as 'candidate-Lastname'
	, C.middleName as 'candidate-middleName'
	, CONVERT(VARCHAR(10),C.dateOfBirth,110) as 'candidate-dob'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation END as 'candidate-citizenship'
	, REPLACE(REPLACE(case 
		when (C.email is not null and C.email <> '' and C.email like '%@%') then C.email
		when (C.email2 is not null and C.email2 <> '' and C.email2 like '%@%') then C.email2
		when (C.email3 is not null and C.email3 <> '' and C.email3 like '%@%') then C.email3
		else ''	end, '?', ''), ' ', '') as 'candidate-email'
	, C.mobile as 'candidate-mobile'
	, C.phone as 'candidate-phone'	
	, C.workPhone as 'candidate-workPhone'
	, concat(C.address1,' ',C.address2) as 'candidate-address'
	, C.city as 'candidate-city'
	, C.state as 'candidate-state'
	, C.zip as 'candidate-zipCode'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation END as 'candidate-Country'
	, C.salaryLow as 'candidate-currentSalary'
	, C.salary as 'candidate-desiredSalary'

	--, case when (c.employeeType = '' OR c.employeeType is NULL) THEN '' ELSE concat('Employment Type / Job Type: ',c.employeeType,char(10)) END as 'candidate-employeeType'
	--, UPPER(C.employmentPreference) as  'candidate-type'
	, UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tmp_EP.employmentPreference,'Permanent','FULL_TIME'),'Freelance','PART_TIME'),'Part-time','PART_TIME'),'Contract','CASUAL'),'Remote','CASUAL'),'Owner/Partner','CASUAL')) as 'candidate-employmentPreference'
	, skill3.name as 'skills'	
	, UE.[school] as 'candidate-schoolName' --NO
	, CONVERT(VARCHAR(10),UE.graduationDate,110) as 'candidate-graduationDate' --NO
	, UE.[degree] as 'candidate-degreeName' --NO
	, UE.[gpa] as 'candidate-gpa' --NO
	, C.companyName as 'candidate-company1'
	--, C.companyName as 'candidate-employer1'
	, C.occupation as 'candidate-jobTitle1'
	--, C.recruiterUserID
	, t3.email as 'candidate-owners' --NO
	, t4.finame as 'candidate-resume-filename' --filename --NO
	, tmp_6.ResumeId as 'candidate-resume' --IDname --NO
	--, C.comments as 'candidate-note'
	, replace(left(cast(tmp_note.Notes as varchar(max)),30000),'&#x0D;','') as 'candidate-comments'
from bullhorn1.Candidate C
left join tmp_country tc ON C.countryID = tc.code
left join bullhorn1.BH_UserContact UC on C.userID = UC.userID
--left join bullhorn1.BH_JobPosting JP ON C.recruiterUserID = JP.userID
--left join bullhorn1.BH_Client Cl ON UC.userID = Cl.userID

left join tmp_3 t3 ON C.recruiterUserID = t3.userID
left join (select userID, min(userEducationID) as userEducationID from [bullhorn1].[BH_UserEducation] group by userID) UE_2 on C.userID = UE_2.userID
left join employmentPreference2 tmp_EP on uc.userID = tmp_EP.userid
left join (select * from [bullhorn1].[BH_UserEducation] where isDeleted = 0) UE on UE.userEducationID = UE_2.userEducationID

left join t4 on C.userID = t4.candidateUserID --5861 line
left join tmp_6 on C.userID = tmp_6.candidateUserID
left join tmp_note on C.userID = tmp_note.Userid
left join skill3 on UC.userID = skill3.userid
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
where C.status not like '%Archive%' and tmp_EP.ord = 1
)



select t1.*,notes.[candidate-note] from t1 left join notes on t1.[candidate-externalId] = notes.candidateID
--select t1.* from t1 --inner join tmp_email_2 on t1.[candidate-externalId] = tmp_email_2.candidateID order by userID

