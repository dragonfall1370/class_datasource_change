/*
with
-- EMAIL
  mail1 (ID,email) as (select C.candidateID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isdeleted <> 1 and C.status <> 'Archive' *//*and C.isPrimaryOwner = 1*//* )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed
*/

with
mail1 (ID,email) as (
       select C.candidateID
	      , replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
	from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID
	cross apply string_split( concat(UC.email,' ',UC.email2,' ',UC.email3) ,' ')
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and C.isdeleted <> 1 and C.status <> 'Archive'
--       select REFERENCE
--	, replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
--	from PROP_EMAIL
--	cross apply string_split(EMAIL_ADD,' ')
--	where EMAIL_ADD like '%_@_%.__%' and REFERENCE in (61065,43945)
	)
--select * from mail1 where id in (21244,21818) and email <> ''

, mail2 (ID,email,rn,ID_rn) as (
       select distinct ID
              , trim(' ' from email) as email
              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
              , row_number() over(partition by ID order by trim(' ' from email)) as ID_rn --distinct if contacts may have more than 1 email
	from mail1
	where email like '%_@_%.__%'
	)
--select * from mail2

, ed (ID,email) as (
       select ID
	      , case when rn > 1 then concat(email,'_',rn)
	else email end as email
	from mail2
	where email is not NULL and email <> ''
	and ID_rn = 1
	)
, e2 (ID,email) as (select ID, email from mail2 where ID_rn = 2)
, e3 (ID,email) as (select ID, email from mail2 where ID_rn = 3)	
--select * from mail1 where ID in (391, 2447) or email like '%lburlovich@challenger.com.au%'

-- BusinessSector >>> INDUSTRY
--select distinct ltrim(rtrim(name)) from bullhorn1.BH_BusinessSectorList
, INDUSTRY0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast( ltrim(rtrim( convert(nvarchar(max),businessSectorIDList) )) as nvarchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM bullhorn1.Candidate where isdeleted <> 1 and status <> 'Archive') t CROSS APPLY x.nodes('/M') AS Split(a) )
, INDUSTRY(userId, BusinessSector) as (SELECT userid, STRING_AGG( ltrim(rtrim( convert(nvarchar(max),BSL.name) )),', ' ) WITHIN GROUP (ORDER BY BSL.name) name from INDUSTRY0 left join bullhorn1.BH_BusinessSectorList BSL ON INDUSTRY0.businessSectorID = BSL.businessSectorID WHERE INDUSTRY0.businessSectorID <> '' GROUP BY userid )
--select * from INDUSTRY


-- CATEGORY >>> FUNCTIONAL EXPERTISE
, CATEGORY(candidateID, userid, categoryIDList, categoryID, name) as (
       select C.candidateID, C.userid, C.categoryIDList, C.categoryID, UC.name
       FROM bullhorn1.Candidate C
       left join ( select UC.userID, UC.categoryID, replace(replace(replace(replace(replace(name,'4294:',''),'4295:',''),'4296:',''),'4297:',''),'850:','') as name from BULLHORN1.View_UserCategory UC left join bullhorn1.BH_CategoryList CL ON UC.categoryid = CL.categoryID ) UC on UC.userid = C.userID
       where UC.name is not null and UC.name <> '' and C.isdeleted <> 1 and C.status <> 'Archive' --and C.userid in (165180)
       )
-- select distinct name from CATEGORY
-- select * from CATEGORY where userid in (165180,165184 ,165199, 161585)


-- SPECIALTY >>> SUB FUNCTIONAL EXPERTISE
, SPECIALTY(candidateID, userid, specialtyIDList, SpecialtyID, name) as (
       select C.candidateID, C.userid, C.specialtyIDList, UC.SpecialtyID, UC.name
       FROM bullhorn1.Candidate C
       left join ( select US.userID, US.SpecialtyID, VS.name as name from BULLHORN1.View_UserSpecialty US left join bullhorn1.View_Specialty VS ON US.SpecialtyID = VS.specialtyID ) UC on UC.userid = C.userID
       where UC.name is not null and UC.name <> '' and C.isdeleted <> 1 and C.status <> 'Archive' --and C.userid in (165180)
       )
-- select distinct name from SPECIALTY
-- select * from SPECIALTY where userid in (155843,165184 ,165199, 161585)


-- SKILL
, SKILL (candidateID,userid, SkillName) as ( 
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
, AdmissionRows(userId, CombinedText, dateadded) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText, COI.dateadded from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT userid, STRING_AGG( CombinedText,'|| ' ) WITHIN GROUP (ORDER BY dateadded) name from AdmissionRows where CombinedText <> '' GROUP BY userid )
--select top 10 * from admission


-- Reference Summary
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;
, ReferenceSummary(userId, reference) as (
       SELECT userId
       , STRING_AGG(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                   + Coalesce('Candidate Title: ' + NULLIF(convert(nvarchar(max),r.candidateTitle), '') + char(10), '')
                                   + Coalesce('Client Corporation: ' + NULLIF(convert(nvarchar(max),r.clientCorporationID), '') + char(10), '')
                                   + Coalesce('Company: ' + NULLIF(convert(nvarchar(max),r.companyName), '') + char(10), '')
                                   + Coalesce('Date Added: ' + NULLIF(convert(nvarchar(max),r.dateAdded), '') + char(10), '')
                                   + Coalesce('Employment End: ' + NULLIF(convert(nvarchar(max),r.employmentEnd), '') + char(10), '')
                                   + Coalesce('Employment Start: ' + NULLIF(convert(nvarchar(max),r.employmentStart), '') + char(10), '')
                                   + Coalesce('Job Posting: ' + NULLIF(convert(nvarchar(max),r.jobPostingID), '') + char(10), '')
                                   + Coalesce('Reference Email: ' + NULLIF(convert(nvarchar(max),r.referenceEmail), '') + char(10), '')
                                   + Coalesce('Reference First Name: ' + NULLIF(convert(nvarchar(max),r.referenceFirstName), '') + char(10), '')
                                   + Coalesce('Reference Last Name: ' + NULLIF(convert(nvarchar(max),r.referenceLastName), '') + char(10), '')
                                   + Coalesce('Reference Phone: ' + NULLIF(convert(nvarchar(max),r.referencePhone), '') + char(10), '')
                                   + Coalesce('Reference Title: ' + NULLIF(convert(nvarchar(max),r.referenceTitle), '') + char(10), '')
                                   + Coalesce('Reference: ' + NULLIF(convert(nvarchar(max),r.referenceUserID), '') + char(10), '')
                                   + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),r.status), '') + char(10), '')
                                   + Coalesce('Reference ID: ' + NULLIF(convert(nvarchar(max),r.userReferenceID), '') + char(10), '')
                                   + Coalesce('Years Known: ' + NULLIF(convert(nvarchar(max),r.yearsKnown), '') + char(10), '')
                            , 1, 0, '')
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as es
       ,char(10) ) WITHIN GROUP (ORDER BY dateadded) reference
       FROM bullhorn1.BH_UserReference r GROUP BY userId        
       )
--select top 10 * from referenceSummary where userid in (163454);


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
              , convert(varchar(10),UE.graduationDate,110) as graduationDate
              , UE.major
              , UE.school
              , UE.startDate
              , UE.state
              , UE.userEducationID       
       from EducationGroup EG left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)
-- Education Summary
, EducationSummary(userId, es) as (
       SELECT e.userId
       , STRING_AGG(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                     coalesce('Date Added: ' + nullif(cast(e.dateAdded as nvarchar(max)), '') + char(10), '')                   
                                   + coalesce('Certification: ' + nullif(cast(e.certification as nvarchar(max)), '') + char(10), '')
                                   + coalesce('City: ' + nullif(cast(e.city as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Comments: ' + nullif(cast(e.comments as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Country: ' + nullif(cast(e.customText1 as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Degree: ' + nullif(cast(e.degree as nvarchar(max)), '') + char(10), '')
                                   + coalesce('End Date: ' + nullif(cast(e.endDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Expiration Date: ' + nullif(cast(e.expirationDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('GPA: ' + nullif(cast(e.gpa as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Graduation Date: ' + nullif(cast(e.graduationDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Major: ' + nullif(cast(e.major as nvarchar(max)), '') + char(10), '')
                                   + coalesce('School: ' + nullif(cast(e.school as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Start Date: ' + nullif(cast(e.startDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('State: ' + nullif(cast(e.state as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Education ID: ' + nullif(cast(e.userEducationID as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Reference: ' + nullif(cast(r.reference as nvarchar(max)), '') + char(10), '')
                            , 1, 0, '')
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as es
       ,char(10) ) WITHIN GROUP (ORDER BY dateadded) es
       FROM bullhorn1.BH_UserEducation e
       GROUP BY e.userId        
       )
--select top 10 * from EducationSummary where userid in (163454);
-- select * from bullhorn1.BH_UserEducation where customText1 is not null


-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;


-- Employment History -- select *  from bullhorn1.BH_userWorkHistory
, EmploymentHistory(userId, eh) as (
       SELECT userId
         , STRING_AGG(
--       , STUFF(( 
--                     select char(10) + 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                              coalesce('Bonus: ' + nullif(cast(bonus as nvarchar(max)), '') + char(10), '')
                            + coalesce('Client Corporation: ' + nullif(cast(clientCorporationID as nvarchar(max)), '') + char(10), '')
                            + coalesce('Comments: ' + nullif(cast(comments as nvarchar(max)), '') + char(10), '')
                            + coalesce('Commission: ' + nullif(cast(commission as nvarchar(max)), '') + char(10), '')
                            + coalesce('Company Name: ' + nullif(cast(companyName as nvarchar(max)), '') + char(10), '')
                            + coalesce('Date Added: ' + nullif(cast(dateAdded as nvarchar(max)), '') + char(10), '')
                            + coalesce('End Date: ' + nullif(cast(endDate as nvarchar(max)), '') + char(10), '')
                            + coalesce('Job Posting: ' + nullif(cast(title as nvarchar(max)), '') + char(10), '') --jobPostingID
                            --+ coalesce('Placement: ' + nullif(cast(placementID as nvarchar(max)), '') + char(10), '')
                            + coalesce('Salary Low: ' + nullif(cast(salary1 as nvarchar(max)), '') + char(10), '')
                            + coalesce('Salary High: ' + nullif(cast(salary2 as nvarchar(max)), '') + char(10), '')
                            + coalesce('Salary Type: ' + nullif(cast(salaryType as nvarchar(max)), '') + char(10), '')
                            + coalesce('Start Date: ' + nullif(cast(startDate as nvarchar(max)), '') + char(10), '')
                            + coalesce('Termination Reason: ' + nullif(cast(terminationReason as nvarchar(max)), '') + char(10), '')
                            + coalesce('Title: ' + nullif(cast(title as nvarchar(max)), '') + char(10), '')
                            --+ coalesce('User Work History ID: ' + nullif(cast(userWorkHistoryID as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Comments: ' + nullif(replace([dbo].[udf_StripHTML](comments),'Â ',''), '') + char(10), '')
                                   --+ coalesce('Comments: ' + nullif(replace([dbo].[fn_ConvertHTMLToText](comments),'Â ',''), '') + char(10), '')
                            , 1, 0, '') 
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as eh
--                     from bullhorn1.BH_userWorkHistory
--       WHERE userId = a.userId order by startDate desc
--       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
--       FROM bullhorn1.BH_userWorkHistory as a
--       left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
--       --where userid in (164043)
--       GROUP BY a.userId 
       ,char(10) ) WITHIN GROUP (ORDER BY startDate desc) eh
       FROM bullhorn1.BH_userWorkHistory GROUP BY userId
       )
--select top 10 * from EmploymentHistory where userid in (164043);


-- Web Responses
, wr as (
        select jr.userid, jp.title,jr.status
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isdeleted <> 1 and CA.status <> 'Archive') CAI on JR.userID = CAI.CandidateUserID
        left join bullhorn1.BH_JobPosting  jp on jp.jobPostingID = jr.jobPostingID )
, wr1 as (SELECT userID, STRING_AGG( concat('Title: ',title,' - Status: ',status) ,char(10)) WITHIN GROUP (ORDER BY title) name from wr GROUP BY userID )
--select top 10 * from wr1


-- Latest Comment
, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_UserComment )

/*-- OWNER
, owner as (select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isdeleted <> 1 and CA.status <> 'Archive')
-- Secondary OWNER
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select owner2a.userid, UC.email, UC.name from owner2a left join (select userid, email, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
--, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
, owner2c as (SELECT userID, STRING_AGG( email,',') WITHIN GROUP (ORDER BY email) email from owner2b where email like '%_@_%.__%' GROUP BY userID ) */

, owner as ( select ca.candidateid, ca.recruiterUserID, ca.owneruseridlist, concat(ca.recruiterUserID,',', ca.ownerUserIDList) as owners from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isdeleted <> 1 and CA.status <> 'Archive')
--select ca.owneruseridlist from bullhorn1.Candidate CA where owneruseridlist like '%,%'
, owner2a as (SELECT candidateid, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT candidateid, CAST ('<M>' + REPLACE(convert(nvarchar(max),owners),',','</M><M>') + '</M>' AS XML) AS Data FROM owner where owners is not null and owners <> '') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
--, owner2b as (select distinct owner2a.candidateid, UC.email, UC.name from owner2a left join (select ca.candidateid, uc.email, uc.name from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.UserID = UC.userID ) UC on UC.candidateid = owner2a.String)
, owner2b as ( select distinct owner2a.candidateid, UC.email, UC.name from owner2a left join bullhorn1.BH_UserContact UC on UC.userid = owner2a.String)
--select top 10 * from owner2b where candidateid in (31595) and email <> '' and email is not null
, owner2c as (SELECT candidateid, STRING_AGG( email,',') WITHIN GROUP (ORDER BY email) email from owner2b where email like '%_@_%.__%' GROUP BY candidateid )
--select top 10 * from owner2c where candidateid in (31595) and email like '%,%' --candidateid in (39194)


-- COMMENT
--, comment(Userid, comment) as (SELECT Userid, STUFF((SELECT char(10) + 'Date Added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max)) from [bullhorn1].[BH_UserComment] WHERE Userid = a.Userid order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM [bullhorn1].[BH_UserComment] AS a GROUP BY a.Userid )
--, summary(candidateID,summary) as (SELECT candidateID, STUFF((SELECT coalesce(char(10) + 'Date Added: ' + convert(varchar,dateAdded,120) + ' || ' + 'Candidate History: ' + nullif(convert(varchar(max),comments), ''), '') from bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS summary FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID)

-- DOCUMENT
, files(candidateUserID, files) as (SELECT candidateUserID, STRING_AGG( concat(candidateFileID, fileExtension),',' ) WITHIN GROUP (ORDER BY candidateFileID) files from bullhorn1.View_CandidateFile GROUP BY candidateUserID)
--select top 10 * from files
, placementfiles(userID, files) as (SELECT userID, STRING_AGG( concat(placementFileID, fileExtension),',' ) WITHIN GROUP (ORDER BY placementFileID) files from bullhorn1.View_PlacementFile GROUP BY userID)
--select top 10 * from placementfiles
, doc(Userid, files) as ( select f.candidateUserID, STRING_AGG( f.files,',' ) WITHIN GROUP (ORDER BY f.files) files from (SELECT * from files UNION ALL SELECT * from placementfiles) f GROUP BY f.candidateUserID )
--select top 10 * from doc


-- NOTE
, note as (
	SELECT CA.candidateid
		 , Stuff(coalesce('BH Candidate ID: ' + nullif(convert(nvarchar(max),CA.userID), '') + char(10), '')
                     + Coalesce('Web Source (ADMIN ONLY): ' + NULLIF(convert(nvarchar(max),ca.customText13), '') + char(10), '')
                     + Coalesce('Is This a Referral?: ' + NULLIF(convert(nvarchar(max),ca.customText14), '') + char(10), '')
                     + Coalesce('Resume URL: ' + NULLIF(convert(nvarchar(max),ca.customText15), '') + char(10), '')
                     + Coalesce('Profile 1: ' + NULLIF(convert(nvarchar(max),ca.customTextBlock3), '') + char(10), '')
                     + Coalesce('Profile 2: ' + NULLIF(convert(nvarchar(max),ca.customTextBlock4), '') + char(10), '')

--                     + Coalesce('Transportation Options: ' + NULLIF(convert(nvarchar(max),ca.customText11), '') + char(10), '')
--                     + Coalesce('Portfolio URL 2: ' + NULLIF(convert(nvarchar(max),ca.customText12), '') + char(10), '')
--                     + Coalesce('Source Details: ' + NULLIF(convert(nvarchar(max),ca.customText16), '') + char(10), '')
--                     --+ Coalesce('Emergency Contact Name: ' + NULLIF(convert(nvarchar(max),ca.customText18), '') + char(10), '')
--                     --+ Coalesce('Emergency Contact Phone: ' + NULLIF(convert(nvarchar(max),ca.customText19), '') + char(10), '')
--                     + Coalesce('Willing to Relocate to: ' + NULLIF(convert(nvarchar(max),ca.customText2), '') + char(10), '')
--                     + Coalesce('Client-Side/Agency: ' + NULLIF(convert(nvarchar(max),ca.customText20), '') + char(10), '')
--                     + Coalesce('Employment Availability Type: ' + NULLIF(convert(nvarchar(max),ca.customText4), '') + char(10), '')
--                     + Coalesce('Service Office: ' + NULLIF(convert(nvarchar(max),ca.customText5), '') + char(10), '')
--                     --+ Coalesce('Marketing Opt-In: ' + NULLIF(convert(nvarchar(max),ca.customText6), '') + char(10), '')
--                     --+ Coalesce('Critical Info: ' + NULLIF(convert(nvarchar(max),ca.customTextBlock2), '') + char(10), '')
--                     + Coalesce('Date Available: ' + NULLIF(convert(nvarchar(max),ca.dateAvailable), '') + char(10), '')
--                     
--                     + Coalesce('Willing to Commute to: ' + NULLIF(convert(nvarchar(max),ca.desiredLocations), '') + char(10), '')
--                     --+ Coalesce('Employee Type: ' + NULLIF(convert(nvarchar(max),ca.employeeType), '') + char(10), '')
--                     + Coalesce('Employment Preference: ' + NULLIF(convert(nvarchar(max),ca.employmentPreference), '') + char(10), '')
--                     --+ Coalesce('StaffTrak Person ID: ' + NULLIF(convert(nvarchar(max),ca.externalID), '') + char(10), '')
--                     --+ coalesce('Latest Comment: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](lc.comments), '') + char(10), '')
--                     + Coalesce('Referred By (Other): ' + NULLIF(convert(nvarchar(max),ca.referredBy), '') + char(10), '')
--                     + coalesce('Referred By: ' + nullif(cast(CA.referredByUserID as nvarchar(max)), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
--                     + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),ca.status), '') + char(10), '')
--                     --+ Coalesce('Interview Notes: ' + NULLIF(convert(nvarchar(max),ca.comments), '') + char(10), '') --Comments
--                     + coalesce('Resume: ' + nullif(UC1.description, '') + char(10), '')
                     --+ coalesce('CV: ' + nullif(UW.description, '') + char(10), '')
--                     + coalesce('Available Until: ' + nullif(cast(CA.dateAvailableEnd as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Bar admission: ' + nullif(cast(AD.Admission as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Business Sector: ' + nullif(cast(BS.BusinessSector as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Current Hourly Rate: ' + nullif(cast(CA.hourlyRateLow as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Date Available: ' + nullif(cast(CA.dateAvailable as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Date Registered: ' + nullif(convert(varchar(10),CA.dateAdded,120), '') + char(10), '')
--                     + coalesce('Description: ' + nullif(cast(CA.description as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Desired Hourly Rate: ' + nullif(cast(CA.hourlyRate as nvarchar(max)), '') + char(10), '')
--                     + coalesce('General Comment: ' + nullif(cast(CA.comments as nvarchar(max)), '') + char(10), '')
--                     + coalesce('ID Number: ' + nullif(cast(CA.ssn as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Opted In - SMS Messages: ' + nullif(cast(CA.smsOptIn as nvarchar(max)), '') + char(10), '')                
--                     + coalesce('Other email: ' + nullif(cast(e3.email as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Other Phone: ' + nullif(cast(CA.phone2 as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Placements: ' + nullif(cast(pm.status as nvarchar(max)), '') + char(10), '') --CA.activePlacements
--                     + coalesce('Practice Area / Category: ' + nullif(cast(CName.Name as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Reffered by UserID: ' + nullif(cast(CA.referredByUserID as nvarchar(max)), '') + char(10), '')                    
--                     + coalesce('Registered By: ' + nullif(cast(CA.recruiterUserID) as nvarchar(max), '') + ' - ' + UC2.firstname + ' ' + UC2.lastname + char(10), '')
--                     + coalesce('Secondary Owners: ' + nullif(cast(owner2c.name as nvarchar(max)), '') + char(10), '') --CA.secondaryOwners
--                     + coalesce('Skills: ' + nullif(cast(SN.SkillName as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Skype: ' + nullif(cast(CA.phone2 as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Source: ' + nullif(cast(CA.source as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Specialty: ' + nullif(cast(CA.specialtyIDList as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Summary: ' + nullif(cast(summary.summary as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Visa Status: ' + nullif(cast(ca.EmployeeType as nvarchar(max)), '') + char(10), '')
--                     + coalesce('Web Responses: ' + nullif(cast(wr1.name as nvarchar(max)), '') + char(10), '') --CA.jobResponseJobPostingID                        
--                     + coalesce('Willing to Relocate: ' + nullif(cast(CA.willRelocate as nvarchar(max)), '') + char(10), '')                       
--                     + coalesce('LTD Company Address: ' + nullif(cast( concat(CA.secondaryaddress1,' ',CA.secondaryAddress2) as nvarchar(max)), '') + char(10), '')
--                     + coalesce('LTD Company City: ' + nullif(cast(CA.secondaryCity as nvarchar(max)), '') + char(10), '')
--                     + coalesce('LTD Company Country: ' + nullif(cast(tmp_country.country as nvarchar(max)), '') + char(10), '')
--                     + coalesce('LTD Company Post Code: ' + nullif(cast(CA.secondaryZip as nvarchar(max)), '') + char(10), '')
--                     + coalesce('LTD Company County: ' + nullif(cast(CA.secondaryState as nvarchar(max)), '') + char(10), '')
                     , 1, 0, '') as note
	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
       --left join e3 on CA.userID = e3.ID
--	left join (select userid, firstname, lastname from bullhorn1.BH_UserContact) UC ON UC.userID = CA.referredByUserID
--	left join (select userid, trim([bullhorn1].[fn_ConvertHTMLToText](description)) as description from bullhorn1.BH_UserContact) UC1 on UC1.userID = CA.UserID
--       left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact ) UC2 ON UC2.userID = CA.recruiterUserID
--       left join tmp_country on cast(CA.secondaryCountryID as varchar(2)) = tmp_country.code
       /*left join (SELECT userid, STUFF((
                        SELECT char(10) + nullif(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid*/	
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
       --left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
	where ca.isdeleted <> 1 and ca.status <> 'Archive' --and candidateID in (75710,75708)
	)
--select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 10 * from note


select --top 10
         C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
--	, case when C.gender in ('M') then 'MALE' when C.gender in ('F') then 'FEMALE' else '' end as 'candidate-gender'
        , case 
              when C.nameprefix in ('Mr','Mr.') then 'MALE' 
              when C.nameprefix in ('Miss','Ms.','Mrs','Mrs.') then 'FEMALE' 
              else '' end as 'candidate-gender'
--	, case when C.gender in ('M') then 'MR' when C.gender in ('F') then 'MISS' else '' end as 'candidate-title'
        , case 
              when C.nameprefix in ('Dr','Dr.') then 'DR' 
              when C.nameprefix in ('Mr','Mr.') then 'MR' 
              when C.nameprefix in ('Miss','Miss.','Ms','Ms.') then 'MISS' 
              when C.nameprefix in ('Mrs','Mrs.') then 'MRS' 
              else '' end as 'candidate-title'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	--, C.middleName as 'candidate-middleName'
	, convert(varchar(10),C.dateOfBirth,120) as 'candidate-dob'
	--, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
	, iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) as 'candidate-email'
	
	, C.phone as 'candidate-homePhone'
	, C.mobile as 'candidate-phone'
	--, C.phone2 as 'candidate-homePhone'
	, Stuff( coalesce(' ' + nullif(C.phone2, ''), '') + coalesce(' ' + nullif(C.phone3, ''), '') + coalesce(', ' + nullif(C.workphone, ''), ''), 1, 1, '') as 'candidate-workPhone'
	--, Stuff( coalesce(' ' + nullif(C.phone, ''), '') + coalesce(' ' + nullif(C.phone2, ''), '') + coalesce(' ' + nullif(C.phone3, ''), '') + coalesce(', ' + nullif(C.workphone, ''), ''), 1, 1, '') as 'candidate-workPhone'
	--, C.workPhone as 'candidate-workPhone'
	
	, trim(Stuff( coalesce(' ' + nullif(trim(C.address1), ''), '') + coalesce(', ' + nullif(trim(C.address2), ''), '') + coalesce(' ' + nullif(trim(C.city), ''), '') + coalesce(', ' + nullif(trim(C.state), ''), '') + coalesce(', ' + nullif(trim(C.zip), ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '')) as 'candidate-address'
	, trim(Stuff( coalesce(' ' + nullif(trim(C.city), ''), '') + coalesce(', ' + nullif(trim(C.state), ''), '') + coalesce(', ' + nullif(trim(C.zip), ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '')) as 'candidate-LocationName' 
	, trim(C.city) as 'candidate-city'
	, trim(C.state) as 'candidate-state'
       , trim(C.zip) as 'candidate-zipCode'
	, tc.abbreviation as 'candidate-Country'
	--, tc.abbreviation as 'candidate-citizenship'
	
	, c.customText10 as 'candidate-currency'
	, cast(C.salary as int) as 'candidate-desiredSalary' --,C.customTextBlock2
	, cast(C.salaryLow as int) as 'candidate-currentSalary' --,C.customTextBlock3

	--, Education.school as 'candidate-schoolName'
	--, Education.graduationDate as 'candidate-graduationDate'
	--, Education.degree as 'candidate-degreeName'
	--, Education.major as '#candidate-major'
	--, SKILL.SkillName as 'candidate-skills'
/*       , trim(Stuff(
                   coalesce(nullif(SKILL.SkillName, '') + char(10), '')
                 + coalesce(nullif(convert(varchar(max),C.skillset), '') + char(10), '')
                 + coalesce('Languages: ' + nullif(convert(varchar(max),C.customText1), '') + char(10), '')
                 --+ coalesce(nullif(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
                 , 1, 0, '') ) as 'candidate-skills'*/
	--, 'PERMANENT' as 'candidate-jobTypes' --[PERMANENT, PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMP_TO_PERM]
	, case
              when c.employeeType in ('APPLICANT                     ','CANDIDATE','Employee Type','Please Select','US Standard Employee') then 'PERMANENT' --'[{"desiredJobTypeId":"1"}]'
              when c.employeeType in ('Assignment Employee','CAN Assignment Employee','CAN Contractor','Contractor','NL Contractor','US Contractor') then 'CONTRACT' --'[{"desiredJobTypeId":"2"}]'
              when c.employeeType = '' then 'PERMANENT' --'[{"desiredJobTypeId":"1"}]'
              when c.employeeType is null then 'PERMANENT' --'[{"desiredJobTypeId":"1"}]'
              end as 'candidate-jobTypes' --'desired_job_type_json'	
	, C.companyName as 'candidate-company1'
	, C.occupation as 'candidate-jobTitle1'
	, C.companyName as 'candidate-employer1'
	, owner2c.email as 'candidate-owners' --, C.recruiterUserID
	, doc.files as 'candidate-resume' --, stuff( coalesce(' ' + nullif(files.ResumeId, ''), '') + coalesce(', ' + nullif(p.placementfile, ''), ''), 1, 1, '') as 'candidate-resume'
       , CASE WHEN substring(c.companyURL , 1, 1) = ';' then substring(c.companyURL, 2, LEN(c.companyURL)) else companyURL END as 'candidate-xing'
	, note.note as 'candidate-note' --***
--       , es.es as 'candidate-education' --***
--	, eh.eh as 'candidate-workHistory' --***

--	, C.Nickname as 'Preferred Name'
-- 	, e2.email as 'candidate-PersonalEmail'
-- 	, c.customtext1 as 'Candidate Company Name'
--     , c.customtext2 as 'Candidate Company Number'
--     , c.desiredlocations as 'Desired Location Address'
--     , ca.comments as 'in Candidate Actions add an Interview category'
-- select count (*) -- select distinct employeeType -- select distinct employmentPreference -- nameprefix -- select distinct customText10 --dateAvailable -- -- gender -- -- select skillset, skillIDlist, customTextBlock1 --select distinct employmentPreference
from bullhorn1.Candidate C --where C.isdeleted <> 1 and C.status <> 'Archive'
left join owner2c on owner2c.candidateid = C.candidateid
left join ed on ed.ID = C.candidateID -- candidate-email-deduplication
--left join e2 on e2.ID = C.candidateID
left join tmp_country tc ON tc.code =  c.countryID
left join doc on doc.UserID = C.userID
left join note on C.candidateid = note.candidateid
--left join SKILL on SKILL.candidateID =  C.candidateID
--left join INDUSTRY on INDUSTRY.userid = C.userid
--left join Education on C.userID = Education.userID
--left join EducationSummary es on es.userID = C.userID
--left join EmploymentHistory eh on EH.userid = C.userid --WORK HISTORY
--left join comment on C.userID = comment.Userid
where C.isdeleted <> 1 and C.status <> 'Archive' --where C.isPrimaryOwner = 1
--and C.userid in (165180)
and C.candidateID in (43849,111887)
--and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
--and concat (C.FirstName,' ',C.LastName) like '%Partha%'
--and e1.email = '' or e1.email is null --e1.email <> ''
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID

select top 1000
candidateid
, comments
, customText1
, customText11
, customText16
, customText2
, customText5
, customTextBlock2

, desiredLocations
, referredBy
, referredByUserID
, status
from bullhorn1.Candidate C 
where customText16 <> '' and customText16 is not null

select distinct  nickname from bullhorn1.Candidate C 
select dateavailable from bullhorn1.Candidate C where dateavailable is not null

select distinct  customText16 from bullhorn1.Candidate C 
select candidateID as 'additional_id'
       , 'add_can_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11280 as 'field_id'
       , trim(convert(nvarchar(max),customText16)) as field_value
       , 11280 as 'constraint_id'
from bullhorn1.Candidate C
where C.isdeleted <> 1 and C.status <> 'Archive' 
and convert(nvarchar(max),comments) not in ('','NULL') 
and customText16 is not null and customText16 <> ''



-- CUSTOM FIELD > INTERVIEW NOTES
select candidateID as 'additional_id'
       , 'add_can_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11268 as 'field_id'
       , convert(nvarchar(max),comments) as field_value
       , 11268 as 'constraint_id'
from bullhorn1.Candidate C 
where C.isdeleted <> 1 and C.status <> 'Archive' 
and convert(nvarchar(max),comments) not in ('','NULL') and comments is not null
and candidateid in (98978)


--CUSTOM FIELD > Transportation Options
select distinct customText11 from bullhorn1.Candidate C 
with
customText11 (candidateID,customText11) as (
       SELECT 
              candidateID
              , trim( replace(replace(replace(customText11.value,'  ',' '),' )',')'),'( ','(') ) as customText11 --, trim( ind.value ) as ind 
       FROM (
              SELECT candidateID, trim( customText11.value ) as customText11 
              FROM bullhorn1.Candidate m 
              CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.customText11) ), ',') AS customText11
              ) m
       CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.customText11) ), ';') AS customText11
       where (customText11 is not null and convert(nvarchar(max),customText11) <> '' and customText11 <> 'Please Select')
       )
--select distinct customText11, count(*) from customText11 where customText11 <> '#N/A' group by customText11
--select distinct customText11 from customText11 where customText11 <> '#N/A'
, t as ( 
       select 
              candidateID
              , case customText11
              when 'Access to Car' then 1
              when 'Access to Scooter' then 2
              when 'agency' then 3
              when 'autoshare' then 4
              when 'Bike' then 5
              when 'Car' then 6
              when 'client' then 7
              when 'downtown toronto' then 8
              when 'GO' then 9
              when 'mississauga' then 10
              when 'Motorcycle' then 11
              when 'offsite only' then 12
              when 'pub' then 13
              when 'public' then 14
              when 'Public Transit' then 15
              when 'ttc' then 16
              when 'Walking' then 17
              else null end as field_value
       from customText11 ) --where customText11 <> '#N/A'
select candidateID as 'additional_id'
       , 'add_can_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11269 as 'field_id'
       , STRING_AGG( field_value,',' ) WITHIN GROUP (ORDER BY field_value asc) field_value
       , 11269 as 'constraint_id'
from t group by candidateID
-- select candidateID, 11267 as field_id, STRING_AGG( field_value,',' ) WITHIN GROUP (ORDER BY field_value asc) field_value from t group by candidateID


-- CUSTOM FIELD > Willing to Relocate (Checkbox multi select)
select distinct customText2 from bullhorn1.Candidate C where customText2 is not null and customText2 <> '' and customText2 <> 'Please Select' order by customText2
select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11272 as 'field_id'
       , trim (convert(nvarchar(max),customText2)) as field_value
       , 11272 as 'constraint_id'
from bullhorn1.Candidate C
where customText2 is not null and customText2 <> '' and customText2 <> 'Please Select' order by customText2


-- CUSTOM FIELD > Service Office
select distinct customText5 from bullhorn1.Candidate C  where customText5 is not null and customText5 not in ('','Please Select');
select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11272 as 'field_id'
       , 11272 as 'constraint_id'
        , case
when customText5 = 'Nova Scotia' then 3
when customText5 = 'Cincinnati' then 2
when customText5 = 'Amsterdam' then 1
when customText5 = 'Toronto' then 5
when customText5 = 'Ottawa' then 4
    end as 'field_value'
-- select distinct customText13        
from bullhorn1.Candidate C where customText5 is not null and customText5 not in ('','Please Select')


-- CUSTOM FIELD > Critical Info (Text Area)
select distinct convert(nvarchar(max),customTextBlock2) from bullhorn1.Candidate C 
select candidateID as 'additional_id'
       , 'add_can_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11274 as 'field_id'
       , convert(nvarchar(max),customTextBlock2) as field_value
       , 11274 as 'constraint_id'
from bullhorn1.Candidate C 
where C.isdeleted <> 1 and C.status <> 'Archive' 
and convert(nvarchar(max),customTextBlock2) not in ('','NULL') and customTextBlock2 is not null


-- CUSTOM SCRIPT - Require Custom Logic
select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11279 as 'field_id'
       , UC1.description as field_value
       , 11279 as 'constraint_id'
from bullhorn1.Candidate C
left join (select userid, ltrim([bullhorn1].[fn_ConvertHTMLToText](description)) as description from bullhorn1.BH_UserContact) UC1 on UC1.userID = C.UserID 
where C.isdeleted <> 1 and C.status <> 'Archive' 
and C.UserID is not null


-- CUSTOM FIELD > Willing to Commute to (Checkbox - multi select)
select distinct convert(nvarchar(max),desiredLocations) from bullhorn1.Candidate C 
with
desiredLocations (candidateID,desiredLocations) as (
       SELECT 
              candidateID
              , trim( replace(replace(replace(desiredLocations.value,'  ',' '),' )',')'),'( ','(') ) as desiredLocations --, trim( ind.value ) as ind 
       FROM (
              SELECT candidateID, trim( desiredLocations.value ) as desiredLocations 
              FROM bullhorn1.Candidate m 
              CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.desiredLocations) ), ',') AS desiredLocations
              ) m
       CROSS APPLY STRING_SPLIT( trim( convert(varchar(500),m.desiredLocations) ), ';') AS desiredLocations
       where (desiredLocations is not null and convert(nvarchar(max),desiredLocations) <> '' and desiredLocations <> 'Please Select')
       )
--select distinct desiredLocations, count(*) from desiredLocations where desiredLocations <> '#N/A' group by desiredLocations
, t as ( 
       select 
              candidateID
              , case desiredLocations
when '---Amsterdam Area---' then 1
when '---Ottawa Area---' then 2
when '---Toronto Area---' then 3
when '1 hour max' then 4
when 'Ajax' then 5
when 'almost anywhere' then 6
when 'anywhere' then 7
when 'anywhere for F to F meetings' then 8
when 'Anywhere transit goes' then 9
when 'Atlanta' then 10
when 'Aurora' then 11
when 'Bells Corner' then 12
when 'Bramp' then 13
when 'Brampton' then 14
when 'Brooklyn ok' then 15
when 'bur' then 16
when 'Burlington' then 17
when 'Calgary' then 18
when 'Cambridge' then 19
when 'Cambridge/Kitchener-Waterloo' then 20
when 'Canada' then 21
when 'Chicago' then 22
when 'Cincinnati' then 23
when 'cincinnnati' then 24
when 'Columbus' then 25
when 'Connecticut' then 26
when 'do' then 27
when 'dowbtown' then 28
when 'down' then 29
when 'Downtown' then 30
when 'downtown mainly' then 31
when 'downtown only' then 32
when 'Downtown Ottawa' then 33
when 'downtown pref' then 34
when 'Downtown Toronto' then 35
when 'etob' then 36
when 'Etobicoke' then 37
when 'etoc' then 38
when 'etonicoke' then 39
when 'Europe' then 40
when 'fairly open' then 41
when 'GA' then 42
when 'Gatineau' then 43
when 'GTA' then 44
when 'guel' then 45
when 'Guelph' then 46
when 'Hamilton' then 47
when 'Kanata' then 48
when 'Kitchener' then 49
when 'lives scarb.' then 50
when 'Manhattan' then 51
when 'Manhattan ideal' then 52
when 'mark' then 53
when 'Markham' then 54
when 'mid' then 55
when 'Midtown' then 56
when 'Midtown Toronto' then 57
when 'Milton' then 58
when 'miss' then 59
when 'Mississauga' then 60
when 'Nepean' then 61
when 'new jersey' then 62
when 'New York' then 63
when 'New York City' then 64
when 'Newmarket' then 65
when 'no' then 66
when 'no long distance commute' then 67
when 'north' then 68
when 'North York' then 69
when 'NOT Brooklyn' then 70
when 'NY' then 71
when 'NYC' then 72
when 'oak' then 73
when 'Oakville' then 74
when 'OH' then 75
when 'open' then 76
when 'Oshawa' then 77
when 'Ottawa' then 78
when 'Ottawa East' then 79
when 'Peterborough' then 80
when 'Philly' then 81
when 'pick' then 82
when 'Pickering' then 83
when 'pref downtown' then 84
when 'public transit access only' then 85
when 'Remote Only' then 86
when 'RH' then 87
when 'Richmond Hill' then 88
when 'Russell' then 89
when 'scar' then 90
when 'scarb' then 91
when 'Scarborough' then 92
when 'St. Catherines' then 93
when 'Stoney Creek' then 94
when 'Stouffville' then 95
when 'TH' then 96
when 'the city' then 97
when 'The Glebe' then 98
when 'Thornhill' then 99
when 'tor' then 100
when 'Toronto' then 101
when 'toronto area' then 102
when 'united states' then 103
when 'up' then 104
when 'uptown' then 105
when 'Uptown Toronto' then 106
when 'Uxbridge' then 107
when 'Vancouver' then 108
when 'Vanier' then 109
when 'Vaughan' then 110
when 'wants downtown' then 111
when 'Waterloo' then 112
when 'West Chester' then 113
when 'Westboro' then 114
when 'Westchester' then 115
when 'Whitby' then 116
when 'white plains' then 117
when 'willing to coomute for about an hour' then 118
when 'within 45 mins of residence' then 119
when 'woodbridge' then 120
when 'yes' then 121
              else null end as field_value
       from desiredLocations ) --where customText11 <> '#N/A'
select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11275 as 'field_id'
       , STRING_AGG( field_value,',' ) WITHIN GROUP (ORDER BY field_value asc) field_value
       , 11275 as 'constraint_id'
from t group by candidateID


--CUSTOM FIELD > Referred By (Other)
select distinct referredBy from bullhorn1.Candidate C 

select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11276 as 'field_id'
       , trim(convert(nvarchar(max),referredBy)) as field_value
       , 11276 as 'constraint_id'
from bullhorn1.Candidate C 
where C.isdeleted <> 1 and C.status <> 'Archive' 
and convert(nvarchar(max),referredBy) not in ('','NULL') and externalID is not null

-- CUSTOM FIELD > Referred By User identity
select distinct referredByUserID from bullhorn1.Candidate CA where CA.referredByUserID is not null

select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11277 as 'field_id'
       , coalesce( nullif( trim(cast(UC.email as nvarchar(max))), '') + ' - ' + trim(UC.firstname) + ' ' + trim(UC.lastname), '') as field_value--UC.firstname, UC.lastname, UC.email
       , 11277 as 'constraint_id'      
from bullhorn1.Candidate CA
left join (select userid, firstname, lastname, email from bullhorn1.BH_UserContact) UC ON UC.userID = CA.referredByUserID
where CA.referredByUserID is not null


-- CUSTOM FIELD > Status
select distinct Status from bullhorn1.Candidate C 
select candidateID as 'additional_id'
       , 'add_cand_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11278 as 'field_id'
       , convert(nvarchar(max),status) as field_value
       , 11278 as 'constraint_id'
from bullhorn1.Candidate C 
where C.isdeleted <> 1 and C.status <> 'Archive' 



/*
select    C.candidateID as 'externalId'
        , coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
        , c.dayratelow
        , c.hourlyrate
from bullhorn1.Candidate C
where dayratelow <> '' or hourlyrate <> ''


with t as (
select    C.candidateID as 'externalId'
        , coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
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





select --top 10
         C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'         
 	, c.customtext1 as 'Candidate Company Name'
       , c.customtext2 as 'Candidate Company Number'
       , c.desiredlocations as 'Desired Location Address'
-- select count (*) -- select distinct employmentPreference -- nameprefix --gender -- -- select skillset, skillIDlist, customTextBlock1 --select top 10 *
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
where C.isdeleted <> 1 and C.status <> 'Archive'
and (c.customtext1 is not null and c.customtext1 <> '') 
or (c.customtext2 is not null and c.customtext2 <> '') 



select --top 10
         C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	, case
when c.employmentPreference = 'Contract' then '[{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Contract,Permanent' then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Contract,Temporary' then '[{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Part Time' then '[{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Permanent' then '[{"desiredJobTypeId":"1"}]'
when c.employmentPreference = 'Permanent,Contract' then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Permanent,Contract,Temporary' then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Temporary' then '[{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Temporary,Contract' then '[{"desiredJobTypeId":"2"}]'
when c.employmentPreference = 'Temporary,Permanent' then '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
when c.employmentPreference = '' then '[{"desiredJobTypeId":"1"}]'
when c.employmentPreference is null then '[{"desiredJobTypeId":"1"}]'
	end as 'desired_job_type_json'
-- select count (*) -- select distinct employmentPreference -- nameprefix --gender -- -- select skillset, skillIDlist, customTextBlock1 --select top 10 *
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
where C.isdeleted <> 1 and C.status <> 'Archive'


*/
        
        