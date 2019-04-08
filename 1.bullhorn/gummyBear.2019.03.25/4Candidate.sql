
with
-- EMAIL
  mail1 (ID,email) as (select C.candidateID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isdeleted <> 1 and C.status <> 'Archive' /*and C.isPrimaryOwner = 1*/ )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed


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
       SELECT userId
       , STRING_AGG(
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                     coalesce('Date Added: ' + nullif(cast(dateAdded as nvarchar(max)), '') + char(10), '')                   
                                   + coalesce('Certification: ' + nullif(cast(certification as nvarchar(max)), '') + char(10), '')
                                   + coalesce('City: ' + nullif(cast(city as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Comments: ' + nullif(cast(comments as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Country: ' + nullif(cast(customText1 as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Degree: ' + nullif(cast(degree as nvarchar(max)), '') + char(10), '')
                                   + coalesce('End Date: ' + nullif(cast(endDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Expiration Date: ' + nullif(cast(expirationDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('GPA: ' + nullif(cast(gpa as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Graduation Date: ' + nullif(cast(graduationDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Major: ' + nullif(cast(major as nvarchar(max)), '') + char(10), '')
                                   + coalesce('School: ' + nullif(cast(school as nvarchar(max)), '') + char(10), '')
                                   + coalesce('Start Date: ' + nullif(cast(startDate as nvarchar(max)), '') + char(10), '')
                                   + coalesce('State: ' + nullif(cast(state as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Education ID: ' + nullif(cast(userEducationID as nvarchar(max)), '') + char(10), '')
                            , 1, 0, '')
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') --as es
       ,char(10) ) WITHIN GROUP (ORDER BY dateadded) es
       FROM bullhorn1.BH_UserEducation GROUP BY userId        
       )
-- select top 10 * from EducationSummary where userid in (163454);
-- select * from bullhorn1.BH_UserEducation where customText1 is not null
-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;


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


-- OWNER
/*, owner as (select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isdeleted <> 1 and CA.status <> 'Archive')
-- Secondary OWNER
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select owner2a.userid, UC.email, UC.name from owner2a left join (select userid, email, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
--, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
, owner2c as (SELECT userID, STRING_AGG( email,',') WITHIN GROUP (ORDER BY email) email from owner2b where email like '%_@_%.__%' GROUP BY userID )
--select * from owner2c where userid in (8281,12389,6467,10883,4281)*/

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
		 , Stuff(   coalesce('BH Candidate ID: ' + nullif(cast(CA.userID as nvarchar(max)), '') + char(10), '')  
                        + coalesce('Other email: ' + nullif(cast(e3.email as nvarchar(max)), '') + char(10), '')
                        + coalesce('Desired Salary-High: ' + nullif(cast(CA.customFloat1 as nvarchar(max)), '') + char(10), '')
                        + coalesce('Notice Period: ' + nullif(cast(CA.customText11 as nvarchar(max)), '') + char(10), '')
                        + coalesce('1st degree GPA: ' + nullif(cast(CA.customText19 as nvarchar(max)), '') + char(10), '')
                        + coalesce('Referred By (Other): ' + nullif(convert(nvarchar(max),CA.referredBy), '') + char(10), '')
                        + coalesce('Referred By: ' + nullif(convert(nvarchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
                        + coalesce('Status: ' + nullif(convert(nvarchar(max),CA.Status), '') + char(10), '')
--                        + coalesce('Desired Locations: ' + nullif(cast(CA.desiredLocations as varchar(max)), '') + char(10), '')
--                        + coalesce('Employment Preference: ' + nullif(cast(CA.employmentPreference as varchar(max)), '') + char(10), '')
--                        + coalesce('Willing to Relocate: ' + nullif(cast(CA.willRelocate as varchar(max)), '') + char(10), '')                       
--                        + coalesce('HKID#: ' + nullif(convert(varchar(max),CA.customText1), '') + char(10), '')
--                        + coalesce('Other Phone: ' + nullif(convert(varchar(max),CA.phone2), '') + char(10), '')
--                        + coalesce('CV: ' + nullif(UC1.description, '') + char(10), '')
                        + coalesce('General Comments: ' + nullif(convert(nvarchar(max),CA.comments), '') + char(10), '')
                        --+ coalesce('CV: ' + nullif(UW.description, '') + char(10), '')
                        --+ coalesce('Latest Comment: ' + nullif([dbo].[fn_ConvertHTMLToText](lc.comments), '') + char(10), '')
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
                        
                        + coalesce('Bar admission: ' + nullif(cast(AD.Admission as varchar(max)), '') + char(10), '')
                        + coalesce('General Work Function: ' + nullif(cast(CA.customText2 as varchar(max)), '') + char(10), '') 
                        + coalesce('Summary: ' + nullif(cast(summary.summary as varchar(max)), '') + char(10), '') */
                        , 1, 0, '') as note
	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
       left join e3 on CA.candidateID = e3.ID
	left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
       /*left join (SELECT userid, STUFF((
                        SELECT char(10) + nullif(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid*/	
       --left join (select userid, ltrim([dbo].[fn_ConvertHTMLToText](description)) as description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
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
	where isdeleted <> 1 and status <> 'Archive' )
--select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note


select --top 10
         C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
	--, case C.gender when 'M' then 'MR' when 'F' then 'MISS' else '' end as 'candidate-title'
	--, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
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
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	, C.middleName as 'candidate-middleName'
	, convert(varchar(10),C.dateOfBirth,120) as 'candidate-dob'
	, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
	, C.customText1 as 'candidate-LinkedIn'

	, C.mobile as 'candidate-phone'
	, C.mobile as 'candidate-mobile'
	, C.phone as 'candidate-homePhone'	
	, Stuff( coalesce(' ' + nullif(C.phone2, ''), '') + coalesce(', ' + nullif(C.workphone, ''), ''), 1, 1, '') as 'candidate-workPhone'
	
	, Stuff( coalesce(' ' + nullif(C.address1, ''), '') + coalesce(', ' + nullif(C.address2, ''), '') + coalesce(' ' + nullif(C.city, ''), '') + coalesce(', ' + nullif(C.state, ''), '') + coalesce(', ' + nullif(C.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '') as 'candidate-address'
	, Stuff( coalesce(' ' + nullif(C.city, ''), '') + coalesce(', ' + nullif(C.state, ''), '') + coalesce(', ' + nullif(C.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '') as 'candidate-LocationName' 
	, C.city as 'candidate-city'
	, C.state as 'candidate-state'
       , C.zip as 'candidate-zipCode'
	, tc.abbreviation as 'candidate-Country'
       , iif( tc2.abbreviation is null,
              case
		when c.customText6 like 'Bosnia%' then 'BA'
		when c.customText6 like '%Brazil%' then 'BR'
		when c.customText6 like 'Brunei%' then 'BN'
		when c.customText6 like 'Hong%' then 'HK'
		when c.customText6 like '%Korea%' then 'KR'
		when c.customText6 like 'Laos%' then 'LA'
		when c.customText6 like 'Other%' then 'AT'
		when c.customText6 like 'Serbia%' then 'RS'
		when c.customText6 like 'Syria%' then 'SY'
		when c.customText6 like 'Turkey%' then 'TR'
		else '' end
              , tc2.abbreviation) as 'candidate-citizenship'
	
	, cast(C.salaryLow as int) as 'candidate-currentSalary' -- Desired Annual Salary
	, cast(C.salary as int) as 'candidate-desiredSalary' -- Current Salary (Annual) Mths/year = 12
	--, Education.school as 'candidate-schoolName'
	--, Education.graduationDate as 'candidate-graduationDate'
	--, Education.degree as 'candidate-degreeName'
	--, Education.major as '#candidate-major'
	--, SKILL.SkillName as 'candidate-skills'
--       , ltrim(Stuff( 
--                   coalesce(nullif(SKILL.SkillName, '') + char(10), '')
--                 + coalesce(nullif(convert(varchar(max),C.skillset), ''), '')
--                 + coalesce(nullif(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
--                 , 1, 0, '') ) as 'candidate-skills'
	, 'PERMANENT' as 'candidate-jobTypes'
	, C.companyName as 'candidate-company1'
	, C.occupation as 'candidate-jobTitle1'
	, C.companyName as 'candidate-employer1'
	
	, owner2c.email as 'candidate-owners' --, C.recruiterUserID
	, doc.files as 'candidate-resume' --, stuff( coalesce(' ' + nullif(files.ResumeId, ''), '') + coalesce(', ' + nullif(p.placementfile, ''), ''), 1, 1, '') as 'candidate-resume'
	, note.note as 'candidate-note' --***
       , es.es as 'candidate-education' --***
	, eh.eh as 'candidate-workHistory' --***

	--, C.Nickname as 'Preferred Name'
 	--, e2.email as 'candidate-PersonalEmail'
 	--, customText17 as 'candidate-skype'
-- select count (*) -- select distinct namePrefix --gender --employmentPreference -- select customText17 --customText1--skillset, skillIDlist, customTextBlock1 --select top 10 * -- select distinct customText6
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
left join owner2c on owner2c.candidateid = C.candidateid
left join ed on ed.ID = C.candidateID -- candidate-email-deduplication
--left join e2 on e2.ID = C.candidateID
left join tmp_country tc ON tc.code =  c.countryID
left join tmp_country tc2 ON tc2.country =  c.customText6
left join doc on doc.UserID = C.userID
left join note on C.candidateid = note.candidateid
--left join SKILL on SKILL.userId =  C.userID
--left join INDUSTRY on INDUSTRY.userid = C.userid
left join EducationSummary es on es.userID = C.userID
--left join EmploymentHistory eh on EH.userid = C.userid --WORK HISTORY
--left join Education on C.userID = Education.userID
--left join comment on C.userID = comment.Userid
where C.isdeleted <> 1 and C.status <> 'Archive' --where C.isPrimaryOwner = 1
--and C.userid in (165180)
--and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
--and concat (C.FirstName,' ',C.LastName) like '%Partha%'
--and e1.email = '' or e1.email is null --e1.email <> ''
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID

/*
select    C.candidateID as 'externalId'
	, C.Nickname as 'PreferredName'
        , coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
        , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
from bullhorn1.Candidate C
where Nickname <> '' and Nickname is not null


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

*/