
with
-- EMAIL
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed

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
       SELECT userId, STUFF(( 
                     select char(10) + 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                      Coalesce('Date Added: ' + NULLIF(cast(dateAdded as varchar(max)), '') + char(10), '')                   
                                   + Coalesce('Certification: ' + NULLIF(cast(certification as varchar(max)), '') + char(10), '')
                                   + Coalesce('City: ' + NULLIF(cast(city as varchar(max)), '') + char(10), '')
                                   + Coalesce('Comments: ' + NULLIF(cast(comments as varchar(max)), '') + char(10), '')
                                   --+ Coalesce('Country: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
                                   + Coalesce('Degree: ' + NULLIF(cast(degree as varchar(max)), '') + char(10), '')
                                   + Coalesce('End Date: ' + NULLIF(cast(endDate as varchar(max)), '') + char(10), '')
                                   + Coalesce('Expiration Date: ' + NULLIF(cast(expirationDate as varchar(max)), '') + char(10), '')
                                   + Coalesce('GPA: ' + NULLIF(cast(gpa as varchar(max)), '') + char(10), '')
                                   + Coalesce('Graduation Date: ' + NULLIF(cast(graduationDate as varchar(max)), '') + char(10), '')
                                   + Coalesce('Major: ' + NULLIF(cast(major as varchar(max)), '') + char(10), '')
                                   + Coalesce('School: ' + NULLIF(cast(school as varchar(max)), '') + char(10), '')
                                   + Coalesce('Start Date: ' + NULLIF(cast(startDate as varchar(max)), '') + char(10), '')
                                   + Coalesce('State: ' + NULLIF(cast(state as varchar(max)), '') + char(10), '')
                                   --+ Coalesce('Education ID: ' + NULLIF(cast(userEducationID as varchar(max)), '') + char(10), '')
                            , 1, 0, '') 
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as es
                     from bullhorn1.BH_UserEducation
       WHERE userId = a.userId 
       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
       FROM bullhorn1.BH_UserEducation as a GROUP BY a.userId 
       )
-- select * from EducationSummary where userid in (163454);
-- select * from bullhorn1.BH_UserEducation where customText1 is not null
-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;


-- Employment History -- select *  from bullhorn1.BH_userWorkHistory
, EmploymentHistory(userId, eh) as (
       SELECT a.userId, STUFF(( 
                     select char(10) + 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( 
                            stuff(
                                Coalesce('Bonus: ' + NULLIF(cast(bonus as varchar(max)), '') + char(10), '')
                            + Coalesce('Client Corporation: ' + NULLIF(cast(clientCorporationID as varchar(max)), '') + char(10), '')
                            + Coalesce('Comments: ' + NULLIF(cast(comments as varchar(max)), '') + char(10), '')
                            + Coalesce('Commission: ' + NULLIF(cast(commission as varchar(max)), '') + char(10), '')
                            + Coalesce('Company Name: ' + NULLIF(cast(companyName as varchar(max)), '') + char(10), '')
                            + Coalesce('Date Added: ' + NULLIF(cast(dateAdded as varchar(max)), '') + char(10), '')
                            + Coalesce('End Date: ' + NULLIF(cast(endDate as varchar(max)), '') + char(10), '')
                            + Coalesce('Job Posting: ' + NULLIF(cast(title as varchar(max)), '') + char(10), '') --jobPostingID
                            --+ Coalesce('Placement: ' + NULLIF(cast(placementID as varchar(max)), '') + char(10), '')
                            + Coalesce('Salary Low: ' + NULLIF(cast(salary1 as varchar(max)), '') + char(10), '')
                            + Coalesce('Salary High: ' + NULLIF(cast(salary2 as varchar(max)), '') + char(10), '')
                            + Coalesce('Salary Type: ' + NULLIF(cast(salaryType as varchar(max)), '') + char(10), '')
                            + Coalesce('Start Date: ' + NULLIF(cast(startDate as varchar(max)), '') + char(10), '')
                            + Coalesce('Termination Reason: ' + NULLIF(cast(terminationReason as varchar(max)), '') + char(10), '')
                            + Coalesce('Title: ' + NULLIF(cast(title as varchar(max)), '') + char(10), '')
                            --+ Coalesce('User Work History ID: ' + NULLIF(cast(userWorkHistoryID as varchar(max)), '') + char(10), '')
                                   --+ Coalesce('Comments: ' + NULLIF(replace([dbo].[udf_StripHTML](comments),'Â ',''), '') + char(10), '')
                                   --+ Coalesce('Comments: ' + NULLIF(replace([dbo].[fn_ConvertHTMLToText](comments),'Â ',''), '') + char(10), '')
                            , 1, 0, '') 
                     ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                     ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                     ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                     ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                     ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                     ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as eh
                     from bullhorn1.BH_userWorkHistory
       WHERE userId = a.userId order by startDate desc
       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
       FROM bullhorn1.BH_userWorkHistory as a
       left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
       --where userid in (164043)
       GROUP BY a.userId 
       )
-- select * from EmploymentHistory where userid in (164043);


-- Secondary OWNER
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select  owner2a.userid, UC.name from owner2a left join (select userid, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
--select * from owner2c where userid in (8281,12389,6467,10883,4281)

-- Web Responses
, wr as (
        select jr.userid, jp.title,jr.status
        from bullhorn1.BH_JobResponse JR
        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
        left join bullhorn1.BH_JobPosting  jp on jp.jobPostingID = jr.jobPostingID )
, wr1 as (SELECT userID, STUFF((SELECT ', ' + concat('Title: ',title,' - Status: ',status)  from wr WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM wr AS a GROUP BY a.userID )
--select * from wr1

-- Latest Comment
, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_UserComment )

-- COMMENT
, comment(Userid, comment) as (SELECT Userid, STUFF((SELECT char(10) + 'Date Added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max)) from [bullhorn1].[BH_UserComment] WHERE Userid = a.Userid order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM [bullhorn1].[BH_UserComment] AS a GROUP BY a.Userid )
, summary(candidateID,summary) as (SELECT candidateID, STUFF((SELECT coalesce(char(10) + 'Date Added: ' + convert(varchar,dateAdded,120) + ' || ' + 'Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), '') from bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS summary FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID)

-- DOCUMENT
, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF((SELECT ',' + name from bullhorn1.View_CandidateFile WHERE candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID)

-- Files
, files(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + concat(candidateFileID, fileExtension) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID) --where a.type = 'Resume') ==> get all candidates files

-- Placement Files
, placementfiles(userID, placementfile) as (SELECT userID, STUFF((SELECT DISTINCT ',' + concat(placementFileID, fileExtension) from bullhorn1.View_PlacementFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_PlacementFile AS a GROUP BY a.userID)
--select top 10 * from placementfiles

-- NOTE
, note as (
	SELECT CA.userID
		 , Stuff( Coalesce('ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')  
                        --+ Coalesce('Other email: ' + NULLIF(cast(e3.email as varchar(max)), '') + char(10), '')
                        + Coalesce('E&D Sent: ' + NULLIF(convert(varchar(max),CA.customTextBlock3), '') + char(10), '')
                        + Coalesce('Opted Out: ' + NULLIF(convert(varchar(max),CA.massmailoptout), '') + char(10), '')
                        + Coalesce('Skype: ' + NULLIF(cast(CA.phone2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Referred by: ' + NULLIF(convert(varchar(max),CA.referredBy), '') + char(10), '')

--                        + Coalesce('Referred by 2: ' + NULLIF(convert(varchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
--                        + Coalesce('Status: ' + NULLIF(convert(varchar(max),CA.Status), '') + char(10), '')
--                        --+ coalesce('CV: ' + NULLIF([dbo].[fn_ConvertHTMLToText](UC1.description), '') + char(10), '')
                        + Coalesce('General Comments: ' + NULLIF(convert(varchar(max),CA.comments), '') + char(10), '')
                        + coalesce('Latest Comment: ' + NULLIF([dbo].[fn_ConvertHTMLToText](lc.comments), '') + char(10), '')
                        + coalesce('CV: ' + NULLIF(UW.description, '') + char(10), '')
                        
                        /*+ coalesce('Placements: ' + NULLIF(convert(varchar(max),pm.status), '') + char(10), '') --CA.activePlacements
                        + coalesce('Secondary Owners: ' + nullif(convert(varchar(max),owner2c.name), '') + char(10), '') --CA.secondaryOwners
                        + coalesce('Web Responses: ' + NULLIF(convert(varchar(max),wr1.name), '') + char(10), '') --CA.jobResponseJobPostingID                        
                        + Coalesce('Singapore / PR Yes/No: ' + NULLIF(cast(ca.EmployeeType as varchar(max)), '') + char(10), '')
                        
                        + Coalesce('Current Salary: ' + NULLIF(cast(CA.customTextBlock3 as varchar(max)), '') + char(10), '')
                        + Coalesce('Total Annual Salary: ' + NULLIF(cast(CA.customText1 as varchar(max)), '') + char(10), '')
                        + Coalesce('Desired Salary: ' + NULLIF(cast(CA.customTextBlock2 as varchar(max)), '') + char(10), '')
                        + Coalesce('Willing to Relocate: ' + NULLIF(cast(CA.willRelocate as varchar(max)), '') + char(10), '')
                        + Coalesce('Practice Area / Category: ' + NULLIF(cast(CName.Name as varchar(max)), '') + char(10), '')
                        + Coalesce('Skills: ' + NULLIF(cast(SN.SkillName as varchar(max)), '') + char(10), '')
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
                        + Coalesce('ID Number: ' + NULLIF(cast(CA.ssn as varchar(max)), '') + char(10), '')
                        + Coalesce('AA/EE: ' + NULLIF(cast(CA.customText5 as varchar(max)), '') + char(10), '')
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
                        + Coalesce('General Work Function: ' + NULLIF(cast(CA.customText2 as varchar(max)), '') + char(10), '') */
                        --+ Coalesce('Summary: ' + NULLIF(cast(summary.summary as varchar(max)), '') + char(10), '') 
                        , 1, 0, '') as note
	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
       left join e3 on CA.userID = e3.ID
	left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
       left join (SELECT userid, STUFF((
                        SELECT char(10) + NULLIF(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid   	
	left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
	--left join SkillName SN on CA.userID = SN.userId
	--left join BusinessSector BS on CA.userID = BS.userId
        --left join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
        --left join admission AD on CA.userID = AD.Userid
        --left join CName on CA.userID = CName.Userid
        --left join SpeName on CA.userID = SpeName.Userid
        --left join mail5 on CA.userID = mail5.ID
        left join summary on CA.userID = summary.CandidateID
        --left join (select userid, status from bullhorn1.BH_Placement ) pm on pm.userid = ca.userid
        --left join owner2c on owner2c.userid = CA.userid
        --left join wr1 on wr1.userid = CA.userid
        left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
        left join (select userid, description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
	where CA.isPrimaryOwner = 1 )
--select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note


select --top 10
         C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
	, case C.gender when 'M' then 'MR' when 'F' then 'MISS' else '' end as 'candidate-title'
	, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	--, C.middleName as 'candidate-middleName'
	--, convert(varchar(10),C.dateOfBirth,120) as 'candidate-dob'
	, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
	, C.mobile as 'candidate-phone'
	, C.mobile as 'candidate-mobile'
	, C.phone as 'candidate-homePhone'	
	, C.workPhone as 'candidate-workPhone'
	, 'PERMANENT' as 'candidate-jobTypes'
	--, Stuff( coalesce(' ' + nullif(C.address1, ''), '') + coalesce(', ' + nullif(C.address2, ''), ''), 1, 1, '') as 'candidate-address' --, C.address1 as 'candidate-address'
	, Stuff( coalesce(' ' + nullif(C.address1, ''), '') + coalesce(', ' + nullif(C.address2, ''), '') + Coalesce(' ' + NULLIF(C.city, ''), '') + Coalesce(', ' + NULLIF(C.state, ''), '') + Coalesce(', ' + NULLIF(C.zip, ''), '') + Coalesce(', ' + NULLIF(tc.country, ''), ''), 1, 1, '') as 'candidate-address'
	, Stuff( Coalesce(' ' + NULLIF(C.city, ''), '') + Coalesce(', ' + NULLIF(C.state, ''), '') + Coalesce(', ' + NULLIF(C.zip, ''), '') + Coalesce(', ' + NULLIF(tc.country, ''), ''), 1, 1, '') as 'candidate-LocationName' 
	, C.city as 'candidate-city'
	, C.zip as 'candidate-zipCode'
	, C.state as 'candidate-state'
       , tc.abbreviation as 'candidate-Country' --, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN '' ELSE tc.abbreviation END as 'candidate-Country'
	--, tc.abbreviation as 'candidate-citizenship' --, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL' OR tc.abbreviation = 'ZR') THEN '' ELSE tc.abbreviation END as 'candidate-citizenship'
	, cast(C.salaryLow as int) as 'candidate-currentSalary' --,C.customTextBlock3
	, cast(C.salary as int) as 'candidate-desiredSalary' --,C.customTextBlock2
	--, Education.school as 'candidate-schoolName'
	--, Education.graduationDate as 'candidate-graduationDate'
	--, Education.degree as 'candidate-degreeName'
	--, Education.major as '#candidate-major'
	--, SN.SkillName as 'candidate-skills'
--       , ltrim(Stuff( 
--                     coalesce(nullif(SN.SkillName, '') + char(10), '')
--                 + coalesce(nullif(convert(varchar(max),C.skillset), ''), '')
--                 --+ Coalesce(NULLIF(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
--                 , 1, 0, '') ) as 'candidate-skills'
	, C.companyName as 'candidate-company1'
	, C.occupation as 'candidate-jobTitle1'
	, C.companyName as 'candidate-employer1'
	, owner.email as 'candidate-owners' --, C.recruiterUserID as '#recruiterUserID'
	, stuff( coalesce(' ' + nullif(files.ResumeId, ''), '') + coalesce(', ' + nullif(p.placementfile, ''), ''), 1, 1, '') as 'candidate-resume'
	--, note.note as 'candidate-note' -->>
--	, eh.eh as 'candidate-workHistory'
--       , es.es as 'candidate-education'	
--	--, left(comment.comment,32760) as 'candidate-comments'
--	, C.Nickname as 'Preferred Name' -->
 	, e2.email as 'candidate-PersonalEmail' -->
 	, c.source as 'candidate-source' -->>
-- select count (*) -- select distinct gender --employmentPreference -- select skillset, skillIDlist, customTextBlock1 --select top 10 *
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
--left join SkillName SN on C.userID = SN.userId --SUB FUNCTIONAL EXPERTISE
--left join BusinessSector BS on BS.userid = C.userid -- INDUSTRY
--left join EmploymentHistory EH on EH.userid = C.userid --WORK HISTORY
left join tmp_country tc ON c.countryID = tc.code
left join owner on C.recruiterUserID = owner.recruiterUserID
left join ed on C.userID = ed.ID -- candidate-email-deduplication
left join e2 on C.userID = e2.ID
--left join Education on C.userID = Education.userID
--left join EducationSummary es on es.userID = C.userID
--left join t4 on t4.candidateUserID = C.userID
left join files on C.userID = files.candidateUserID
left join placementfiles p  on p.userid = C.userid
--left join comment on C.userID = comment.Userid
--left join note on C.userID = note.Userid
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