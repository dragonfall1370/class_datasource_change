
with
-- EMAIL
--  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
  mail1 (ID,email) as (select C.candidateID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),'*',' '),'|',' '),'‘',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isdeleted <> 1 and C.status <> 'Archive' /*and C.isPrimaryOwner = 1*/ )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 as (select ID, email from mail4 where rn = 4)
--select * from ed where email like '%kenaslai@hotmail.com%'

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
              --, UE.customText1, UE.customText2, UE.customText3, UE.customText4
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
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( 
                     stuff(
                               Coalesce('Date Added: ' + NULLIF(cast(dateAdded AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')                   
                            + Coalesce('Certification: ' + NULLIF(cast(certification as nvarchar(max)), '') + char(10), '')
                            + Coalesce('City: ' + NULLIF(cast(city as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Comments: ' + NULLIF(cast(comments as nvarchar(max)), '') + char(10), '')
                            --+ Coalesce('Country: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
                            + Coalesce('Degree: ' + NULLIF(cast(degree as nvarchar(max)), '') + char(10), '')
                            + Coalesce('End Date: ' + NULLIF(cast(endDate AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                            + Coalesce('Expiration Date: ' + NULLIF(cast(expirationDate AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                            + Coalesce('GPA: ' + NULLIF(cast(gpa as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Graduation Date: ' + NULLIF(cast(graduationDate AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                            + Coalesce('Major: ' + NULLIF(cast(major as nvarchar(max)), '') + char(10), '')
                            + Coalesce('School: ' + NULLIF(cast(school as nvarchar(max)), '') + char(10), '')
                            + Coalesce('Start Date: ' + NULLIF(cast(startDate AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                            + Coalesce('State: ' + NULLIF(cast(state as nvarchar(max)), '') + char(10), '')
                            --+ Coalesce('Education ID: ' + NULLIF(cast(userEducationID as varchar(max)), '') + char(10), '')
                     , 1, 0, '') 
                                                ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                                ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                                ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                                ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                                ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                                ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') 
                     as es
                     from bullhorn1.BH_UserEducation
       WHERE userId = a.userId 
       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
       FROM bullhorn1.BH_UserEducation as a GROUP BY a.userId 
       )
-- select * from EducationSummary where userid in (163454);
-- select * from bullhorn1.BH_UserEducation where customText1 is not null
-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;


-- Employment History -- select top 1000 *  from bullhorn1.BH_userWorkHistory where userid in (158197)
, EmploymentHistory(userId, eh) as (
       SELECT a.userId, STUFF(( 
                     select char(10) + 
                     REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                                REPLACE( REPLACE( REPLACE( REPLACE( 
                     stuff(
                         Coalesce('Bonus: ' + NULLIF(cast(bonus as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Client Corporation: ' + NULLIF(cast(clientCorporationID as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Comments: ' + NULLIF(cast(comments as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Commission: ' + NULLIF(cast(commission as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Company Name: ' + NULLIF(cast(companyName as nvarchar(max)), '') + char(10), '')
--                     + Coalesce('Old Date Field: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
--                     + Coalesce('Salary/Pay Rate (Legacy): ' + NULLIF(cast(customText2 as varchar(max)), '') + char(10), '')
                     + Coalesce('Date Added: ' + NULLIF(cast(dateAdded AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                     + Coalesce('End Date: ' + NULLIF(cast(endDate AT TIME ZONE 'China Standard Time' as varchar(max)), '') + char(10), '')
                     + Coalesce('Job Posting: ' + NULLIF(cast(title as nvarchar(max)), '') + char(10), '') --jobPostingID
                     --+ Coalesce('Placement: ' + NULLIF(cast(placementID as varchar(max)), '') + char(10), '')
                     + Coalesce('Salary Low: ' + NULLIF(cast(salary1 as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Salary High: ' + NULLIF(cast(salary2 as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Salary Type: ' + NULLIF(cast(salaryType as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Start Date: ' + NULLIF(cast(startDate AT TIME ZONE 'China Standard Time' as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Termination Reason: ' + NULLIF(cast(terminationReason as nvarchar(max)), '') + char(10), '')
                     + Coalesce('Title: ' + NULLIF(cast(title as nvarchar(max)), '') + char(10), '')
                     --+ Coalesce('User Work History ID: ' + NULLIF(cast(userWorkHistoryID as varchar(max)), '') + char(10), '')
                            --+ Coalesce('Comments: ' + NULLIF(replace([dbo].[udf_StripHTML](comments),'Â ',''), '') + char(10), '')
                            --+ Coalesce('Comments: ' + NULLIF(replace([dbo].[fn_ConvertHTMLToText](comments),'Â ',''), '') + char(10), '')
                     , 1, 0, '') 
                                                ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                                ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                                ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                                ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                                ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                                ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') 
                     as eh
                     from bullhorn1.BH_userWorkHistory
       WHERE userId = a.userId order by startDate desc
       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
       FROM bullhorn1.BH_userWorkHistory as a
       left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
       --where userid in (164043)
       GROUP BY a.userId 
       )
-- select * from EmploymentHistory where userid in (289878);


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
		 , Stuff( Coalesce('BH Candidate ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')  
		          + Coalesce('Email 2: ' + NULLIF(cast(e2.email as varchar(max)), '') + char(10), '')
		          + Coalesce('Email 3: ' + NULLIF(cast(e3.email as varchar(max)), '') + char(10), '')
		          + Coalesce('Visa Status: ' + NULLIF(cast(CA.customText15 as nvarchar(max)), '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),CA.Status), '') + char(10), '')
                        + Coalesce('General Comments: ' + NULLIF(convert(nvarchar(max),CA.comments), '') + char(10), '')
                        + coalesce('Resume: ' + NULLIF(UC1.description, '') + char(10), '')
                        --+ coalesce('Resume: ' + NULLIF(UW.description, '') + char(10), '')
                        , 1, 0, '') as note
	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
       left join e2 on CA.userID = e2.ID
       left join e3 on CA.userID = e3.ID
	left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
       /*left join (SELECT userid, STUFF((
                        SELECT char(10) + NULLIF(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid*/
       left join (select userid, ltrim([dbo].[fn_ConvertHTMLToText](replace(description,'p.std   { margin-top: 0; margin-bottom: 0; border: 0 0 0 0; }',''))) as description from bullhorn1.BH_UserContact /*where userid in (161358, 157430, 149777)*/ ) UC1 on CA.UserID = UC1.userID                             	
--        left join (select userid, description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
	where ca.isdeleted <> 1 and ca.status <> 'Archive' )
--select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note


select top 10
         C.candidateID as 'candidate-externalId' , C.userID as '#userID'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
       , note.note as 'candidate-note'
 	
-- select count (*) -- select distinct customText17 --customText7 --gender --employmentPreference -- select skillset, skillIDlist, customTextBlock1 --select top 10 * -- select mobile, phone, phone2, phone3, workphone
from bullhorn1.Candidate C 
--left join ed on C.candidateid = ed.ID -- candidate-email-deduplication
left join note on C.userID = note.Userid --<<<<<<<<<<<<
where C.isdeleted <> 1 and C.status <> 'Archive' --C.isPrimaryOwner = 1
and C.candidateID in (21, 22, 34, 37, 38, 53, 58, 64, 65, 68)
--and C.userid in (161358, 157430, 149777)
--nd note.note like '%margin-top%'
--and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
--and concat (C.FirstName,' ',C.LastName) like '%Partha%'
--and e1.email = '' or e1.email is null --e1.email <> ''
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID

--select count (*) from bullhorn1.Candidate C where C.isdeleted <> 1 and C.status <> 'Archive'
