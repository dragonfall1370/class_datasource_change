
/*with
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
mail1 (ID,userID,email) as (
       select distinct C.candidateID, C.userID
	      , replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
	from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID
	cross apply string_split( concat(UC.email,' ',UC.email2,' ',UC.email3) ,' ')
	where (UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%') and C.isdeleted <> 1 --and C.status <> 'Archive'
--	and C.candidateID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)
--       select REFERENCE
--	, replace(translate(value, '!'':"<>[]();,+/\&?•*|‘','                     '),char(9),'') as email --to translate special characters
--	from PROP_EMAIL
--	cross apply string_split(EMAIL_ADD,' ')
--	where EMAIL_ADD like '%_@_%.__%' and REFERENCE in (61065,43945)
	)
--select * from mail1 where email <> '' and ID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)

, mail1a (ID,userID,email) as (
       select --top 100
              C.candidateID --, C.userID as '#userID'
              , coalesce( nullif( mail1.email,''), concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co')) as email
       from bullhorn1.Candidate C
       left join mail1 on mail1.ID = C.candidateID -- candidate-email-deduplication
--       where candidateID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)
	)
--select * from mail1a where ID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)
	
, mail2 (ID,userID,email,rn,ID_rn) as (
       select distinct ID --, userID
              , trim(' ' from email) as email
              , row_number() over(partition by trim(' ' from email) order by ID asc) as rn --distinct email if emails exist more than once
              , row_number() over(partition by ID order by trim(' ' from email)) as ID_rn --distinct if contacts may have more than 1 email
	from mail1a
	--where email like '%_@_%.__%'
	)
--select * from mail2 where ID in (7232,180193,49941,191426,191425,10986,158302,158303,56726,60613,85518,66771,85519,94526,152784,152781,199953,199597)

, ed (ID,email) as (
       select ID
	      , case when rn > 1 then concat(email,'_',rn)
	else email end as email
	from mail2
	where email is not NULL and email <> ''
	and ID_rn = 1
	)
--select * from ed where ID in (186063, 188424)
	
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
, files(candidateUserID, files) as (SELECT candidateUserID, STRING_AGG( concat( convert(nvarchar(max),candidateFileID), fileExtension),',' ) WITHIN GROUP (ORDER BY candidateFileID) files from bullhorn1.View_CandidateFile GROUP BY candidateUserID)
--select top 100 * from files
--select * from files
, placementfiles(userID, files) as (SELECT userID, STRING_AGG( concat(placementFileID, fileExtension),',' ) WITHIN GROUP (ORDER BY placementFileID) files from bullhorn1.View_PlacementFile GROUP BY userID)
--select * from placementfiles
, doc(Userid, files) as ( select f.candidateUserID, STRING_AGG( f.files,',' ) WITHIN GROUP (ORDER BY f.files) files from (SELECT * from files UNION ALL SELECT * from placementfiles) f GROUP BY f.candidateUserID )
--select * from doc


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
                                   + Coalesce('Job Posting: ' + NULLIF(convert(nvarchar(max),r.jobPostingID), '') + char(10), '')
                                   + Coalesce('Date Added: ' + NULLIF(convert(nvarchar(max),r.dateAdded), '') + char(10), '')
                                   + Coalesce('Employment Start: ' + NULLIF(convert(nvarchar(max),r.employmentStart), '') + char(10), '')
                                   + Coalesce('Employment End: ' + NULLIF(convert(nvarchar(max),r.employmentEnd), '') + char(10), '')
                                   + Coalesce('Reference ID: ' + NULLIF(convert(nvarchar(max),r.userReferenceID), '') + char(10), '')
                                   + Coalesce('Reference: ' + NULLIF(convert(nvarchar(max),r.referenceUserID), '') + char(10), '')
                                   + Coalesce('Reference Title: ' + NULLIF(convert(nvarchar(max),r.referenceTitle), '') + char(10), '')
                                   + Coalesce('Reference First Name: ' + NULLIF(convert(nvarchar(max),r.referenceFirstName), '') + char(10), '')
                                   + Coalesce('Reference Last Name: ' + NULLIF(convert(nvarchar(max),r.referenceLastName), '') + char(10), '')
                                   + Coalesce('Reference Email: ' + NULLIF(convert(nvarchar(max),r.referenceEmail), '') + char(10), '')
                                   + Coalesce('Reference Phone: ' + NULLIF(convert(nvarchar(max),r.referencePhone), '') + char(10), '')
                                   + Coalesce('Status: ' + NULLIF(convert(nvarchar(max),r.status), '') + char(10), '')
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
                            --stuff(
                              concat_ws ('',
                              coalesce('Client Corporation: ' + nullif(cast(clientCorporationID as nvarchar(max)), '') + '<br/>', '')
                            , coalesce('Company Name: ' + nullif(cast(companyName as nvarchar(max)) + '<br/>', ''), '')
                            , coalesce('Job Posting: ' + nullif(cast(title as nvarchar(max)), '')  + '<br/>', '') --jobPostingID
                            , coalesce('Title: ' + nullif(cast(title as nvarchar(max)), '') + '<br/>', '')
                            --, coalesce('Placement: ' + nullif(cast(placementID as nvarchar(max)), '') + char(10), '')
                            , coalesce('Salary Low: ' + nullif(cast(salary1 as nvarchar(max)), '')  + '<br/>', '')
                            , coalesce('Salary High: ' + nullif(cast(salary2 as nvarchar(max)), '') + '<br/>', '')
                            , coalesce('Salary Type: ' + nullif(cast(salaryType as nvarchar(max)), '') + '<br/>', '')
                            , coalesce('Bonus: ' + nullif(cast(bonus as nvarchar(max)), '') + '<br/>', '')
                            , coalesce('Start Date: ' + nullif(cast(startDate as nvarchar(max)), '') + '<br/>' , '')
                            , coalesce('End Date: ' + nullif(cast(endDate as nvarchar(max)), '')  + '<br/>', '')
                            , coalesce('Date Added: ' + nullif(cast(dateAdded as nvarchar(max)) + '<br/>', ''), '')
                            , coalesce('Termination Reason: ' + nullif(cast(terminationReason as nvarchar(max)), '') + '<br/>', '')
                            , coalesce('Commission: ' + nullif(cast(commission as nvarchar(max)), '') + '<br/>', '')
                            , coalesce('Comments: ' + nullif(cast(comments as nvarchar(max)), '') + '<br/>' + '<br/>', '')
                            --+ coalesce('User Work History ID: ' + nullif(cast(userWorkHistoryID as nvarchar(max)), '') + char(10), '')
                                   --+ coalesce('Comments: ' + nullif(replace([dbo].[udf_StripHTML](comments),'Â ',''), '') + char(10), '')
                                   --+ coalesce('Comments: ' + nullif(replace([dbo].[fn_ConvertHTMLToText](comments),'Â ',''), '') + char(10), '')
                            --, 1, 0, '')
                            )
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
       ,'<br/><br/>' ) WITHIN GROUP (ORDER BY startDate desc) eh
       FROM bullhorn1.BH_userWorkHistory GROUP BY userId
       )
--select top 10 * from EmploymentHistory --where userid in (164043);




-- NOTE
, note as (
	SELECT CA.candidateid --, Stuff(
		 , concat_ws ('',	 
		            coalesce('BH Candidate ID: ' + nullif(cast(CA.userID as nvarchar(max)), '') + '<br/>', '')
                        , coalesce('Other email: ' + nullif(cast(e3.email as nvarchar(max)), '') + '<br/>', '')
, Coalesce('Current Location: ' + NULLIF(convert(nvarchar(max),CA.customText13), '') + '<br/>', '')
                        , coalesce('Expected Salary: ' + nullif(cast(CA.customText3 as nvarchar(max)), '') + '<br/>', '')

, Coalesce('Latest Package Detail: ' + NULLIF(convert(nvarchar(max),CA.customText1), '') + '<br/>', '')
, Coalesce('Notice Period: ' + NULLIF(convert(nvarchar(max),CA.customText11), '') + '<br/>', '')
--, Coalesce('Hong Kong ID: ' + NULLIF(convert(nvarchar(max),CA.customText12), '') + '<br/>', '')

, Coalesce('Geographical Scope: ' + NULLIF(convert(nvarchar(max),CA.customText14), '') + '<br/>', '')
--, Coalesce('Open to Relocate: ' + NULLIF(convert(nvarchar(max),CA.customText15), '') + '<br/>', '')
, Coalesce('Executive Brief: ' + NULLIF(convert(nvarchar(max),CA.customTextBlock1), '') + '<br/>', '')
--, Coalesce('Latest Comment: ' + NULLIF(convert(nvarchar(max),CA.latestComment), '') + '<br/>', '')
, coalesce('Referred By: ' + nullif(convert(nvarchar(max),CA.referredBy), '') + '<br/>', '')
, coalesce('Referred By UserID: ' + nullif(convert(nvarchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + '<br/>', '')
, coalesce('Opted In - SMS Messages: ' + nullif(cast(case ca.massmailoptout when 1 then 'No' when 0 then 'Yes' end as nvarchar(max)), '') + '<br/>', '') --smsOptIn
, coalesce('Status: ' + nullif(convert(nvarchar(max),CA.Status), '') + '<br/>', '')

--                        , coalesce('Available Until: ' + nullif(cast(CA.dateAvailableEnd as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Bar admission: ' + nullif(cast(AD.Admission as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Business Sector: ' + nullif(cast(INDUSTRY.BusinessSector as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Current Hourly Rate: ' + nullif(cast(CA.hourlyRateLow as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Date Available: ' + nullif(cast(CA.dateAvailable as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Date Registered: ' + nullif(convert(nvarchar(10),CA.dateAdded,120), '') + '<br/>', '')
--                        , coalesce('Description: ' + nullif(cast(UC.description as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Desired Hourly Rate: ' + nullif(cast(CA.hourlyRate as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Desired Locations: ' + nullif(cast(CA.desiredLocations as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Employment Preference: ' + nullif(cast(CA.employmentPreference as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('General Comments: ' + nullif(convert(nvarchar(max),CA.comments), '') + '<br/>', '')
--                        , coalesce('ID Number: ' + nullif(cast(CA.ssn as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Latest Comment: ' + nullif([bullhorn1].[fn_ConvertHTMLToText](lc.comments), '') + '<br/>', '')
--                        , coalesce('Opted In - SMS Messages: ' + nullif(cast(case ca.massmailoptout when 1 then 'No' when 0 then 'Yes' end as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Other Phone: ' + nullif(convert(nvarchar(max),CA.phone2), '') + '<br/>', '')
--                        , coalesce('Placements: ' + nullif(convert(nvarchar(max),pm.status), '') + '<br/>', '') --CA.activePlacements
--                        , coalesce('Practice Area / Category: ' + nullif(cast(CATEGORY.Name as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Referred By (Other): ' + nullif(convert(nvarchar(max),CA.referredBy), '') + '<br/>', '')
--                        , coalesce('Referred By UserID: ' + nullif(convert(nvarchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + '<br/>', '')
--                        , coalesce('Registered By: ' + nullif(convert(nvarchar(max),CA.recruiterUserID), '') + ' - ' + UC0.firstname + ' ' + UC0.lastname + '<br/>', '')
--                        , coalesce('Secondary Owners: ' + nullif(convert(nvarchar(max),owner2c.email), '') + '<br/>', '') --CA.secondaryOwners
--                        , coalesce('Singapore / PR Yes/No: ' + nullif(cast(ca.EmployeeType as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Skills: ' + nullif(cast(SN.SkillName as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Skype: ' + nullif(cast(CA.phone2 as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Source: ' + nullif(cast(CA.source as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('Specialty: ' + nullif(cast(CA.specialtyIDList as nvarchar(max)), '') + '<br/>', '')
--                        
--                        , coalesce('Web Responses: ' + nullif(convert(nvarchar(max),wr1.name), '') + '<br/>', '') --CA.jobResponseJobPostingID
--                        , coalesce('Willing to Relocate: ' + nullif(cast(CA.willRelocate as nvarchar(max)), '') + '<br/>', '')
--                       
--                        , coalesce('LTD Company Address: ' + nullif(cast( concat(CA.secondaryaddress1,' ',CA.secondaryAddress2) as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('LTD Company City: ' + nullif(cast(CA.secondaryCity as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('LTD Company Country: ' + nullif(cast(tmp_country.country as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('LTD Company County: ' + nullif(cast(CA.secondaryState as nvarchar(max)), '') + '<br/>', '')
--                        , coalesce('LTD Company Post Code: ' + nullif(cast(CA.secondaryZip as nvarchar(max)), '') + '<br/>', '')
                        , coalesce('CV: ' + nullif(UC1.description, '') + '<br/>', '')
--                      + coalesce('CV: ' + nullif(UW.description, '') + '<br/>', '')
                        --+ coalesce('Summary: ' + nullif(cast(summary.summary as nvarchar(max)), '') + '<br/>', '')
                        ) as note --, 1, 0, '') as note
	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
       left join e3 on CA.userID = e3.ID
	left join (select userid, firstname, lastname from bullhorn1.BH_UserContact) UC0 ON UC0.userID = CA.recruiterUserID
	left join (select userid, firstname, lastname from bullhorn1.BH_UserContact) UC ON UC.userID = CA.referredByUserID
       left join (select userid, ltrim([bullhorn1].[fn_ConvertHTMLToText](description)) as description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
	--left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
	left join tmp_country on cast(CA.secondaryCountryID as varchar(2)) = tmp_country.code
       left join admission AD on CA.userID = AD.Userid
       left join INDUSTRY on INDUSTRY.userId = CA.userID --BusinessSector
       left join CATEGORY on CATEGORY.Userid = CA.userID
       left join owner2c on owner2c.candidateid = CA.candidateid
       left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
       left join (select userid, status from bullhorn1.BH_Placement ) pm on pm.userid = ca.userid
       left join SKILL SN on CA.userID = SN.userId
       --left join SPECIALTY on SPECIALTY.candidateID = CA.userID
       left join wr1 on wr1.userid = CA.userid
       --left join summary on CA.userID = summary.CandidateID
       /*left join (SELECT userid, STUFF((
                        SELECT char(10) + nullif(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
                        FROM (   select userid, description_truong
                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
                        ) uw on uw.userid = ca.userid*/
	where ca.isdeleted <> 1 /*and ca.status <> 'Archive'*/ )
--select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note


select --top 1
         C.candidateID as 'candidate-externalId' , C.userID as '#userID'
	, case
	      when C.namePrefix in ('Mr','Mr.','Mr. D','(MR)','(Mr.)',N'先生') then 'MR'
	      when C.namePrefix in ('MA','Madam','Mrs','Mrs.') then 'MRS'
	      when C.namePrefix in ('M','M.','(Ms)','Ms','Ms.',N'女士') then 'MS'
	      when C.namePrefix in ('Miss','Miss.','(Ms.)',N'小姐') then 'MISS'
	      when C.namePrefix in ('Dr','Dr.') then 'DR'
	      else '' end as 'candidate-title'
	, case when C.gender in ('M') then 'MALE' when C.gender in ('F') then 'FEMALE' else '' end as 'candidate-gender'
/*        , case 
              when C.nameprefix in ('Dr','Dr.') then 'DR' 
              when C.nameprefix in ('Mr','Mr.') then 'MR' 
              when C.nameprefix in ('Miss','Miss.','Ms','Ms.') then 'MISS' 
              when C.nameprefix in ('Mrs','Mrs.') then 'MRS' 
              else '' end as 'candidate-title'
        , case 
              when C.nameprefix in ('Mr','Mr.') then 'MALE' 
              when C.nameprefix in ('Miss','Ms.','Mrs','Mrs.') then 'FEMALE' 
              else '' end as 'candidate-gender'*/
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , case when c.status = 'Archive' then concat(coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) ,' (Archive)')
              else coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID))
              end as 'contact-lastName'
       --, stuff( coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) + coalesce(' ' + nullif(C.namesuffix, ''), ''), 1, 0, '')  as 'contact-lastName'

	, C.middleName as 'candidate-middleName'
	, convert(varchar(10),C.dateOfBirth,120) as 'candidate-dob'
	--, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
	, ed.email as 'candidate-email' --, iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) as 'candidate-email'
	
	, C.mobile as 'candidate-phone'
	, C.mobile as 'candidate-mobile'
	, C.phone as 'candidate-homePhone'	
--	, C.workPhone as 'candidate-workPhone' 
	, Stuff( coalesce(' ' + nullif(C.phone2, ''), '') + coalesce(', ' + nullif(C.workphone, ''), ''), 1, 1, '') as 'candidate-workPhone'
	
	, Stuff( coalesce(' ' + nullif(C.address1, ''), '') + coalesce(', ' + nullif(C.address2, ''), '') + coalesce(' ' + nullif(C.city, ''), '') + coalesce(', ' + nullif(C.state, ''), '') + coalesce(', ' + nullif(C.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '') as 'candidate-address'
	, Stuff( coalesce(' ' + nullif(C.city, ''), '') + coalesce(', ' + nullif(C.state, ''), '') + coalesce(', ' + nullif(C.zip, ''), '') + coalesce(', ' + nullif(tc.country, ''), ''), 1, 1, '') as 'candidate-LocationName' 
	, C.city as 'candidate-city'
	, C.state as 'candidate-state'
       , C.zip as 'candidate-zipCode'
	, tc.abbreviation as 'candidate-Country'
	--, tc.abbreviation as 'candidate-citizenship'
	, case
		when C.customText6 like 'African%' then 'ZA'
		when C.customText6 like 'South African%' then 'ZA'
		when C.customText6 like 'America%' then 'US'
		when C.customText6 like 'Austral%' then 'AU'
		when C.customText6 like 'Austria%' then 'AT'
		when C.customText6 like 'Austril%' then 'AU'
		when C.customText6 like 'Belgian%' then 'BE'
		when C.customText6 like 'Belgium%' then 'BE'
		when C.customText6 like 'Birtish%' then 'GB'
		when C.customText6 like 'Brazili%' then 'BR'
		when C.customText6 like 'British%' then 'GB'
		when C.customText6 like 'Canada%' then 'CA'
		when C.customText6 like 'Canadia%' then 'CA'
		when C.customText6 like 'Canandi%' then 'CA'
		when C.customText6 like 'China%' then 'CN'
		when C.customText6 like 'Chinese%' then 'CN'
		when C.customText6 like 'Chines%' then 'CN'
		when C.customText6 like 'Chin%' then 'CN'
		when C.customText6 like 'Croatia%' then 'HR'
		when C.customText6 like 'C%' then ''
		when C.customText6 like 'Dual British and Hong Kong Citizenship%' then 'GB'
		when C.customText6 like 'Egypt%' then 'EG'
		when C.customText6 like 'English%' then 'GB'
		when C.customText6 like 'EU%' then 'HK'
		when C.customText6 like 'Female%' then ''
		when C.customText6 like 'Fijian%' then 'FJ'
		when C.customText6 like 'Filipin%' then 'PH'
		when C.customText6 like 'French%' then 'FR'
		when C.customText6 like 'German%' then 'DE'
		when C.customText6 like 'HKID%' then 'HK'
		when C.customText6 like 'Perm. HKID holder%' then 'HK'
		when C.customText6 like 'HKSAR%' then 'HK'
		when C.customText6 like 'HK%' then 'HK'
		when C.customText6 like 'Hongkon%' then 'HK'
		when C.customText6 like 'Hong%' then 'HK'
		when C.customText6 like 'IANG%' then ''
		when C.customText6 like 'Indian%' then 'IN'
		when C.customText6 like 'Indones%' then 'ID'
		when C.customText6 like 'Iraqi%' then 'IQ'
		when C.customText6 like 'Italian%' then 'IT'
		when C.customText6 like 'Ivorian%' then 'CI'
		when C.customText6 like 'Japanes%' then 'JP'
		when C.customText6 like 'Korean%' then 'KR'
		when C.customText6 like 'KOREAN%' then 'KR'
		when C.customText6 like 'Korea%' then 'KR'
		when C.customText6 like 'Malaysi%' then 'MY'
		when C.customText6 like 'Malay%' then 'MY'
		when C.customText6 like ' Malaysian%' then 'MY'
		when C.customText6 like 'Malyasi%' then 'MY'
		when C.customText6 like 'Maylasi%' then 'MY'
		when C.customText6 like 'Microne%' then 'FM'
		when C.customText6 like 'Nepales%' then 'NP'
		when C.customText6 like 'Nigeria%' then 'NG'
		when C.customText6 like 'New Zealand%' then 'NZ'
		when C.customText6 like 'Pakista%' then 'PK'
		when C.customText6 like 'PALIBUR%' then ''
		when C.customText6 like 'Perm.%' then ''
		when C.customText6 like 'Philipp%' then 'PH'
		when C.customText6 like 'Phillip%' then 'PH'
		when C.customText6 like 'Polish%' then 'PL'
		when C.customText6 like 'Portugu%' then 'PT'
		when C.customText6 like 'Russian%' then 'RU'
		when C.customText6 like 'Singapo%' then 'SG'
		when C.customText6 like 'Spanish%' then 'ES'
		when C.customText6 like 'Swedish%' then 'SE'
		when C.customText6 like 'Taiwane%' then 'TW'
		when C.customText6 like 'Taiwan%' then 'TW'
		when C.customText6 like 'Tawnine%' then 'CN'
		when C.customText6 like 'Thailan%' then 'TH'
		when C.customText6 like 'Tunisia%' then 'TN'
		when C.customText6 like 'Ugandan%' then 'UG'
		when C.customText6 like 'UK%' then 'GB'
		when C.customText6 like 'Unknown%' then ''
		when C.customText6 like 'Uruguay%' then 'UY'
		when C.customText6 like 'US%' then 'US'
		when C.customText6 like 'Vietnam%' then 'VN'
		when C.customText6 like 'Zealand%' then 'NZ'
		when C.customText6 like '%UNITED%ARAB%' then 'AE'
		when C.customText6 like '%UAE%' then 'AE'
		when C.customText6 like '%U.A.E%' then 'AE'
		when C.customText6 like '%UNITED%KINGDOM%' then 'GB'
		when C.customText6 like '%UNITED%STATES%' then 'US'
		when C.customText6 like '%USA%' then 'US'
		when C.customText6 like 'US' then 'US'
		when C.customText6 in (N'何家文',N'曾藹群',N'盧瑞婷',N'羅嘉樂',N'馬奕朗',N'黃 榮黃') then 'CN'
              else '' end as 'candidate-citizenship'
	
	, cast(C.salaryLow as bigint) as 'candidate-currentSalary' --Current Salary (Monthly type)
	, cast(C.salary as int) as 'candidate-desiredSalary' --customText3 --***

	, 'PERMANENT' as 'candidate-jobTypes'
	, C.companyName as 'candidate-company1'
	, C.occupation as 'candidate-jobTitle1'
	, C.companyName as 'candidate-employer1'
	, owner2c.email as 'candidate-owners' --, C.recruiterUserID
	, doc.files as 'candidate-resume' --, stuff( coalesce(' ' + nullif(files.ResumeId, ''), '') + coalesce(', ' + nullif(p.placementfile, ''), ''), 1, 1, '') as 'candidate-resume'

	--, SKILL.SkillName as 'candidate-skills'
       /*, ltrim(Stuff( 
                   coalesce(nullif(SKILL.SkillName, '') + char(10), '')
                 + coalesce(nullif(convert(varchar(max),C.skillset), ''), '')
                 --+ coalesce(nullif(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
                 , 1, 0, '') ) as 'candidate-skills'*/
       , C.customText7 as 'candidate-skills'
       
	--, Education.school as 'candidate-schoolName'
	--, Education.graduationDate as 'candidate-graduationDate'
	--, Education.degree as 'candidate-degreeName'
	--, Education.major as '#candidate-major'

       --, stuff( coalesce(' ' + nullif(es.es, '') + char(10), '') + coalesce('REFERENCE: ' + char(10) + nullif(rs.reference, '') + char(10), ''), 1, 1, '') as 'candidate-education' --*** 
--       , stuff( coalesce('Highest Education Degree' + nullif(c.educationDegree, '') + char(10), '') + coalesce('Education Summary: ' + char(10) + nullif(es.es, '') + char(10), ''), 1, 0, '') as 'candidate-education' -- <<<***

--       , concat_ws (''
--              , coalesce(' ' + nullif(eh.eh, '') + '<br/>' + '<br/>', '')
--              , coalesce('REFERENCE: ' + char(10) + nullif(rs.reference, '') + '<br/>' + '<br/>', '')
--              ) as 'candidate-workHistory' --*** 
	--, eh.eh as 'candidate-workHistory' --***

	--, note.note as 'candidate-note' --***
--	, C.Nickname as 'Preferred Name' --***
-- 	, e2.email as 'candidate-PersonalEmail' --***
--     , c.desiredlocations as 'Desired Location Address'
-- 	, c.customtext1 as 'Candidate Company Name'
--     , c.customtext2 as 'Candidate Company Number'
-- select count (*) --  select distinct gender --educationDegree -- customText3, salary --gender --, nameprefix -- -- select distinct employeetype -- select distinct employmentPreference -- select skillset, skillIDlist, customTextBlock1 --select top 10 *
from bullhorn1.Candidate C --where C.isPrimaryOwner = 1
left join owner2c on owner2c.candidateid = C.candidateid
left join ed on ed.ID = C.candidateID -- candidate-email-deduplication
--left join e2 on e2.ID = C.candidateID
left join doc on doc.UserID = C.userID
left join tmp_country tc ON tc.code =  c.countryID
--left join SKILL on SKILL.userId =  C.userID
--left join INDUSTRY on INDUSTRY.userid = C.userid
--left join note on C.candidateid = note.candidateid
--left join Education on C.userID = Education.userID
--left join ReferenceSummary rs on rs.userID = C.userID
--left join EducationSummary es on es.userID = C.userID
--left join EmploymentHistory eh on EH.userid = C.userid --WORK HISTORY
--left join comment on C.userID = comment.Userid
where C.isdeleted <> 1 --and C.status <> 'Archive' --where C.isPrimaryOwner = 1
--and rs.reference is not null
--and C.userid in (165180)
--and C.candidateID in (391, 2447)
--and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
--and concat (C.FirstName,' ',C.LastName) like '%Partha%'
--and e1.email = '' or e1.email is null --e1.email <> ''
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
--and (c.customtext1 is not null and c.customtext1 <> '') 
--or (c.customtext2 is not null and c.customtext2 <> '') 

select distinct customText15 from bullhorn1.Candidate C where customText15 is not null and customText15 <> ''

select distinct customText12 from bullhorn1.Candidate C where customText12 is not null and customText12 <> ''
select candidateID as 'additional_id'
       , 'add_can_info' as 'additional_type'
       , 1005 as 'form_id'
       , 11269 as 'field_id'
       , trim(convert(nvarchar(max),customText12))as field_value
       , 11269 as 'constraint_id'
from t group by candidateID
 where customText12 is not null and customText12 <> ''

select distinct reasonClosed from reasonClosed where reasonClosed is not null and reasonClosed <> ''
--select distinct customText11, count(*) from customText11 where customText11 <> '#N/A' group by customText11
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



/*
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
/*
select    C.candidateID as 'externalId'
       , customText3	as 'Expected Salary	YES	Desired Annual Salary'

	, cast(C.salaryLow as bigint) as 'candidate-currentSalary' --Current Salary (Monthly type)
	, cast(C.salary as int) as 'candidate-desiredSalary' --customText3 --***
from bullhorn1.Candidate C where C.candidateID in (163343)
*/