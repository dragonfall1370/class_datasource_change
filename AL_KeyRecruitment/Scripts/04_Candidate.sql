drop table if exists VCCans;

with
-- EMAIL
  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
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
, owner as (select distinct CA.recruiterUserID, UC.email, UC.name from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

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
                     , 1, 0, '') as es
                     from bullhorn1.BH_UserEducation
       WHERE userId = a.userId 
       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
       FROM bullhorn1.BH_UserEducation as a GROUP BY a.userId 
       )
--select * from EducationSummary where userid in (163454);
-- select * from bullhorn1.BH_UserCertification

-- Secondary OWNER
, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, owner2b as (select  owner2a.userid, UC.name from owner2a left join (select userid, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
--select * from owner2c where userid in (8281,12389,6467,10883,4281)

-- Web Responses
--, wr as (
--        select jr.userid, jp.title,jr.status
--        from bullhorn1.BH_JobResponse JR
--        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
--        left join bullhorn1.BH_JobPosting  jp on jp.jobPostingID = jr.jobPostingID )
--, wr1 as (SELECT userID, STUFF((SELECT ', ' + concat('Title: ', [dbo].[ufn_RemoveForXMLUnsupportedCharacters](title),' - Status: ',status)  from wr WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM wr AS a GROUP BY a.userID )
--select * from wr1

-- Latest Comment
--, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_UserComment )

--, reference (userId, note) as (
--       SELECT a.userId, STUFF(( 
--                     select char(10) + 
--                     stuff(
--                     --+ Coalesce('Reference ID: ' + NULLIF(cast(r.userReferenceID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Candidate Title: ' + NULLIF(cast(r.candidateTitle as varchar(max)), '') + char(10), '')
--                     + Coalesce('Client Corporation: ' + NULLIF(cast(r.clientCorporationID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Company: ' + NULLIF(cast(r.companyName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Date Added: ' + NULLIF(cast(r.dateAdded as varchar(max)), '') + char(10), '')
--                     + Coalesce('Employment End: ' + NULLIF(cast(r.employmentEnd as varchar(max)), '') + char(10), '')
--                     + Coalesce('Employment Start: ' + NULLIF(cast(r.employmentStart as varchar(max)), '') + char(10), '')
--                     + Coalesce('Job Posting: ' + NULLIF(cast(r.jobPostingID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Email: ' + NULLIF(cast(r.referenceEmail as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference First Name: ' + NULLIF(cast(r.referenceFirstName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Last Name: ' + NULLIF(cast(r.referenceLastName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Phone: ' + NULLIF(cast(r.referencePhone as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Title: ' + NULLIF(cast(r.referenceTitle as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference: ' + NULLIF(cast(r.referenceUserID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Status: ' + NULLIF(cast(r.status as varchar(max)), '') + char(10), '')
--                     + Coalesce('Years Known: ' + NULLIF(cast(r.yearsKnown as varchar(max)), '') + char(10), '')
--                     , 1, 0, '') as note
--                     from bullhorn1.BH_UserReference r
--       WHERE userId = a.userId order by dateAdded desc
--       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
--       FROM bullhorn1.BH_UserReference as a
--       --left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
--       GROUP BY a.userId 
--       )
--select * from reference 

-- PLACEMENT
--, placementNotesTmp as (
--	select 
--	C.CandidateId
--	,C.fullname
--	, pl.dateadded 
--	, Stuff( 'PLACEMENT: ' + char(10)
--		+ Coalesce('Billing Contact: ' + NULLIF(cast( concat(UC1.FirstName,' ',UC1.LastName,'     ',UC1.email)  as varchar(max)), '') + char(10), '')  --pl.billingUserID
--				--+ Coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as varchar(max)), '') + char(10), '')
--		+ Coalesce('Bill Rate: ' + NULLIF(cast(pl.clientBillRate as varchar(max)), '') + char(10), '')
--		+ Coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as varchar(max)), '') + char(10), '')
--		+ Coalesce('Comments: ' + NULLIF(cast(pl.comments as varchar(max)), '') + char(10), '')
--				--+ Coalesce('Contract Employment Info: ' + NULLIF(cast(pl.contractInfoHeader as varchar(max)), '') + char(10), '')
--		+ Coalesce('Primary Timesheet Approver: ' + NULLIF(cast(pl.correlatedCustomText1 as varchar(max)), '') + char(10), '')
--		+ Coalesce('Secondary Timecard Approver: ' + NULLIF(cast(pl.correlatedCustomText2 as varchar(max)), '') + char(10), '')
--		+ Coalesce('Purchase Order Number: ' + NULLIF(cast(pl.correlatedCustomText3 as varchar(max)), '') + char(10), '')
--		+ Coalesce('Cost Center: ' + NULLIF(cast(pl.costCenter as varchar(max)), '') + char(10), '')
--		+ Coalesce('Insurance Reference: ' + NULLIF(cast(pl.customText1 as varchar(max)), '') + char(10), '')
--		+ Coalesce('Start Date: ' + NULLIF(cast(pl.dateBegin as varchar(max)), '') + char(10), '')
--		+ Coalesce('Effective Date (Client): ' + NULLIF(cast(pl.dateClientEffective as varchar(max)), '') + char(10), '')
--		+ Coalesce('Effective Date: ' + NULLIF(cast(pl.dateEffective as varchar(max)), '') + char(10), '')
--		+ Coalesce('Scheduled End: ' + NULLIF(cast(pl.dateEnd as varchar(max)), '') + char(10), '')
--		+ Coalesce('Days Guaranteed: ' + NULLIF(cast(pl.daysGuaranteed as varchar(max)), '') + char(10), '')
--		+ Coalesce('Days Pro-Rated: ' + NULLIF(cast(pl.daysProRated as varchar(max)), '') + char(10), '')
--		+ Coalesce('Employment Type: ' + NULLIF(cast(pl.employmentType as varchar(max)), '') + char(10), '')
--		+ Coalesce('Placement Fee (%): ' + NULLIF(cast(pl.fee as varchar(max)), '') + char(10), '')
--		--+ Coalesce('Placement Fee (Flat): ' + NULLIF(cast(pl.flatFee as varchar(max)), '') + char(10), '')
--		+ Coalesce('Hours of Operation: ' + NULLIF(cast(pl.hoursOfOperation as varchar(max)), '') + char(10), '')
--		+ Coalesce('Hours Per Day: ' + NULLIF(cast(pl.hoursPerDay as varchar(max)), '') + char(10), '')
--		+ Coalesce('Rate Entry Type: ' + NULLIF(cast(pl.isMultirate as varchar(max)), '') + char(10), '')
--		--+ Coalesce('Mark-up %: ' + NULLIF(cast(pl.markUpPercentage as varchar(max)), '') + char(10), '')
--		+ Coalesce('Over-time Pay Rate: ' + NULLIF(cast(pl.overtimeRate as varchar(max)), '') + char(10), '')
--		+ Coalesce('Pay Rate: ' + NULLIF(cast(pl.payRate as varchar(max)), '') + char(10), '')
--				--+ Coalesce('Pay Rate Information: ' + NULLIF(cast(pl.payRateInfoHeader as varchar(max)), '') + char(10), '')
--				--+ Coalesce('Permanent Employment Info: ' + NULLIF(cast(pl.permanentInfoHeader as varchar(max)), '') + char(10), '')
--		+ Coalesce('Referral Fee Type: ' + NULLIF(cast(pl.referralFeeType as varchar(max)), '') + char(10), '')
--		+ Coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as varchar(max)), '') + char(10), '')
--		+ Coalesce('Salary: ' + NULLIF(cast(pl.salary as varchar(max)), '') + char(10), '')
--		+ Coalesce('Pay Unit: ' + NULLIF(cast(pl.salaryUnit as varchar(max)), '') + char(10), '')
--		+ Coalesce('Status: ' + NULLIF(cast(pl.status as varchar(max)), '') + char(10), '')
--		, 1, 0, '') as 'content'

--	from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
--	left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
--	left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid
--)
--, placementNotes as (
--	select
--	CandidateId
--	, string_agg([dbo].[fn_ConvertHTMLToText](content), char(10) + char(10)) as content
--	from placementNotesTmp
--	group by CandidateId
--)

-- NOTE
--, note as (
--	SELECT
--		CA.userID
--		, Stuff(
--			Coalesce('ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')  
--            + Coalesce('General Comments: ' + NULLIF(convert(varchar(max),CA.comments), '') + char(10), '')
--			--+ Coalesce('Shift Availability: ' + NULLIF(cast(ca.CustomComponent3 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Notice Period: ' + NULLIF(cast(ca.customText4 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Monthly Salary: ' + NULLIF(cast(ca.customText5 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Current Benefits: ' + NULLIF(cast(ca.customText6 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Date Available: ' + NULLIF(cast(ca.dateAvailable as varchar(max)), '') + char(10), '')
--			+ Coalesce('Available Until: ' + NULLIF(cast(ca.dateAvailableEnd as varchar(max)), '') + char(10), '')
--			+ Coalesce('CV: ' + NULLIF(UW.description, '') , '')
--			+ Coalesce('Employment Preference: ' + NULLIF(cast(ca.employmentPreference as varchar(max)), '') + char(10), '')
--			+ Coalesce('Desired Hourly Rate: ' + NULLIF(cast(ca.hourlyRate as varchar(max)), '') + char(10), '')
--            + Coalesce('Current Hourly Rate: ' + NULLIF(cast(ca.hourlyRateLow as varchar(max)), '') + char(10), '')
--			+ Coalesce('Web Responses: ' + NULLIF(cast(wr1.name as varchar(max)), '') + char(10), '')
--			+ Coalesce('Referred by: ' + NULLIF(convert(varchar(max),CA.referredBy), '') + char(10), '')
--			+ Coalesce('Referred by User: ' + NULLIF(convert(varchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
--			+ Coalesce('Status: ' + NULLIF(cast(ca.status as varchar(max)), '') + char(10), '')
--			+ Coalesce('Willing to Relocate: ' + NULLIF( cast( iif(ca.willRelocate = 1, 'No', 'Yes') as varchar(max)), '') + char(10), '')
--			+ Coalesce('Placements: ' + NULLIF(convert(varchar(max), pns.content), '') + char(10), '')


--			--+ Coalesce('LTD Company Name: ' + NULLIF(cast(ca.customText1 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company No.: ' + NULLIF(cast(ca.customText2 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Phone.: ' + NULLIF(cast(ca.customText3 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('Desired Daily Rate: ' + NULLIF(cast(ca.dayRate as varchar(max)), '') + char(10), '')
--   --         + Coalesce('Current Daily Rate: ' + NULLIF(cast(ca.dayRateLow as varchar(max)), '') + char(10), '')
--   --         + Coalesce('Employee Payment Type: ' + NULLIF(cast(ca.employeeType as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Address 1: ' + NULLIF(cast(CA.secondaryAddress1 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Address 2: ' + NULLIF(cast(CA.secondaryAddress2 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company City: ' + NULLIF(cast(CA.secondaryCity as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company County: ' + NULLIF(cast(CA.secondaryState as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Post Code: ' + NULLIF(cast(CA.secondaryZip as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Country: ' + NULLIF(cast(t.country as varchar(max)), '') + char(10), '') --CA.secondaryCountryID                     
--   --         + Coalesce('Reference: ' + NULLIF(convert(varchar(max),r.note), '') + char(10), '')
                     
--			, 1, 0, ''
--		) as note
--	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
--	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
--       --left join e3 on CA.userID = e3.ID
--       left join reference r on r.userid = ca.userid
--	left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
--       left join (SELECT userid, STUFF((
--                        SELECT char(10) + NULLIF(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
--                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
--                        FROM (   select userid, description_truong
--                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
--                        ) uw on uw.userid = ca.userid
--       left join (select * from tmp_country) t on CA.secondaryCountryID = t.code    
--	   left join placementNotes pns on CA.candidateID = pns.candidateID
--	--left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
--	--left join SkillName SN on CA.userID = SN.userId
--	--left join BusinessSector BS on CA.userID = BS.userId
--        --left join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
--        --left join admission AD on CA.userID = AD.Userid
--        --left join CName on CA.userID = CName.Userid
--        --left join SpeName on CA.userID = SpeName.Userid
--        --left join mail5 on CA.userID = mail5.ID
--        --left join summary on CA.userID = summary.CandidateID
--        --left join (select userid, status from bullhorn1.BH_Placement ) pm on pm.userid = ca.userid
--        --left join owner2c on owner2c.userid = CA.userid
--        left join wr1 on wr1.userid = CA.userid
--        --left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
--        --left join (select userid, description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
--	where CA.isPrimaryOwner = 1
--)
--select count(*) from note --8545
--select * from note --where AddedNote like '%Business Sector%'
--select top 100 * from note
-- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;
-- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;


-- Employment History -- select *  from bullhorn1.BH_userWorkHistory
, EmploymentHistory(userId, eh) as (
       SELECT a.userId, STUFF(( 
                     select char(10) + 
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
                     , 1, 0, '') as eh
                     from bullhorn1.BH_userWorkHistory
       WHERE userId = a.userId order by startDate desc
       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
       FROM bullhorn1.BH_userWorkHistory as a
       left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
       --where userid in (164043)
       GROUP BY a.userId 
       )
-- select * from EmploymentHistory where userid in (164043);


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


select --top 5
        C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
       , case 
              when C.namePrefix like '%dr%' then 'DR' 
              when C.namePrefix like '%miss%' then 'MISS' when C.namePrefix like '%ms%' then 'MISS' 
              when C.namePrefix like '%mrs%' then 'MRS' 
              when C.namePrefix like '%mr%' then 'MR'
              else '' end as 'candidate-title'
       , case 
              when C.namePrefix like '%miss%' then 'FEMALE' when C.namePrefix like '%mrs%' then 'FEMALE' when C.namePrefix like '%ms%' then 'FEMALE'
              when C.namePrefix like '%mr%' then 'MALE'
              else '' end as 'candidate-gender'
	, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	, trim(isnull(C.middleName, '')) as 'candidate-middleName'
	, trim(isnull(C.Nickname, '')) as 'Preferred Name' -->>
	, isnull(concat('', CONVERT(VARCHAR(10),C.dateOfBirth,120)), '') as 'candidate-dob'
	, isnull(iif(ed.rn > 1,concat(ed.rn, '-', ed.email), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ), '') as 'candidate-email'
	, trim(isnull(e2.email, '')) as 'candidate-PersonalEmail' -->>
	
	, C.mobile as 'candidate-phone'
	, C.mobile as 'candidate-mobile'
	, C.phone as 'candidate-homePhone'	
	, isnull(Stuff( Coalesce(' ' + NULLIF(C.phone2, ''), '')
                 + Coalesce(', ' + NULLIF(C.workPhone, ''), '')
              , 1, 1, ''), '') as 'candidate-workPhone'
	
	, case lower(trim(isnull(C.type, '')))
		when lower('Contract') then 'CONTRACT'
		when lower('Fixed Term') then 'CONTRACT'
		when lower('Temporary') then 'TEMPORARY'
		when lower('Permanent') then 'PERMANENT'

		else 'PERMANENT'
	end
	as 'candidate-jobTypes'
	
	, isnull(Stuff(  Coalesce('  ' + NULLIF(C.address1, ''), '')
                 + Coalesce(', ' + NULLIF(C.address2, ''), '') 
                 + Coalesce(', ' + NULLIF(C.city, ''), '') 
                 + Coalesce(', ' + NULLIF(C.state, ''), '') 
                 + Coalesce(', ' + NULLIF(C.zip, ''), '') 
                 + Coalesce(', ' + NULLIF(tc.country, ''), '') 
              , 1, 2, ''), '') as 'candidate-address'
	, trim(isnull(C.city, '')) as 'candidate-city'
	, trim(isnull(C.state, '')) as 'candidate-state'
	, trim(isnull(C.zip, '')) as 'candidate-zipCode'
    , isnull(tc.abbreviation, 'GB') as 'candidate-Country'
	--, tc.abbreviation 'candidate-citizenship'
	, iif(C.salaryLow is null, cast(0.00 as decimal), cast(C.salaryLow as decimal)) as 'candidate-currentSalary' --,C.customTextBlock3
	, cast(C.salary as decimal) as 'candidate-desiredSalary' --,C.customTextBlock2
	--, Education.school as 'candidate-schoolName'
	--, Education.graduationDate as 'candidate-graduationDate'
	--, Education.degree as 'candidate-degreeName'
	--, Education.major as '#candidate-major'
	--, SN.SkillName as 'candidate-skills'
         , ltrim(Stuff( Coalesce(NULLIF(SN.SkillName, '') + char(10), '')
                 --+ Coalesce(NULLIF(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
                 + Coalesce(NULLIF(convert(varchar(max),C.skillset), ''), '')
                 , 1, 0, '') ) as 'candidate-skills'		
	, trim(isnull(C.companyName, '')) as 'candidate-company1'
	, trim(isnull(C.occupation, '')) as 'candidate-jobTitle1'
	, trim(isnull(C.companyName, '')) as 'candidate-employer1'
	--, C.recruiterUserID as '#recruiterUserID'
	, trim(isnull(owner.email, '')) as 'candidate-owners'
	--, t4.finame as '#Candidate File'
	, trim(',' from concat(files.ResumeId, ',' , p.placementfile)) as 'candidate-resume'
	--, trim(isnull(note.note, '')) as 'candidate-note'
	--, left(comment.comment,32760) as 'candidate-comments'
	, trim(isnull(eh.eh, '')) as 'candidate-workHistory'
	, trim(isnull(es.es, '')) as 'candidate-education'
	--, C.companyURL as 'candidate_linkedinURL'
-- select count (*) -- select distinct gender --employmentPreference -- select skillset, skillIDlist, customTextBlock1, companyURL --select top 10 * -- select distinct namePrefix
	, iif(charindex('?', trim(isnull(C.customText15, ''))) > 0
		, left(trim(isnull(C.customText15, '')), charindex('?', trim(isnull(C.customText15, ''))) - 1)
		, trim(isnull(C.customText15, ''))
	) as [candidate-linkedln]
	, 'GBP' as [candidate-currency]
	, C.isDeleted

into VCCans

from bullhorn1.Candidate C --where C.isPrimaryOwner = 1 --8545
left join SkillName SN on C.userID = SN.userId
--left join BusinessSector BS on BS.userid = C.userid -- INDUSTRY
left join EmploymentHistory EH on EH.userid = C.userid --WORK HISTORY
left join tmp_country tc ON c.countryID = tc.code
left join owner on C.recruiterUserID = owner.recruiterUserID
--left join e1 on C.userID = e1.ID
left join ed on C.userID = ed.ID -- candidate-email-DUPLICATION
left join e2 on C.userID = e2.ID
--left join Education on C.userID = Education.userID
left join EducationSummary es on es.userID = C.userID
--left join t4 on t4.candidateUserID = C.userID
left join files on C.userID = files.candidateUserID
left join placementfiles p  on p.userid = C.userid
--left join comment on C.userID = comment.Userid
--left join note on C.userID = note.Userid
where C.isPrimaryOwner = 1
and c.isDeleted = 0
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

select * from VCCans
--where isDeleted = 0
order by [candidate-externalId] -- this is a MUST there must be ORDER BY statement
-- the paging comes here
--OFFSET     20000 ROWS       -- skip N rows
--FETCH NEXT 20000 ROWS ONLY; -- take M rows

------------------------------
--drop table if exists VCCans;

--with
---- EMAIL
--  mail1 (ID,email) as (select UC.userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(UC.email,',',UC.email2,',',UC.email3),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from bullhorn1.BH_UserContact UC left join bullhorn1.Candidate C on C.userID = UC.UserID where UC.email like '%_@_%.__%' or UC.email2 like '%_@_%.__%' or UC.email3 like '%_@_%.__%' and C.isPrimaryOwner = 1 )
--, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
--, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
--, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
--, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
--, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
--, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
--, e4 as (select ID, email from mail4 where rn = 4)
----select * from ed

---- OWNER
--, owner as (select distinct CA.recruiterUserID, UC.email, UC.name from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

---- SkillName: split by separate rows by comma, then combine them into SkillName
--, SkillName0(userid, skillID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS skillID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(skillIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
--, SkillName(userId, SkillName) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + SL.name from SkillName0 left join bullhorn1.BH_SkillList SL ON SkillName0.skillID = SL.skillID WHERE SkillName0.skillID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SkillName0 as a where a.skillID <> '' GROUP BY a.userId)

---- BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry)
--, BusinessSector0(userid, businessSectorID) as (SELECT userid, Split.a.value('.', 'VARCHAR(2000)') AS businessSectorID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') AS Split(a) )
--, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID WHERE BusinessSector0.businessSectorID <> '' and userId = a.userId FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS URLList FROM BusinessSector0 as a where a.businessSectorID <> '' GROUP BY a.userId)
----, BusinessSector(userId, BusinessSector) as (SELECT userId, BSL.name from BusinessSector0 left join bullhorn1.BH_BusinessSectorList BSL ON BusinessSector0.businessSectorID = BSL.businessSectorID )
---- select distinct BusinessSector from BusinessSector0

---- CATEGORY - VC FE info
--, CateSplit(userid, categoryid) as (SELECT userid, Split.a.value('.','varchar(2000)') AS categoryID FROM (SELECT userid, CAST('<M>' + REPLACE(cast(categoryIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1) t CROSS APPLY x.nodes('/M') as Split(a) )
--, CName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID WHERE CateSplit.categoryid <> '' and Userid = a.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM CateSplit as a where a.categoryid <> '' GROUP BY a.Userid)
----, CName(Userid, Name) as (SELECT Userid, CL.occupation from CateSplit left join bullhorn1.BH_CategoryList CL ON CateSplit.categoryid = CL.categoryID )
---- select distinct Name from CName

---- SPECIALTY - VC SFE info
--, SpecSplit(userid, specialtyid) as (SELECT userid,Split.a.value('.','varchar(2000)') AS SpecialtyID FROM (SELECT userid,CAST('<M>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</M><M>') + '</M>' AS XML) AS x FROM  bullhorn1.Candidate) t CROSS APPLY x.nodes('/M') as Split(a) )
--, SpeName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + VS.name from SpecSplit left join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID WHERE SpecSplit.specialtyid <> '' and Userid = b.Userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '') AS URLList FROM SpecSplit as b where b.specialtyid <> '' GROUP BY b.Userid)

---- ADMISSION
--, AdmissionRows(userId, CombinedText) as (select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
--, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)

---- NEWEST EDUCATION
---- select * from bullhorn1.BH_UserEducation 
--, EducationGroup as (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID)
--, Education as (
--       select EG.userID
--              , UE.certification
--              , UE.city
--              , UE.comments
--              --, UE.customText1
--              , UE.dateAdded
--              , UE.degree
--              , UE.endDate
--              , UE.expirationDate
--              , UE.gpa
--              , convert(varchar(10),UE.graduationDate,110) as graduationDate
--              , UE.major
--              , UE.school
--              , UE.startDate
--              , UE.state
--              , UE.userEducationID       
--       from EducationGroup EG left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)
---- Education Summary
--, EducationSummary(userId, es) as (
--       SELECT userId, STUFF(( 
--                     select char(10) + 
--                     stuff(
--                               Coalesce('Date Added: ' + NULLIF(cast(dateAdded as varchar(max)), '') + char(10), '')                   
--                            + Coalesce('Certification: ' + NULLIF(cast(certification as varchar(max)), '') + char(10), '')
--                            + Coalesce('City: ' + NULLIF(cast(city as varchar(max)), '') + char(10), '')
--                            + Coalesce('Comments: ' + NULLIF(cast(comments as varchar(max)), '') + char(10), '')
--                            --+ Coalesce('Country: ' + NULLIF(cast(customText1 as varchar(max)), '') + char(10), '')
--                            + Coalesce('Degree: ' + NULLIF(cast(degree as varchar(max)), '') + char(10), '')
--                            + Coalesce('End Date: ' + NULLIF(cast(endDate as varchar(max)), '') + char(10), '')
--                            + Coalesce('Expiration Date: ' + NULLIF(cast(expirationDate as varchar(max)), '') + char(10), '')
--                            + Coalesce('GPA: ' + NULLIF(cast(gpa as varchar(max)), '') + char(10), '')
--                            + Coalesce('Graduation Date: ' + NULLIF(cast(graduationDate as varchar(max)), '') + char(10), '')
--                            + Coalesce('Major: ' + NULLIF(cast(major as varchar(max)), '') + char(10), '')
--                            + Coalesce('School: ' + NULLIF(cast(school as varchar(max)), '') + char(10), '')
--                            + Coalesce('Start Date: ' + NULLIF(cast(startDate as varchar(max)), '') + char(10), '')
--                            + Coalesce('State: ' + NULLIF(cast(state as varchar(max)), '') + char(10), '')
--                            --+ Coalesce('Education ID: ' + NULLIF(cast(userEducationID as varchar(max)), '') + char(10), '')
--                     , 1, 0, '') as es
--                     from bullhorn1.BH_UserEducation
--       WHERE userId = a.userId 
--       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
--       FROM bullhorn1.BH_UserEducation as a GROUP BY a.userId 
--       )
----select * from EducationSummary where userid in (163454);
---- select * from bullhorn1.BH_UserCertification

---- Secondary OWNER
--, owner2a as (SELECT userID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT userID, CAST ('<M>' + REPLACE(convert(varchar(20),ownerUserIDList),',','</M><M>') + '</M>' AS XML) AS Data FROM bullhorn1.BH_UserContact where ownerUserIDList like '%,%') AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
--, owner2b as (select  owner2a.userid, UC.name from owner2a left join (select userid, name from bullhorn1.BH_UserContact) UC on UC.userid = owner2a.String)
--, owner2c as (SELECT userID, STUFF((SELECT ', ' + name  from owner2b WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM owner2b AS a GROUP BY a.userID )
----select * from owner2c where userid in (8281,12389,6467,10883,4281)

---- Web Responses
--, wr as (
--        select jr.userid, jp.title,jr.status
--        from bullhorn1.BH_JobResponse JR
--        left join (select CA.userID as CandidateUserID, CA.candidateID, UC.userID, UC.name as CandidateName, UC.email as CandidateEmail from bullhorn1.BH_Candidate CA left join bullhorn1.BH_UserContact UC on CA.userID = UC.userID where CA.isPrimaryOwner = 1) CAI on JR.userID = CAI.CandidateUserID
--        left join bullhorn1.BH_JobPosting  jp on jp.jobPostingID = jr.jobPostingID )
--, wr1 as (SELECT userID, STUFF((SELECT ', ' + concat('Title: ', [dbo].[ufn_RemoveForXMLUnsupportedCharacters](title),' - Status: ',status)  from wr WHERE userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM wr AS a GROUP BY a.userID )
----select * from wr1

---- Latest Comment
--, lc (userid,comments,dateAdded,rn) as ( SELECT userid, comments, dateAdded, r1 = ROW_NUMBER() OVER (PARTITION BY userid ORDER BY dateAdded desc) FROM bullhorn1.BH_UserComment )

--, reference (userId, note) as (
--       SELECT a.userId, STUFF(( 
--                     select char(10) + 
--                     stuff(
--                     --+ Coalesce('Reference ID: ' + NULLIF(cast(r.userReferenceID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Candidate Title: ' + NULLIF(cast(r.candidateTitle as varchar(max)), '') + char(10), '')
--                     + Coalesce('Client Corporation: ' + NULLIF(cast(r.clientCorporationID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Company: ' + NULLIF(cast(r.companyName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Date Added: ' + NULLIF(cast(r.dateAdded as varchar(max)), '') + char(10), '')
--                     + Coalesce('Employment End: ' + NULLIF(cast(r.employmentEnd as varchar(max)), '') + char(10), '')
--                     + Coalesce('Employment Start: ' + NULLIF(cast(r.employmentStart as varchar(max)), '') + char(10), '')
--                     + Coalesce('Job Posting: ' + NULLIF(cast(r.jobPostingID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Email: ' + NULLIF(cast(r.referenceEmail as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference First Name: ' + NULLIF(cast(r.referenceFirstName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Last Name: ' + NULLIF(cast(r.referenceLastName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Phone: ' + NULLIF(cast(r.referencePhone as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference Title: ' + NULLIF(cast(r.referenceTitle as varchar(max)), '') + char(10), '')
--                     + Coalesce('Reference: ' + NULLIF(cast(r.referenceUserID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Status: ' + NULLIF(cast(r.status as varchar(max)), '') + char(10), '')
--                     + Coalesce('Years Known: ' + NULLIF(cast(r.yearsKnown as varchar(max)), '') + char(10), '')
--                     , 1, 0, '') as note
--                     from bullhorn1.BH_UserReference r
--       WHERE userId = a.userId order by dateAdded desc
--       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
--       FROM bullhorn1.BH_UserReference as a
--       --left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
--       GROUP BY a.userId 
--       )
----select * from reference 


---- NOTE
--, note as (
--	SELECT
--		CA.userID
--		, Stuff(
--			Coalesce('ID: ' + NULLIF(cast(CA.userID as varchar(max)), '') + char(10), '')  
--            + Coalesce('General Comments: ' + NULLIF(convert(varchar(max),CA.comments), '') + char(10), '')
--			--+ Coalesce('Shift Availability: ' + NULLIF(cast(ca.CustomComponent3 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Notice Period: ' + NULLIF(cast(ca.customText4 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Monthly Salary: ' + NULLIF(cast(ca.customText5 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Current Benefits: ' + NULLIF(cast(ca.customText6 as varchar(max)), '') + char(10), '')
--			+ Coalesce('Date Available: ' + NULLIF(cast(ca.dateAvailable as varchar(max)), '') + char(10), '')
--			+ Coalesce('Available Until: ' + NULLIF(cast(ca.dateAvailableEnd as varchar(max)), '') + char(10), '')
--			+ Coalesce('CV: ' + NULLIF(UW.description, '') , '')
--			+ Coalesce('Employment Preference: ' + NULLIF(cast(ca.employmentPreference as varchar(max)), '') + char(10), '')
--			+ Coalesce('Desired Hourly Rate: ' + NULLIF(cast(ca.hourlyRate as varchar(max)), '') + char(10), '')
--            + Coalesce('Current Hourly Rate: ' + NULLIF(cast(ca.hourlyRateLow as varchar(max)), '') + char(10), '')
--			+ Coalesce('Web Responses: ' + NULLIF(cast(wr1.name as varchar(max)), '') + char(10), '')
--			+ Coalesce('Referred by: ' + NULLIF(convert(varchar(max),CA.referredBy), '') + char(10), '')
--			+ Coalesce('Referred by User: ' + NULLIF(convert(varchar(max),CA.referredByUserID), '') + ' - ' + UC.firstname + ' ' + UC.lastname + char(10), '')
--			+ Coalesce('Status: ' + NULLIF(cast(ca.status as varchar(max)), '') + char(10), '')
--			+ Coalesce('Willing to Relocate: ' + NULLIF( cast( iif(ca.willRelocate = 1, 'No', 'Yes') as varchar(max)), '') + char(10), '')


--			--+ Coalesce('LTD Company Name: ' + NULLIF(cast(ca.customText1 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company No.: ' + NULLIF(cast(ca.customText2 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Phone.: ' + NULLIF(cast(ca.customText3 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('Desired Daily Rate: ' + NULLIF(cast(ca.dayRate as varchar(max)), '') + char(10), '')
--   --         + Coalesce('Current Daily Rate: ' + NULLIF(cast(ca.dayRateLow as varchar(max)), '') + char(10), '')
--   --         + Coalesce('Employee Payment Type: ' + NULLIF(cast(ca.employeeType as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Address 1: ' + NULLIF(cast(CA.secondaryAddress1 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Address 2: ' + NULLIF(cast(CA.secondaryAddress2 as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company City: ' + NULLIF(cast(CA.secondaryCity as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company County: ' + NULLIF(cast(CA.secondaryState as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Post Code: ' + NULLIF(cast(CA.secondaryZip as varchar(max)), '') + char(10), '')
--   --         + Coalesce('LTD Company Country: ' + NULLIF(cast(t.country as varchar(max)), '') + char(10), '') --CA.secondaryCountryID                     
--   --         + Coalesce('Reference: ' + NULLIF(convert(varchar(max),r.note), '') + char(10), '')
                     
--			, 1, 0, ''
--		) as note
--	-- select top 10 * -- select count(*) -- select referredBy, referredByUserID
--	from bullhorn1.Candidate CA --where CA.isPrimaryOwner = 1 --where convert(varchar(max),CA.comments) <> ''
--       --left join e3 on CA.userID = e3.ID
--       left join reference r on r.userid = ca.userid
--	left join ( select userid, firstname, lastname from bullhorn1.BH_UserContact )UC ON UC.userID = CA.referredByUserID
--       left join (SELECT userid, STUFF((
--                        SELECT char(10) + NULLIF(description_truong, '') + char(10) + '--------------------------------------------------' + char(10)
--                        from bullhorn1.BH_UserWork where userid = a.userid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS description 
--                        FROM (   select userid, description_truong
--                                        from bullhorn1.BH_UserWork) AS a GROUP BY a.userid 
--                        ) uw on uw.userid = ca.userid
--       left join (select * from tmp_country) t on CA.secondaryCountryID = t.code                        
--	--left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
--	--left join SkillName SN on CA.userID = SN.userId
--	--left join BusinessSector BS on CA.userID = BS.userId
--        --left join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
--        --left join admission AD on CA.userID = AD.Userid
--        --left join CName on CA.userID = CName.Userid
--        --left join SpeName on CA.userID = SpeName.Userid
--        --left join mail5 on CA.userID = mail5.ID
--        --left join summary on CA.userID = summary.CandidateID
--        --left join (select userid, status from bullhorn1.BH_Placement ) pm on pm.userid = ca.userid
--        --left join owner2c on owner2c.userid = CA.userid
--        left join wr1 on wr1.userid = CA.userid
--        --left join (select * from lc where rn = 1) lc on lc.userid = CA.userid
--        --left join (select userid, description from bullhorn1.BH_UserContact) UC1 on CA.UserID = UC1.userID
--	where CA.isPrimaryOwner = 1
--)
----select count(*) from note --8545
----select * from note --where AddedNote like '%Business Sector%'
----select top 100 * from note
---- select * from bullhorn1.BH_UserCertification where licenseNumber is not null;
---- select referenceTitle,* from bullhorn1.BH_UserReference where referenceTitle is not null;


---- Employment History -- select *  from bullhorn1.BH_userWorkHistory
--, EmploymentHistory(userId, eh) as (
--       SELECT a.userId, STUFF(( 
--                     select char(10) + 
--                     stuff(
--                         Coalesce('Bonus: ' + NULLIF(cast(bonus as varchar(max)), '') + char(10), '')
--                     + Coalesce('Client Corporation: ' + NULLIF(cast(clientCorporationID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Comments: ' + NULLIF(cast(comments as varchar(max)), '') + char(10), '')
--                     + Coalesce('Commission: ' + NULLIF(cast(commission as varchar(max)), '') + char(10), '')
--                     + Coalesce('Company Name: ' + NULLIF(cast(companyName as varchar(max)), '') + char(10), '')
--                     + Coalesce('Date Added: ' + NULLIF(cast(dateAdded as varchar(max)), '') + char(10), '')
--                     + Coalesce('End Date: ' + NULLIF(cast(endDate as varchar(max)), '') + char(10), '')
--                     + Coalesce('Job Posting: ' + NULLIF(cast(title as varchar(max)), '') + char(10), '') --jobPostingID
--                     --+ Coalesce('Placement: ' + NULLIF(cast(placementID as varchar(max)), '') + char(10), '')
--                     + Coalesce('Salary Low: ' + NULLIF(cast(salary1 as varchar(max)), '') + char(10), '')
--                     + Coalesce('Salary High: ' + NULLIF(cast(salary2 as varchar(max)), '') + char(10), '')
--                     + Coalesce('Salary Type: ' + NULLIF(cast(salaryType as varchar(max)), '') + char(10), '')
--                     + Coalesce('Start Date: ' + NULLIF(cast(startDate as varchar(max)), '') + char(10), '')
--                     + Coalesce('Termination Reason: ' + NULLIF(cast(terminationReason as varchar(max)), '') + char(10), '')
--                     + Coalesce('Title: ' + NULLIF(cast(title as varchar(max)), '') + char(10), '')
--                     --+ Coalesce('User Work History ID: ' + NULLIF(cast(userWorkHistoryID as varchar(max)), '') + char(10), '')
--                            --+ Coalesce('Comments: ' + NULLIF(replace([dbo].[udf_StripHTML](comments),'Â ',''), '') + char(10), '')
--                            --+ Coalesce('Comments: ' + NULLIF(replace([dbo].[fn_ConvertHTMLToText](comments),'Â ',''), '') + char(10), '')
--                     , 1, 0, '') as eh
--                     from bullhorn1.BH_userWorkHistory
--       WHERE userId = a.userId order by startDate desc
--       FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS es 
--       FROM bullhorn1.BH_userWorkHistory as a
--       left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
--       --where userid in (164043)
--       GROUP BY a.userId 
--       )
---- select * from EmploymentHistory where userid in (164043);


---- COMMENT
--, comment(Userid, comment) as (SELECT Userid, STUFF((SELECT char(10) + 'Date Added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max)) from [bullhorn1].[BH_UserComment] WHERE Userid = a.Userid order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM [bullhorn1].[BH_UserComment] AS a GROUP BY a.Userid )
--, summary(candidateID,summary) as (SELECT candidateID, STUFF((SELECT coalesce(char(10) + 'Date Added: ' + convert(varchar,dateAdded,120) + ' || ' + 'Candidate History: ' + NULLIF(convert(varchar(max),comments), ''), '') from bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID order by dateAdded desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS summary FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID)

---- DOCUMENT
--, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF((SELECT ',' + name from bullhorn1.View_CandidateFile WHERE candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID)

---- Files
--, files(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + concat(candidateFileID, fileExtension) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID) --where a.type = 'Resume') ==> get all candidates files

---- Placement Files
--, placementfiles(userID, placementfile) as (SELECT userID, STUFF((SELECT DISTINCT ',' + concat(placementFileID, fileExtension) from bullhorn1.View_PlacementFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_PlacementFile AS a GROUP BY a.userID)
----select top 10 * from placementfiles


--select --top 5
--        C.candidateID as 'candidate-externalId' --, C.userID as '#userID'
--       , case 
--              when C.namePrefix like '%dr%' then 'DR' 
--              when C.namePrefix like '%miss%' then 'MISS' when C.namePrefix like '%ms%' then 'MISS' 
--              when C.namePrefix like '%mrs%' then 'MRS' 
--              when C.namePrefix like '%mr%' then 'MR'
--              else '' end as 'candidate-title'
--       , case 
--              when C.namePrefix like '%miss%' then 'FEMALE' when C.namePrefix like '%mrs%' then 'FEMALE' when C.namePrefix like '%ms%' then 'FEMALE'
--              when C.namePrefix like '%mr%' then 'MALE'
--              else '' end as 'candidate-gender'
--	, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
--       , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
--	, C.middleName as 'candidate-middleName'
--	, C.Nickname as 'Preferred Name' -->>
--	, CONVERT(VARCHAR(10),C.dateOfBirth,120) as 'candidate-dob'
--	, iif(ed.rn > 1,concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemailaddress.co'),ed.email) ) as 'candidate-email'
--	, e2.email as 'candidate-PersonalEmail' -->>
	
--	, C.mobile as 'candidate-phone'
--	, C.mobile as 'candidate-mobile'
--	, C.phone as 'candidate-homePhone'	
--	, Stuff( Coalesce(' ' + NULLIF(C.phone2, ''), '')
--                 + Coalesce(', ' + NULLIF(C.workPhone, ''), '')
--              , 1, 1, '') as 'candidate-workPhone'
	
--	, case lower(trim(isnull([type], '')))
--		when lower('Contract') then 'CONTRACT'
--		when lower('Fixed Contract') then 'CONTRACT'
--		when lower('Temporary') then 'CONTRACT'
--		when lower('Permanent') then 'PERMANENT'
--		when lower('Project') then 'PROJECT_CONSULTING'
--		when lower('Temp to Perm') then 'TEMP_TO_PERMANENT'
--		when lower('Perm & Contract') then 'PERMANENT,CONTRACT'

--		else 'PERMANENT'
	
--	end as 'candidate-jobTypes'
	
--	, Stuff(  Coalesce('  ' + NULLIF(C.address1, ''), '')
--                 + Coalesce(', ' + NULLIF(C.address2, ''), '') 
--                 + Coalesce(', ' + NULLIF(C.city, ''), '') 
--                 + Coalesce(', ' + NULLIF(C.state, ''), '') 
--                 + Coalesce(', ' + NULLIF(C.zip, ''), '') 
--                 + Coalesce(', ' + NULLIF(tc.country, ''), '') 
--              , 1, 2, '') as 'candidate-address'
--	, C.city as 'candidate-city'
--	, C.state as 'candidate-state'
--	, C.zip as 'candidate-zipCode'
--    , tc.abbreviation 'candidate-Country'
--	--, tc.abbreviation 'candidate-citizenship'
--	, cast(C.salaryLow as int) as 'candidate-currentSalary' --,C.customTextBlock3
--	, cast(C.salary as int) as 'candidate-desiredSalary' --,C.customTextBlock2
--	--, Education.school as 'candidate-schoolName'
--	--, Education.graduationDate as 'candidate-graduationDate'
--	--, Education.degree as 'candidate-degreeName'
--	--, Education.major as '#candidate-major'
--	--, SN.SkillName as 'candidate-skills'
--         , ltrim(Stuff( Coalesce(NULLIF(SN.SkillName, '') + char(10), '')
--                 --+ Coalesce(NULLIF(convert(varchar(max),C.customTextBlock1), '') + char(10), '')
--                 + Coalesce(NULLIF(convert(varchar(max),C.skillset), ''), '')
--                 , 1, 0, '') ) as 'candidate-skills'		
--	, C.companyName as 'candidate-company1'
--	, C.occupation as 'candidate-jobTitle1'
--	, C.companyName as 'candidate-employer1'
--	--, C.recruiterUserID as '#recruiterUserID'
--	, owner.email as 'candidate-owners'
--	--, t4.finame as '#Candidate File'
--	, trim(',' from concat(files.ResumeId, ',' , p.placementfile)) as 'candidate-resume'
--	, note.note as 'candidate-note'
--	--, left(comment.comment,32760) as 'candidate-comments'
--	, eh.eh as 'candidate-workHistory'
--	, es.es as 'candidate-education'
--	--, C.companyURL as 'candidate_linkedinURL'
---- select count (*) -- select distinct gender --employmentPreference -- select skillset, skillIDlist, customTextBlock1, companyURL --select top 10 * -- select distinct namePrefix

--into VCCans

--from bullhorn1.Candidate C --where C.isPrimaryOwner = 1 --8545
--left join SkillName SN on C.userID = SN.userId
----left join BusinessSector BS on BS.userid = C.userid -- INDUSTRY
--left join EmploymentHistory EH on EH.userid = C.userid --WORK HISTORY
--left join tmp_country tc ON c.countryID = tc.code
--left join owner on C.recruiterUserID = owner.recruiterUserID
----left join e1 on C.userID = e1.ID
--left join ed on C.userID = ed.ID -- candidate-email-DUPLICATION
--left join e2 on C.userID = e2.ID
----left join Education on C.userID = Education.userID
--left join EducationSummary es on es.userID = C.userID
----left join t4 on t4.candidateUserID = C.userID
--left join files on C.userID = files.candidateUserID
--left join placementfiles p  on p.userid = C.userid
----left join comment on C.userID = comment.Userid
--left join note on C.userID = note.Userid
--where C.isPrimaryOwner = 1
----and C.userid in (37380,5)
----and (C.FirstName like '%Partha%' or C.LastName like '%Partha%')
----and concat (C.FirstName,' ',C.LastName) like '%Partha%'
----and e1.email = '' or e1.email is null --e1.email <> ''
----inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
----left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID

--/*
--select    C.candidateID as 'externalId'
--	, C.Nickname as 'PreferredName'
--        , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
--        , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
--from bullhorn1.Candidate C
--where Nickname <> '' and Nickname is not null


--with t as (
--select    C.candidateID as 'externalId'
--        , Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
--        , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
--        , C.source as original
--        , case 
--                when c.source like '%Inde%' then 29085
--                when c.source like '%Volc%' then 29084
--                when c.source like '%Dova%' then 29086
--                when c.source like '%Broa%' then 29089
--                when c.source like '%Head%' then 29091
--                when c.source like '%Refe%' then 29092
--                when c.source like '%Data%' then 29093
--                when c.source like '%Inst%' then 29087
--                when c.source like '%Logi%' then 29094
--                when c.source like '%Auto%' then 29095
--                when c.source like '%Inde%' then 29098
--                when c.source like '%Alph%' then 29096
--                when c.source like '%Face%' then 29097
--                when c.source like '%Jobstreet%' then 29099
--                when c.source like '%Lee %' then 29101
--                when c.source like '%Link%' then 29090
--                when c.source like '%Link%Job%' then 29102
--                when c.source like '%Regi%' then 29103
--                when c.source like '%JobsDB%' then 29100
--        else '' end as 'candidate_source_id'
--from bullhorn1.Candidate C
--where source <> '' and source is not null )
----select count(*) from t where source is not null
--select * from t where candidate_source_id <> 0 and candidate_source_id <> '29093'
----where externalid = 13144

--*/

--select * from VCCans