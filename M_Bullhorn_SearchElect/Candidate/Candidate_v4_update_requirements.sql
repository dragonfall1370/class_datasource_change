with sn1(userid, skillID) as
(SELECT userid,
LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS skillID
FROM
(
SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(skillIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
FROM  bullhorn1.Candidate
where isPrimaryOwner = 1
)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)

, sn1_1(userId, SkillName) as (
select sn1.userid, SL.name
from sn1 inner join 
bullhorn1.BH_SkillList SL ON sn1.skillID = SL.skillID
)

, SkillName(userId, SkillName) as (SELECT
     userId,
     STUFF(
         (SELECT DISTINCT ', ' + SkillName
          from  sn1_1
          WHERE userId = a.userId
          FOR XML PATH (''))
          , 1, 2, '')  AS URLList
FROM sn1_1 as a
GROUP BY a.userId)

/* sn1 > SkillName: split by separate rows by comma, then combine them into SkillName */

, bs1(userid, businessSectorID) as
(SELECT userid,
LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS businessSectorID
FROM
(
SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x
FROM  bullhorn1.Candidate
where isPrimaryOwner = 1
)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)

, bs1_1(userId, businessSector) as (
select bs1.userid, BSL.name
from bs1 inner join 
bullhorn1.BH_BusinessSectorList BSL ON bs1.businessSectorID = BSL.businessSectorID
)

, BusinessSector(userId, BusinessSector) as (SELECT
     userId,
     STUFF(
         (SELECT DISTINCT ', ' + businessSector
          from  bs1_1
          WHERE userId = a.userId
          FOR XML PATH (''))
          , 1, 2, '')  AS URLList
FROM bs1_1 as a
GROUP BY a.userId)

/* bs1 > BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry) */

, t3(candidateUserID, name) as (select a.candidateUserID, a.name
from bullhorn1.View_CandidateFile a
where a.type = 'Resume')
, t4(candidateUserID, finame) as (SELECT
     candidateUserID,
     STUFF(
         (SELECT ',' + name
          from t3
          WHERE candidateUserID = a.candidateUserID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM t3 AS a
GROUP BY a.candidateUserID)
, tmp_1(userID, email) as 
(select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email
from bullhorn1.BH_UserContact
 )
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (
select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)
	ELSE email END as email
from tmp_1
)
 , tmp_3(userID, email) as (
select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) 
	THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)
	ELSE email END as email
from tmp_2
)

, tmp_5 (candidateUserID, name) as (select a.candidateUserID, concat(a.candidateFileID, a.fileExtension)
from bullhorn1.View_CandidateFile a
where a.type = 'Resume')

, tmp_6(candidateUserID, ResumeId) as (SELECT
     candidateUserID,
     STUFF(
         (SELECT DISTINCT ',' + name
          from tmp_5
          WHERE candidateUserID = a.candidateUserID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM tmp_5 AS a
GROUP BY a.candidateUserID)
--select * from tmp_6 order by candidateUserID

, tmp_email_1 as (
select c.candidateID, case 
	when c.email is not null and c.email <> '' then c.email
	when c.email2 is not null and c.email2 <> '' then c.email2
	when c.email3 is not null and c.email3 <> '' then c.email3
	else ''	
	end as email from bullhorn1.Candidate c
	where isPrimaryOwner = 1
)

/* Remove duplicated emails */
, tmp_email_2 as (
select email, min(candidateID) as candidateID from tmp_email_1 group by email
)

/* Recruiter as candidate owners */
, tmp_email_3 as (
select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA
left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID
where CA.isPrimaryOwner = 1)

, tmp_note(Userid, Notes) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT ' || ' + 'Action: ' + action + ' || ' + convert(varchar(10), dateAdded, 120) + ': ' + cast(comments as varchar(max))
          from  [bullhorn1].[BH_UserComment]
          WHERE Userid = a.Userid
          FOR XML PATH (''))
          , 1, 4, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a
GROUP BY a.Userid)

, tmp_addednote as (SELECT
     CA.userID,
	 concat('Date Added: ',convert(varchar(10),dateAdded,120),char(10)
	 ,'Status: ',status,char(10)
	 ,iif(referredByUserID = '' or referredByUserID is NULL,'',concat('Reffered by UserID: ',referredByUserID,char(10)))
	 ,iif(referredBy = '' or referredBy is NULL,'',concat('Reffered by: ',referredBy,char(10)))
	 ,iif(phone2 = '' or phone2 is NULL,'',concat('Phone 2: ',phone2,char(10)))
	 ,iif(cast(desiredLocations as varchar(2)) = '' or desiredLocations is NULL,'',concat('Desired Locations: ',tmp_country.COUNTRY,char(10)))
	 ,concat('Skills: ',SN.SkillName,char(10)),concat('Business Sector: ',BS.BusinessSector)) as AddedNote
	 ,CA.desiredLocations
	 ,tmp_country.ABBREVIATION
	 ,tmp_country.COUNTRY
	 from bullhorn1.Candidate CA
	 left join SkillName SN on CA.userID = SN.userId
	 left join BusinessSector BS on CA.userID = BS.userId
	 left join tmp_country on cast(CA.desiredLocations as varchar(2)) = tmp_country.ABBREVIATION
	 where CA.isPrimaryOwner = 1)

, t1 as (
select C.userID 
	, case C.gender 
	when 'M' then 'MR'
	when 'F' then 'MISS'
	else '' end as 'candidate-title'
	, case C.gender 
	when 'M' then 'MALE'
	when 'F' then 'FEMALE'
	else '' end as 'candidate-gender'
	, C.candidateID as 'candidate-externalId'
	, C.firstName as 'candidate-firstName'
	, C.[lastName] as 'candidate-Lastname'
	, c.middleName as 'candidate-middleName'
	,CONVERT(VARCHAR(10),C.dateOfBirth,110) as 'candidate-dob'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN ''
	ELSE tc.abbreviation
	END as 'candidate-citizenship'
	, UC2.email as 'candidate-email'
	, UC2.email2 as 'candidate-workEmail'
	, C.phone as 'candidate-homePhone'
	, C.phone2 as 'candidate-phone'	
	, C.mobile as 'candidate-mobile'
	, C.workPhone as 'candidate-workPhone'
	, 'PERMANENT' as 'candidate-jobTypes'
	, C.address1 as 'candidate-address'
	, C.city as 'candidate-city'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN ''
	ELSE tc.abbreviation
	END as 'candidate-Country'
	, C.zip as 'candidate-zipCode'
	, C.state as 'candiadte-state'
	, C.salaryLow as 'candidate-currentSalary'
	, C.salary as 'candidate-desiredSalary'
	, UE.[school] as 'candidate-schoolName'
	, CONVERT(VARCHAR(10),UE.graduationDate,110) as 'candidate-graduationDate'
	, UE.[degree] as 'candidate-degreeName'
	, UE.[gpa] as 'candidate-gpa'
	, C.companyName as 'candidate-company1'
	, C.occupation as 'candidate-jobTitle1'
	, C.companyName as 'candidate-employer1'
	, C.recruiterUserID
	, tmp_email_3.email as 'candidate-owners'
	, t4.finame as 'Candidate File'
	, tmp_6.ResumeId as 'candidate-resume'
	, concat('Bullhorn Candidate ID: ',C.candidateID,char(10)
	, replace(AN.AddedNote,'&amp;','&'),char(10),C.comments) as 'candidate-notes'
	, replace(left(concat('Bullhorn Candidate ID: ',C.candidateID,char(10),replace(AN.AddedNote,'&amp;','&'),char(10)
	, cast(tmp_note.Notes as varchar(max))),32000),'&#x0D;','') as 'candidate-comments'
from bullhorn1.Candidate C
left join tmp_country tc ON c.countryID = tc.code
left join bullhorn1.BH_UserContact UC2 on C.userID = UC2.userID
left join tmp_email_3 on C.recruiterUserID = tmp_email_3.recruiterUserID
--left join tmp_3 te ON te.userID = c.userID
left join (select userID, min(userEducationID) as userEducationID from [bullhorn1].[BH_UserEducation] group by userID) UE_2 on C.userID = UE_2.userID
left join (select * from [bullhorn1].[BH_UserEducation] where isDeleted = 0) UE on UE.userEducationID = UE_2.userEducationID
left join t4 on t4.candidateUserID = C.userID
left join tmp_6 on C.userID = tmp_6.candidateUserID
left join tmp_note on C.userID = tmp_note.Userid
left join tmp_addednote AN on C.userID = AN.Userid
where C.isPrimaryOwner = 1
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
)
select * from t1 
--inner join tmp_email_2 on t1.[candidate-externalId] = tmp_email_2.candidateID
--order by userID

/* Check if candidate is not primary owner

select userID from bullhorn1.Candidate
where isPrimaryOwner = 1
group by userID having count(*) > 1

*/

select * from t1