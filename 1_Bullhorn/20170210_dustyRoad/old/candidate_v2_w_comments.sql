with 
t3(candidateUserID, name) as (select a.candidateUserID, a.name 
from bullhorn1.View_CandidateFile a where a.type = 'Resume')

, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF(
         (SELECT ',' + name
          from t3
          WHERE candidateUserID = a.candidateUserID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM t3 AS a GROUP BY a.candidateUserID)

, tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email
from bullhorn1.BH_UserContact)
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
--select * from tmp_3

, tmp_5 (candidateUserID, name) as (select a.candidateUserID, concat(a.candidateFileID, a.fileExtension)
from bullhorn1.View_CandidateFile a where a.type = 'Resume')
--select * from tmp_5

, tmp_6(candidateUserID, ResumeId) as (SELECT
     candidateUserID,
     STUFF(
         (SELECT DISTINCT ',' + name
          from tmp_5
          WHERE candidateUserID = a.candidateUserID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM tmp_5 AS a GROUP BY a.candidateUserID)
--select * from tmp_6 order by candidateUserID

, tmp_email_1 as (
select c.candidateID, case 
	when c.email is not null and c.email <> '' then c.email
	when c.email2 is not null and c.email2 <> '' then c.email2
	when c.email3 is not null and c.email3 <> '' then c.email3
	else ''	
	end as email from bullhorn1.Candidate c
)
, tmp_email_2 as (
select email, min(candidateID) as candidateID from tmp_email_1 group by email
)
--select * from tmp_email_2

, tmp_note(Userid, Notes) as (SELECT
     Userid,
     STUFF(
         (SELECT DISTINCT ' || ' + convert(varchar(10), dateAdded, 120) + ': ' + cast(comments as varchar(max))
          from  [bullhorn1].[BH_UserComment]
          WHERE Userid = a.Userid
          FOR XML PATH (''))
          , 1, 4, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a
GROUP BY a.Userid)

, t1 as (
select C.userID 
	, case C.gender 
		when 'M' then 'Mr.'
		when 'F' then 'Ms.'
		else '' end as 'candidate-title'
	, C.candidateID as 'candidate-externalId'
	, C.firstName as 'candidate-firstName'
	, C.[lastName] as 'candidate-Lastname'
	, c.middleName as 'candidate-middleName'
	, CONVERT(VARCHAR(10),C.dateOfBirth,110) as 'candidate-dob'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN ''
		ELSE tc.abbreviation
		END as 'candidate-citizenship'
	, case 
		when c.email is not null and c.email <> '' then c.email
		when c.email2 is not null and c.email2 <> '' then c.email2
		when c.email3 is not null and c.email3 <> '' then c.email3
		else ''	
		end as 'candidate-email'
		, C.phone as 'candidate-mobile'
		, C.phone2 as 'candidate-phone'	
		, C.workPhone as 'candidate-workPhone'
		, C.address1 as 'candidate-address'
		, C.city as 'candidate-city'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN ''
		ELSE tc.abbreviation
		END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		, C.state
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
	, UC2.email as 'candidate-owners'
	, t4.finame as 'candidate-resume'
	, tmp_6.ResumeId as 'candidate-resume-with-ID'
	, C.comments as 'candidate-note'
	, replace(left(cast(tmp_note.Notes as varchar(max)),30000),'&#x0D;','') as 'candidate-comments'
from bullhorn1.Candidate C
left join tmp_country tc ON c.countryID = tc.code
left join bullhorn1.BH_UserContact UC2 on C.recruiterUserID = UC2.userID
--left join tmp_3 te ON te.userID = c.userID
left join (select userID, min(userEducationID) as userEducationID from [bullhorn1].[BH_UserEducation] group by userID) UE_2 on C.userID = UE_2.userID
left join (select * from [bullhorn1].[BH_UserEducation] where isDeleted = 0) UE on UE.userEducationID = UE_2.userEducationID
left join t4 on t4.candidateUserID = C.userID
left join tmp_6 on C.userID = tmp_6.candidateUserID
left join tmp_note on C.userID = tmp_note.Userid
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
where C.status not like '%Archive%'
)
--select * from t1

select t1.* 
from t1 --inner join tmp_email_2 on t1.[candidate-externalId] = tmp_email_2.candidateID
order by userID