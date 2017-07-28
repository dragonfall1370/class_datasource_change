/*
with 

/* sn1 > SkillName: split by separate rows by comma, then combine them into SkillName */
  sn1(userid, skillID) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS skillID FROM
(SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(skillIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n))
, sn1_1(userId, SkillName) as (select sn1.userid, SL.name from sn1 inner join bullhorn1.BH_SkillList SL ON sn1.skillID = SL.skillID)
, SkillName(userId, SkillName) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + SkillName from  sn1_1 WHERE userId = a.userId FOR XML PATH ('')), 1, 2, '')  AS URLList FROM sn1_1 as a GROUP BY a.userId)


/* bs1 > BusinessSector: split by separate rows by comma, then combine them into Business Sector(Industry) */
, bs1(userid, businessSectorID) as (SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS businessSectorID 
FROM (SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(businessSectorIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n))
, bs1_1(userId, businessSector) as (select bs1.userid, BSL.name from bs1 inner join bullhorn1.BH_BusinessSectorList BSL ON bs1.businessSectorID = BSL.businessSectorID)
, BusinessSector(userId, BusinessSector) as (SELECT userId, STUFF((SELECT DISTINCT ', ' + businessSector from  bs1_1 WHERE userId = a.userId FOR XML PATH ('')), 1, 2, '')  AS URLList FROM bs1_1 as a GROUP BY a.userId)


/* DOCUMENT */
, t3(candidateUserID, name) as (select a.candidateUserID, a.name from bullhorn1.View_CandidateFile a) --where a.type = 'Resume'
, t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF((SELECT ',' + name from t3 WHERE candidateUserID = a.candidateUserID FOR XML PATH ('')), 1, 1, '')  AS URLList FROM t3 AS a GROUP BY a.candidateUserID)

/* Get candidate category - VC FE info */
, CateSplit(userid, categoryid) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS categoryID FROM 
(SELECT userid, CAST('<XMLRoot><RowData>' + REPLACE(cast(categoryIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.Candidate where isPrimaryOwner = 1)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)

, CombinedCate (userid, categoryid) as (
select distinct userid, categoryid from (select userid, categoryid from CateSplit 
UNION ALL
select userid, categoryid from bullhorn1.Candidate where isPrimaryOwner = 1) a
)

/* CATEGORY */
, CateName(userId, Categoryname) as (
select CombinedCate.userid, CL.occupation from CombinedCate inner join bullhorn1.BH_CategoryList CL ON CombinedCate.categoryid = CL.categoryID)
, CName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + Categoryname from CateName WHERE Userid = a.Userid FOR XML PATH ('')), 1, 2, '')  AS URLList FROM CateName as a GROUP BY a.Userid)

/* Get candidate specialty - VC SFE info */
, SpecSplit(userid, specialtyid) as
(SELECT userid,LTRIM(RTRIM(m.n.value('.[1]','varchar(8000)'))) AS SpecialtyID FROM
(SELECT userid,CAST('<XMLRoot><RowData>' + REPLACE(cast(specialtyIDList as varchar(max)),',','</RowData><RowData>') + '</RowData></XMLRoot>' AS XML) AS x FROM  bullhorn1.Candidate)t
CROSS APPLY x.nodes('/XMLRoot/RowData')m(n)
)
, SpecName(userId, Specialtyname) as (select SpecSplit.userid, VS.name from SpecSplit inner join bullhorn1.View_Specialty VS ON SpecSplit.SpecialtyID = VS.specialtyID)
, SpeName(Userid, Name) as (SELECT Userid, STUFF((SELECT DISTINCT ', ' + Specialtyname from  SpecName WHERE Userid = b.Userid FOR XML PATH ('')), 1, 2, '')  AS URLList FROM SpecName as b GROUP BY b.Userid)

/* Get candidates bar admission */
, AdmissionRows(userId, CombinedText) as (
select UCOI.userID, concat(text1,' ',text2) as CombinedText from bullhorn1.BH_UserCustomObjectInstance UCOI
inner join bullhorn1.BH_CustomObjectInstance COI On UCOI.instanceID = COI.instanceID)
, admission(Userid, Admission) as (SELECT Userid, STUFF((SELECT ' || ' + CombinedText from  AdmissionRows WHERE Userid = c.Userid and CombinedText is not NULL and CombinedText <> '' FOR XML PATH ('')), 1, 4, '')  AS URLList FROM  AdmissionRows as c GROUP BY c.Userid)

/* Get candidates info about Category, Specialty, Bar Admission for Zensho */
, ZenshoInfo as (select CA.userID
	, CA.candidateID
	, Concat(iif(AD.Admission='' or AD.Admission is NULL,'',concat('Bar admission: ',AD.Admission,char(10)))
	, iif(CA.customText2='' or CA.customText2 is NULL,'',concat('General Work Function: ',CA.customText2,char(10)))
	, concat('Practice Area / Category: ',replace(CName.Name,'&amp;','&'),char(10))
	, iif(cast(CA.specialtyIDList as varchar(max)) = '' or CA.specialtyIDList is NULL,'',concat('Specialty: ',replace(SpeName.name,'&amp;','&')))) as MoreCandInfo
from bullhorn1.Candidate CA
left outer join admission AD on CA.userID = AD.Userid
left outer join bullhorn1.BH_BusinessSectorList BSL on cast(CA.businessSectorIDList as varchar(max)) = cast(BSL.businessSectorID as varchar(max))
left outer join CName on CA.userID = CName.Userid
left outer join SpeName on CA.userID = SpeName.Userid
where ca.isPrimaryOwner = 1)


-- Get all mail from email, email2, email3. That will be applied for contact primary emails
, tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact) 
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)	ELSE email END as email from tmp_1)
 , tmp_3(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)	ELSE email END as email from tmp_2) */

-- Get candidates files  
, tmp_5 (candidateUserID, name) as (select a.candidateUserID, concat(a.candidateFileID, a.fileExtension) from bullhorn1.View_CandidateFile a)
--where a.type = 'Resume') ==> get all candidates files
, tmp_6(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + name from tmp_5 WHERE candidateUserID = a.candidateUserID FOR XML PATH ('')), 1, 1, '')  AS URLList FROM tmp_5 AS a GROUP BY a.candidateUserID)
--select * from tmp_6 order by candidateUserID

-- Get candidates email from BH Candidate 
, tmp_email_1 as (
select c.candidateID, case 
	when c.email is not null and c.email <> '' then c.email
	when c.email2 is not null and c.email2 <> '' then c.email2
	when c.email3 is not null and c.email3 <> '' then c.email3
	else concat('candidate',candidateID,'_noemail@aquilamyanmar.com')
	end as email from bullhorn1.Candidate c
	where isPrimaryOwner = 1
)
--select * from tmp_email_1 where tmp_email_1.email like '%zensho%'

-- Remove duplicated emails 
, tmp_email_2 as (select email, min(candidateID) as candidateID from tmp_email_1 group by email)

-- Recruiter as candidate owners --
, tmp_email_3 as (select distinct CA.recruiterUserID, UC.email from bullhorn1.Candidate CA left join bullhorn1.BH_UserContact UC on CA.recruiterUserID = UC.userID where CA.isPrimaryOwner = 1)

, tmp_note(Userid, Notes) as (SELECT Userid,
     STUFF(
         (SELECT char(10) + 'Date added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max))
          from  [bullhorn1].[BH_UserComment]
          WHERE Userid = a.Userid
		  order by dateAdded desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a
GROUP BY a.Userid)

-- candidate-note --
, tmp_addednote as (SELECT
     CA.userID,
	 concat('Date registered: ',convert(varchar(10),dateAdded,120),char(10)
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
	 where CA.isPrimaryOwner = 1)

, EducationGroup as (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID)
, Education as (select EG.userID, UE.userEducationID, UE.school, convert(varchar(10),UE.graduationDate,110) as graduationDate
, UE.degree
, UE.major
, UE.comments
from EducationGroup EG
left join bullhorn1.BH_UserEducation UE on EG.userEducationID = UE.userEducationID)

, t1 as (
*/

with skill as (select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid)
--select top 10 * from skill

select top 100
-- select
	 CL.ContactId as 'candidate-externalId'
	, owner.email as 'candidate-owners'
	, CL.username as 'candidate-owners-'
	, CL.UserId as 'candidate-owners-id'
	, case when ( CL.FirstName = '' or CL.FirstName is null) then 'No Firstname' else replace(CL.FirstName,'?','') end as 'candidate-firstName'
	, case when ( CL.LastName = '' or CL.LastName is null) then 'No Lastname' else replace(CL.LastName,'?','') end as 'candidate-Lastname'
	, CL.Title as 'candidate-title'
	, case
		when CL.Title like '%Ms%' then 'FEMALE' 
		when CL.Title like '%Mrs%' then 'FEMALE' 
		when CL.Title like '%Miss%' then 'FEMALE'
		when CL.Title like '%Md%' then 'FEMALE'
		when CL.Title like '%Mr%' then 'MALE' 
		when CL.Title = 'M' then 'MALE' 
		when CL.Title like '%Sir%' then 'MALE' 
		else '' 
		end as 'candidate-gender'
	, CL.JobTitle as 'candidate-jobTitle1'
	, CL.Company as 'candidate-Company1'
	, CL.CompanyId as 'candidate-Company External ID'
	
	, replace(replace(replace(replace(replace(
		case when ( CL.Email != '' and CL.Email is not null and CL.Email like '%@%')
	 	then CL.Email else (case when ( CL.EMail2 != '' and CL.EMail2 is not null and CL.Email2 like '%@%') then CL.EMail2 else '' end) end
	 	,'?',''),'&',''),'gmail','@gmail'),'yahoo','@yahoo'),'@@','@') as 'candidate-email'
	 , CL.EMail2 as 'candidate-workEmail'
	
	, case when ( cast(CL.DirectTel as varchar(max)) != '' and cast(CL.DirectTel as varchar(max)) is not null) then cast(CL.DirectTel as varchar(max)) else
	 (case when ( cast(CL.MobileTel as varchar(max)) != '' and cast(CL.MobileTel as varchar(max)) is not null) then cast(CL.MobileTel as varchar(max)) else CL.WorkTel end)
	  end as 'candidate-phone' --primary phone
	--, CL.DirectTel as 'candidate-phone'
	, CL.MobileTel as 'candidate-mobile'
	, CL.WorkTel as 'candidate-workPhone'
	, CL.HomeTel as 'candidate-homePhone'
	
	, CL.Address1 as 'candidate-address'
	, CL.City as 'candidate-city'
	, CL.County as 'candidate-state'
	--, CL.Country as 'candidate-country'
	, case	
		when CL.Country like 'Australia%' then 'AU'
		when CL.Country like 'Austria%' then 'AT'
		when CL.Country like 'Bahrain%' then 'BH'
		when CL.Country like 'Belgium%' then 'BE'
		when CL.Country like 'Boulogne%' then 'FR'
		when CL.Country like 'Brazil%' then 'BR'
		when CL.Country like 'Brussels%' then 'BE'
		when CL.Country like 'Bulgaria%' then 'BG'
		when CL.Country like 'Canada%' then 'CA'
		when CL.Country like 'China%' then 'CN'
		when CL.Country like 'Clichy%' then 'FR'
		when CL.Country like 'Colombia%' then 'CO'
		when CL.Country like 'Croatia/Hrvatkska%' then 'HR'
		when CL.Country like 'Czech%' then 'CZ'
		when CL.Country like 'Denmark%' then 'DK'
		when CL.Country like 'Dubai%' then 'UAE'
		when CL.Country like 'England%' then 'GB'
		when CL.Country like 'Finland%' then 'FI'
		when CL.Country like 'France.%' then 'FR'
		when CL.Country like 'France%' then 'FR'
		when CL.Country like 'Germany%' then 'DE'
		when CL.Country like 'GREECE%' then 'GR'
		when CL.Country like 'Hampshire%' then 'GB'
		when CL.Country like 'Herns%' then 'UK'
		when CL.Country like 'Hong%' then 'CN'
		when CL.Country like 'Hungary%' then 'HU'
		when CL.Country like 'India%' then 'IN'
		when CL.Country like 'Indonesia%' then 'ID'
		when CL.Country like 'Ireland%' then 'IE'
		when CL.Country like 'Israel%' then 'IL'
		when CL.Country like 'Italy%' then 'IT'
		when CL.Country like 'ITALY%' then 'IT'
		when CL.Country like 'Japan%' then 'JP'
		when CL.Country like 'Jordan%' then 'JO'
		when CL.Country like 'Korea%' then 'KP KR'
		when CL.Country like 'Lebanon%' then 'LB'
		when CL.Country like 'Levallois-Perret%' then 'FR'
		when CL.Country like 'Levallois%' then 'FR'
		when CL.Country like 'Lithuania%' then 'LT'
		when CL.Country like 'London%' then 'GB'
		when CL.Country like 'Luxembourg%' then 'LU'
		when CL.Country like 'Lyon%' then 'FR'
		when CL.Country like 'M34%' then 'UK'
		when CL.Country like 'Malaysia%' then 'MY'
		when CL.Country like 'Mexico%' then 'MX'
		when CL.Country like 'N14%' then 'GB'
		when CL.Country like 'Netherlands%' then 'NL'
		when CL.Country like 'New%' then 'NZ'
		when CL.Country like 'Norway%' then 'NO'
		when CL.Country like 'Pakistan%' then 'PK'
		when CL.Country like 'Paris%' then 'FR'
		when CL.Country like 'Peru%' then 'PE'
		when CL.Country like 'Philippines%' then 'PH'
		when CL.Country like 'Poland%' then 'PL'
		when CL.Country like 'Portugal%' then 'PT'
		when CL.Country like 'PR%' then 'PR'
		when CL.Country like 'Qatar%' then 'QA'
		when CL.Country like 'ROMANIA%' then 'RO'
		when CL.Country like '%Russian%' then 'RU'
		when CL.Country like 'Saudi%' then 'SA'
		when CL.Country like 'Scotland%' then 'UK'
		when CL.Country like 'Singapore%' then 'SG'
		when CL.Country like 'Slovenia%' then 'SI'
		when CL.Country like 'South%' then 'ZA'
		when CL.Country like 'Spain%' then 'ES'
		when CL.Country like 'Sweden%' then 'SE'
		when CL.Country like 'Switzerland%' then 'CH'
		when CL.Country like 'Swizerland%' then ''
		when CL.Country like 'thailand%' then 'TH'
		when CL.Country like 'The%' then 'NL'
		when CL.Country like 'Turkey%' then 'TR'
		when CL.Country like 'UKraine%' then 'UA'
		when CL.Country like 'UK%' then 'UA'
		when CL.Country like 'Wales%' then 'UK'
		when CL.Country like '%UNITED%ARAB%' then 'AE'
		when CL.Country like '%UAE%' then 'AE'
		when CL.Country like '%U.A.E%' then 'AE'
		when CL.Country like '%UNITED%KINGDOM%' then 'GB'
		when CL.Country like '%UNITED%STATES%' then 'US'
		when CL.Country like '%US%' then 'US'
	end as 'candidate-country'
	
	, concat(
		  case when (CL.Location = '' OR CL.Location is NULL) THEN '' ELSE concat ('Location: ',CL.Location,char(10)) END
		, case when (CL.SubLocation = '' OR CL.SubLocation is NULL) THEN '' ELSE concat ('SubLocation: ',CL.SubLocation,char(10)) END
		, case when (CL.WebSite = '' OR CL.WebSite is NULL) THEN '' ELSE concat ('WebSite: ',CL.WebSite,char(10)) END
		, case when (CL.Fax = '' OR CL.Fax is NULL) THEN '' ELSE concat ('Fax: ',CL.Fax,char(10)) END
		, case when (CL.Department = '' OR CL.Department is NULL) THEN '' ELSE concat ('Department: ',CL.Department,char(10)) END
		, case when (CL.Address2 = '' OR CL.Address2 is NULL) THEN '' ELSE concat ('Address2: ',CL.Address2,char(10)) END
		, case when (CL.Address3 = '' OR CL.Address3 is NULL) THEN '' ELSE concat ('Address3: ',CL.Address3,char(10)) END
		, case when (CL.ContactSource = '' OR CL.ContactSource is NULL) THEN '' ELSE concat ('ContactSource: ',CL.ContactSource,char(10)) END
		, case when (CA.PositionWanted = '' OR CA.PositionWanted is NULL) THEN '' ELSE concat ('PositionWanted: ',CA.PositionWanted,char(10)) END
		, case when (CA.SectorWanted = '' OR CA.SectorWanted is NULL) THEN '' ELSE concat ('SectorWanted: ',CA.SectorWanted,char(10)) END
		, case when (CA.ReasonDeclined = '' OR CA.ReasonDeclined is NULL) THEN '' ELSE concat ('ReasonDeclined: ',CA.ReasonDeclined,char(10)) END
		, case when (CL.ContactStatus = '' OR CL.ContactStatus is NULL) THEN '' ELSE concat ('ContactStatus: ',CL.ContactStatus,char(10)) END
		, case when (CL.CandidateRef = '' OR CL.CandidateRef is NULL) THEN '' ELSE concat ('CandidateRef: ',CL.CandidateRef,char(10)) END
		, case when (CA.Currency1 = '' OR CA.Currency1 is NULL) THEN '' ELSE concat ('Currency1: ',CA.Currency1,char(10)) END
		, case when (CL.Segment = '' OR CL.Segment is NULL) THEN '' ELSE concat ('Segment: ',CL.Segment,char(10)) END
		, case when (CL.Sector = '' OR CL.Sector is NULL) THEN '' ELSE concat ('Sector: ',CL.Sector,char(10)) END
		, case when (CA.NoticePeriod = '' OR CA.NoticePeriod is NULL) THEN '' ELSE concat ('NoticePeriod: ',CA.NoticePeriod,char(10)) END
		, case when (CL.RegDate = '' OR CL.RegDate is NULL) THEN '' ELSE concat ('RegDate: ',CL.RegDate,char(10)) END
		, case when (CL.LastUpdate = '' OR CL.LastUpdate is NULL) THEN '' ELSE concat ('LastUpdate: ',CL.LastUpdate,char(10)) END
	  ) as 'candidate-note'

	
	, CL.PostCode as 'candidate-zipCode'
	
	--, CA.Nationality as 'candidate-citizenship'
	, case
		when CL.Country like 'Africa%' then 'ZA'
		when CL.Country like 'Afrika%' then 'ZA'
		when CL.Country like 'Algeri%' then 'DZ'
		when CL.Country like 'Americ%' then 'US'
		when CL.Country like 'Arabic%' then 'SA'
		when CL.Country like 'Asian%' then 'TR'
		when CL.Country like 'Austra%' then 'AU'
		when CL.Country like 'Bahrai%' then 'BH'
		when CL.Country like 'Belgia%' then 'BE'
		when CL.Country like 'Britis%' then 'GB'
		when CL.Country like 'Canadi%' then 'CA'
		when CL.Country like 'Croati%' then 'HR'
		when CL.Country like 'Czech%' then 'CZ'
		when CL.Country like 'Damman%' then 'SA'
		when CL.Country like 'Danish%' then 'DK'
		when CL.Country like 'Dutch%' then 'NL'
		when CL.Country like 'Egypti%' then 'EG'
		when CL.Country like 'egypt%' then 'EG'
		when CL.Country like 'Emirat%' then 'AE'
		when CL.Country like 'Filipi%' then 'PH'
		when CL.Country like 'French%' then 'FR'
		when CL.Country like 'German%' then 'DE'
		when CL.Country like 'Greek%' then 'GR'
		when CL.Country like 'Hungar%' then 'HU'
		when CL.Country like 'Indian%' then 'IN'
		when CL.Country like 'India%' then 'IN'
		when CL.Country like 'irania%' then 'IR'
		when CL.Country like 'Iraqi%' then 'IQ'
		when CL.Country like 'Iraq%' then 'IQ'
		when CL.Country like 'Irish%' then 'IE'
		when CL.Country like 'Italia%' then 'IT'
		when CL.Country like 'Jordan%' then 'JO'
		when CL.Country like 'Lebane%' then 'LB'
		when CL.Country like 'Morocc%' then 'MA'
		when CL.Country like 'Nether%' then 'NL'
		when CL.Country like 'Nigeri%' then 'NG'
		when CL.Country like 'Omani%' then 'OM'
		when CL.Country like 'Pakist%' then 'PK'
		when CL.Country like 'palest%' then 'PS'
		when CL.Country like 'Palest%' then 'PS'
		when CL.Country like 'Philip%' then 'PH'
		when CL.Country like 'Polish%' then 'PL'
		when CL.Country like 'Portug%' then 'PT'
		when CL.Country like 'Qatari%' then 'QA'
		when CL.Country like 'Romani%' then 'RO'
		when CL.Country like 'Russia%' then 'RU'
		when CL.Country like 'Saudi%' then 'SA'
		when CL.Country like 'Scotis%' then 'GB'
		when CL.Country like 'Scotti%' then 'GB'
		when CL.Country like 'Serbia%' then 'RS'
		when CL.Country like 'Singap%' then 'SG'
		when CL.Country like 'Spanis%' then 'ES'
		when CL.Country like 'Sudane%' then 'SD'
		when CL.Country like 'Swedis%' then 'SE'
		when CL.Country like 'Swiss%' then 'CH'
		when CL.Country like 'Syrian%' then 'SY'
		when CL.Country like 'Szekes%' then 'HU'
		when CL.Country like 'Tunisi%' then 'TN'
		when CL.Country like 'Turkis%' then 'TR'
		when CL.Country like 'Urdu%' then 'PK'
		when CL.Country like 'Zealan%' then 'NZ'
		when CL.Country like '%UNITED%ARAB%' then 'AE'
		when CL.Country like '%UAE%' then 'AE'
		when CL.Country like '%U.A.E%' then 'AE'
		when CL.Country like '%UNITED%KINGDOM%' then 'GB'
		when CL.Country like '%UNITED%STATES%' then 'US'
		when CL.Country like '%US%' then 'US'
	end as 'candidate-citizenship'
	
	, CA.FullTimeJob as 'candidate-employmentType'
	, CA.SalaryWanted as 'candidate-desiredSalary'
	
	, case
		when j.PermanentJob is null then 'PERMANENT'
		when j.PermanentJob = '' then  'PERMANENT'
		when j.PermanentJob like '%Contract%' then 'CONTRACT'
		when j.PermanentJob like '%FREELANCE%' then 'TEMPORARY'
		when j.PermanentJob like '%Part Time%' then 'TEMPORARY'
		when j.PermanentJob like '%perm%' then 'PERMANENT'
		when j.PermanentJob like '%Permamaent %' then 'PERMANENT'
		when j.PermanentJob like '%Permamant %' then 'PERMANENT'
		when j.PermanentJob like '%Permanent%' then 'PERMANENT'
		when j.PermanentJob like '%Temporary%' then 'TEMPORARY'
	end as 'candidate-jobTypes'
	--, CA.JobType as 'candidate-jobTypes'
	
	
	, CA.CurrentSalary as 'candidate-currentSalary'
	, case when (skills.skill = '' OR skills.skill is NULL) THEN '' ELSE concat ('Skills: ',replace(skills.skill,'&amp; ',''),char(10)) END as 'candidate-skills'
	, at.Filename as 'candidate-resume'

	, concat(
		  case when (CL.Comments = '' OR CL.Comments is NULL) THEN '' ELSE concat ('Comments: ',CL.Comments,char(10)) END
		, case when (n1.NotesID = '' OR n1.NotesId is NULL) THEN '' ELSE concat ('NotesId: ',n1.NotesId,char(10)) END
		, case when (n.Text = '' OR n.Text is NULL) THEN '' ELSE replace(concat (n.Text,char(10)),'&amp; ','') END
	) as 'contact-comments'
	
/*	
	, CONVERT(VARCHAR(10),C.dateOfBirth,110) as 'candidate-dob'
	, Education.school as 'candidate-schoolName'
	, Education.graduationDate as 'candidate-graduationDate'
	, Education.degree as 'candidate-degreeName'
	, Education.major as '(candidate-major)'
*/
-- with skill as (select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid)
-- select count(*) --90476
-- select top 20 *
from Contacts CL
left join (SELECT contactid, text = STUFF((SELECT char(10) + 'Note: ' + text + char(10) FROM notes b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 0, '') FROM notes a GROUP BY contactid) n on CL.contactid = n.contactid
left join (SELECT contactid, notesid = STUFF((SELECT ',' + notesid + char(10) FROM notes b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 1, '') FROM notes a GROUP BY contactid) n1 on CL.contactid = n1.contactid
left join (SELECT id, filename = STUFF((SELECT DISTINCT ',' + filename from Attachments WHERE id = a.id FOR XML PATH ('')), 1, 1, '') FROM Attachments a GROUP BY id) at on cl.contactid = at.Id
left join (SELECT contactid, skill = STUFF((SELECT skill + char(10) FROM skill b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 0, '') FROM skill a GROUP BY contactid) skills on CL.contactid = skills.contactid
--left join candidates CA on CL.ContactId = CA.ContactId --91379
--left join vacancies j on CL.ContactId = j.ContactId
left join Companies CC on CL.CompanyId = CC.CompanyId
--left join ( select CL.username as name, case when (CL.Email like '%_@_%.__%') THEN CL.Email ELSE '' END as 'email' from Contacts CL where CL.displayname = CL.username and CL.Email <> '' and CL.displayname is not null ) owner on CL.username = owner.name
where CL.type in ('Active','Candidat','Candidate','Freelance','Internal candidate','Placed','Placed Candidate','Prospective candidate','Works for Client')

--and CL.Title is not null or CL.Title != ''
--CL.email is null or CL.email = ''
--and CL.email2 is not null or CL.email2 != ''
--order by CL.email2 desc


/*
left join tmp_country tc ON c.countryID = tc.code
left join bullhorn1.BH_UserContact UC2 on C.userID = UC2.userID
left join tmp_email_3 on C.recruiterUserID = tmp_email_3.recruiterUserID
left join tmp_email_1 on C.candidateID = tmp_email_1.candidateID
--left join (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID) UE_2 on C.userID = UE_2.userID
--left join (select * from [bullhorn1].[BH_UserEducation] where isDeleted = 0) UE on UE.userEducationID = UE_2.userEducationID
left join Education on C.userID = Education.userID
left join t4 on t4.candidateUserID = C.userID
left join tmp_6 on C.userID = tmp_6.candidateUserID
left join tmp_note on C.userID = tmp_note.Userid
left join tmp_addednote AN on C.userID = AN.Userid
--left join (SELECT candidateID, STUFF((SELECT '  Summary: ' + convert(varchar(max),comments) + char(10) from  bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID FOR XML PATH ('')), 1, 2, '')  AS URLList FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID) sum on C.candidateID = sum.candidateID
left join (SELECT candidateID, STUFF((SELECT case when (convert(varchar(max),comments) = '' or comments is null) then '' else char(10) + ' ' + 'Summary: ' end + convert(varchar(max),comments) from  bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID FOR XML PATH ('')), 1, 2, '')  AS URLList FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID) sum on C.candidateID = sum.candidateID
where C.isPrimaryOwner = 1
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
)

--select top 20 [candidate-firstName], [candidate-Lastname], [candidate-notes]
select *
from t1 
--where [candidate-Lastname] like '%Sheary%'
--where [candidate-notes] like '%Summary%' --order by [candidate-firstName]

--inner join tmp_email_2 on t1.[candidate-externalId] = tmp_email_2.candidateID
--order by userID
*/

/* Check if candidate is not primary owner
select userID from bullhorn1.Candidate
where isPrimaryOwner = 1
group by userID having count(*) > 1
*/