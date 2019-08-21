--CANDIDATE DUPLICATE MAIL REGCONITION
with EmailDupRegconition as (SELECT ApplicantId, PrimaryEmailAddress, ROW_NUMBER() OVER(PARTITION BY PrimaryEmailAddress ORDER BY ApplicantId ASC) AS rn 
from VW_APPLICANT_INFO where PrimaryEmailAddress like '%_@_%.__%')

, CandidateEmail as (select ApplicantId
, case	when rn = 1 then PrimaryEmailAddress
		else concat('DUPLICATE',rn,'-',PrimaryEmailAddress) end as CandidateEmail
, rn
from EmailDupRegconition)

--CANDIDATE Personal EMAIL
, TempPersonalEmail as (select a.ApplicantId, p.CommunicationTypeId, p.Num
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 86 and p.Num like '%_@_%.__%')

, CandidatePersonalEmail as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + Num
          from  TempPersonalEmail
          WHERE ApplicantId = tpe.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CanPersonalEmail
FROM TempPersonalEmail as tpe
GROUP BY tpe.ApplicantId)
--------------------------------------CANDIDATE RESUMES
, CVName as (
select ApplicantId,concat('CV',cvc.CVId,
 coalesce('_' + NULLIF(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CV.Description,'?',''),' ',''),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),''),''), FileExtension)
  as CVFullName
from CV left join CVContents cvc on CV.CVId = cvc.CVId)
, CanResumes as (select ApplicantId, STUFF(
					(Select ',' + CVFullName
					from CVName 
					where ApplicantId = cvn.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'CVName'
FROM CVName as cvn
GROUP BY cvn.ApplicantId)

--, PersonalEmail as (SELECT ApplicantId, Num, ROW_NUMBER() OVER(PARTITION BY Num ORDER BY ApplicantId ASC) AS rn 
--from TempPersonalEmail where Num like '%_@_%.__%')
----select ApplicantId, count(ApplicantId) from TempPersonalEmail group by ApplicantId having count(ApplicantId) >1
--, CandidatePersonalEmail as (select ApplicantId
--, case	when rn = 1 then Num
--		else concat('duplicate',rn,'-',Num) end as CandidateEmail
--, rn
--from PersonalEMail)
--Mobile
--with TempMobile as (select a.ApplicantId, p.CommunicationTypeId, p.PhoneId, p.NumTrimmed, ROW_NUMBER() OVER(PARTITION BY a.ApplicantId ORDER BY p.NumTrimmed ASC) AS rn
--from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
--where p.CommunicationTypeId = 83)

, LatestMobile as (select a.ApplicantId, max(p.PhoneId) as MaxMobileId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 83
group by a.ApplicantId)

, Mobile as (select lm.ApplicantId, left(replace(p.Num,' ',''),30) as Mobile
 from LatestMobile lm left join Phones p on lm.MaxMobileId = p.PhoneId)
 -----------------------CANDIDATE ALL MOBILE
 , AllMobile1 as (select a.ApplicantId, p.Num 
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 83)

, AllMobile as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + Num
          from  AllMobile1
          WHERE ApplicantId = am1.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'canMobile'
FROM AllMobile1 as am1
GROUP BY am1.ApplicantId)
-----------------------------------------

--PrimaryPhone
, LatestPhone as (select a.ApplicantId, max(p.PhoneId) as MaxPhoneId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 79
group by a.ApplicantId)

, Phone as (select lm.ApplicantId, left(replace(p.Num,' ',''),30) as Num
 from LatestPhone lm left join Phones p on lm.MaxPhoneId = p.PhoneId)

 -----------------------------------------All PRIMARY PHONES
 , AllPhone1 as (select a.ApplicantId, p.Num 
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 79)

, AllPhone as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + Num
          from  AllPhone1
          WHERE ApplicantId = ap1.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'canPhone'
FROM AllPhone1 as ap1
GROUP BY ap1.ApplicantId)

----------------------------------------------------------------------------------------------LINKEDIN PROFILE type 89: URL 
, LatestLinkedin89 as (select a.ApplicantId, max(p.PhoneId) as MaxPhoneId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 89 and p.Num like '%linkedin%'
group by a.ApplicantId)

, Linkedin89 as (select li89.ApplicantId, p.Num as LinkedIn89
 from LatestLinkedin89 li89 left join Phones p on li89.MaxPhoneId = p.PhoneId)

 ------------------------------------------LINKEDIN PROFILE type 90: social networking 
, LatestLinkedin90 as (select a.ApplicantId, max(p.PhoneId) as MaxPhoneId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 90 and p.Num like '%linkedin%'
group by a.ApplicantId)

, Linkedin90 as (select li90.ApplicantId, p.Num as LinkedIn90
 from LatestLinkedin90 li90 left join Phones p on li90.MaxPhoneId = p.PhoneId)
 ----------------------------------------GET LINKEDIN BY COMBINE 2 TYPES, PREFER TO TYPE 89
 , LinkedIn as (select a.ApplicantId, coalesce(l89.LinkedIn89,l90.linkedin90) as CanLinkedin
  from Applicants a left join Linkedin89 l89 on a.ApplicantId = l89.ApplicantId
  left join Linkedin90 l90  on a.ApplicantId = l90.ApplicantId)
--	where l89.LinkedIn89 is not null or l90.linkedin90 is not null)

-----------------------------------------------------------------------------------------------------------All linkedin: add to note
, TempLinkedIn as (select a.ApplicantId, p.CommunicationTypeId, p.Num
	from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
	where (p.CommunicationTypeId = 89 or p.CommunicationTypeId = 90) and p.Num like '%linkedin%')

, AllLinkedIn as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + Num
          from  TempLinkedIn
          WHERE ApplicantId = tli.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'canallLinkedin'
FROM TempLinkedIn as tli
GROUP BY tli.ApplicantId)

--------------------------------------------Current Company: Get the latest company of the candidate
, TempCurrentEmployer as (select a.ApplicantId, max(w.WorkHistoryId) as CurrentWorkId
from Applicants a
left join WorkHistory w on a.ApplicantId = w.ApplicantID
group by a.ApplicantId)
, CurrentEmployer as (select tce.ApplicantId, w.WorkHistoryId, w.Company, w.Description JobTitle, w.FromDate, w.ToDate
 from TempCurrentEmployer tce left join WorkHistory w on tce.CurrentWorkId = w.WorkHistoryId)

-----Candidate's Comment
, tempComment as (select aa.ApplicantActionId, aag.ApplicantId, aag.StatusDescription, aa.Notes, aag.CVSentDate, aag.ApplicantFileAs, aag.ClientFileAs, aag.ClientContactFileAs,
 aag.JobRefNo, aag.JobTitle, aag.ConsultantUsername, aag.StatusDate, aag.CreatedUserName, aag.CreatedOn, CV.CVRefNo, aa.JobId, j.StartDate, et.Description
from ApplicantActions aa left join VW_APPLICANT_ACTION_GRID aag on aa.ApplicantActionId = aag.ApplicantActionId
							left join CV on aa.CVId = CV.CVId
							left join Jobs j on aa.JobId = j.JobId
							left join EmploymentTypes et on j.EmploymentTypeId = et.EmploymentTypeId)
, CanComment as (SELECT
     ApplicantId,
     STUFF(
         (SELECT '<hr>' + 'Created date: ' + convert(varchar(20),CreatedOn,120) + char(10) + 'Created by: ' + CreatedUserName + char(10)
		  + coalesce('Relates to job: ' + JobTitle + char(10), '') + coalesce('Job Ref No.' + JobRefNo + char(10), '')
		  + coalesce('Employment type: ' + Description + char(10), '') + coalesce('Job Start date: ' + convert(varchar(20),StartDate,120) + char(10), '')
		  + coalesce('Relates to contact: ' + ClientContactFileAs + char(10), '') + coalesce('Relates to company: ' + ClientFileAs + char(10), '')
		  + coalesce('CV Ref No. ' + CVRefNo + char(10), '') + coalesce('CV Sent date: ' + convert(varchar(20),CVSentDate,120) + char(10), '')
		  + coalesce('Status: ' + StatusDescription + char(10), '') + coalesce('Status date:' + convert(varchar(20),StatusDate,120) + char(10), '')
		  + coalesce('Consultant: ' + Consultantusername + char(10), '') + iif(Notes = '' or Notes is null,'',concat('Notes: ',char(10),Notes))
          from  tempComment
          WHERE ApplicantId = tcmt.ApplicantId
		  order by CreatedOn desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS AllComment
FROM tempComment as tcmt
GROUP BY tcmt.ApplicantId)
--select * from CanComment
----CANDIDATE ADDRESS: get the latest address
, maxadd as (select a.ApplicantId, max(Ad.AddressId) as maxAddressId
from Applicants a
left join Address Ad on a.ApplicantId = Ad.ObjectId
group by a.ApplicantId)
----select * from minadd-----
, tempAddress as (
	select ma.ApplicantId, Ad.AddressId, Ad.Building, Ad.Street, Ad.District, Ad.City, Ad.PostCode
						, Ad.CountyValueId, lv.ValueName as County, Ad.CountryValueId, lv1.ValueName as Country
	from maxadd as ma left join Address as Ad on Ad.AddressId = ma.maxAddressId
					left join ListValues lv on ad.CountyValueId = lv.ListValueId
					left join ListValues lv1 on ad.CountryValueId = lv1.ListValueId)
--select * from tempAddress----
, CanAddress as (
	select ta.ApplicantId,
	ltrim(rtrim(concat(iif(ta.Building = '' or ta.Building is NULL,'',concat(ta.Building,', '))
	, iif(ta.Street = '' or ta.Street is NULL,'',concat(ta.Street,', '))
	, iif(ta.District = '' or ta.District is NULL,'',concat(ta.District,', '))
	, iif(ta.City = '' or ta.City is NULL,'',concat(ta.City,', '))
	, iif(ta.County = '' or ta.County is NULL,'',concat(ta.County,', '))
	, iif(ta.Country = '' or ta.Country is NULL,'',concat(ta.Country,', '))
	, iif(ta.Postcode = '' or ta.Postcode is NULL,'',ta.Postcode)))) as 'canaddress'
	from tempAddress ta)
----------Candidate all address
--, tempAllAddress as (select a.ApplicantId, Ad.AddressId, Ad.Building, Ad.Street, Ad.District, Ad.City, Ad.PostCode
--from Applicants as a left join Address as Ad on a.ApplicantId = Ad.ObjectId)

--, tempAllAddress1 as (
--	select taa.ApplicantId,
--	ltrim(rtrim(concat(iif(taa.Building = '' or taa.Building is NULL,'',concat(taa.Building,', '))
--	, iif(taa.Street = '' or taa.Street is NULL,'',concat(taa.Street,', '))
--	, iif(taa.District = '' or taa.District is NULL,'',concat(taa.District,', '))
--	, iif(taa.City = '' or taa.City is NULL,'',concat(taa.City,', '))
--	, iif(taa.Postcode = '' or taa.Postcode is NULL,'',taa.Postcode)))) as 'canalladdress'
--	from tempAllAddress taa)
, tempAllAddress1 as (select a.ApplicantId,
	 ltrim(Stuff(
			  Coalesce(' ' + NULLIF(ad.Building, ''), '')
			+ Coalesce(', ' + NULLIF(ad.Street, ''), '')
			+ Coalesce(', ' + NULLIF(ad.District, ''), '')
			+ Coalesce(', ' + NULLIF(ad.City, ''), '')
			+ Coalesce(', ' + NULLIF(lv.Description, ''), '')
			+ Coalesce(', ' + NULLIF(lv1.Description, ''), '')
			+ Coalesce(', ' + NULLIF(ad.Postcode, ''), '')
			, 1, 1, '') ) as 'canalladdress'
from Applicants a left join Address ad on a.ApplicantId = Ad.ObjectId
					left join ListValues lv on ad.CountyValueId = lv.ListValueId
					left join ListValues lv1 on ad.CountryValueId = lv1.ListValueId)
, AllAdress as (SELECT ApplicantId, 
     STUFF(
         (SELECT ';' + canalladdress
          from  tempAllAddress1
          WHERE ApplicantId = taa1.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'canalladdress'
FROM tempAllAddress1 as taa1
GROUP BY taa1.ApplicantId)
---------------------------------------------------Candidate Title
, CanTitle as (select ApplicantId, lv.ValueName,
case 
	when lv.ValueName = 'Mrs' then 'MRS'
	when lv.ValueName = 'Miss' then 'MISS'
	when lv.ValueName = 'Mr' then 'MR'
	when lv.ValueName = 'Dr' then 'DR'
	when lv.ValueName = 'Ms' then 'MS'
	else '' end as 'canTitle'
 from Applicants a left join Person p on a.ApplicantId = p.PerSonid
left Join ListValues lv on p.TitleValueId = lv.ListValueId)

--------------------------------------------------Candidate's Attribute Master and Attribute
, temp_CanAttributeMaster as(
select oa.ObjectID, can.ApplicantId, oa.ObjectAttributeId, am.Description as AttributeMaster, a.Description, a.Notes,
	oa.AttributeId, iif(a.Notes = a.Description or a.Notes = '' or a.Notes is NULL,a.Description,concat(a.Description,' (',a.Notes,')')) as Attribute,
	ROW_NUMBER() OVER(PARTITION BY can.ApplicantId ORDER BY am.Description ASC) AS rn 
from ObjectAttributes oa left join Attributes a on oa.AttributeId = a.AttributeId
left join Applicants can on oa.ObjectID = can.ApplicantId
left join AttributeMaster am on a.AttributeMasterId = am.AttributeMasterId
where can.ApplicantId is not null)-- and a.AttributeMasterId is not null
--oa.ObjectID in (select ApplicantId from Applicants)
, CanAttributeMaster as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + AttributeMaster
          from  temp_CanAttributeMaster
          WHERE ApplicantId = tcam.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'AttributeMaster'
FROM temp_CanAttributeMaster as tcam
GROUP BY tcam.ApplicantId)

, CanAttribute as (SELECT ApplicantId, 
     STUFF(
         (SELECT '; ' + Attribute
          from  temp_CanAttributeMaster
          WHERE ApplicantId = tcam.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'Attribute'
FROM temp_CanAttributeMaster as tcam
GROUP BY tcam.ApplicantId)

-------------------------------------------------------------MAIN SCRIPT
select concat('MP', a.ApplicantId) as 'candidate-externalId'
, iif(rtrim(ltrim(p.PersonName)) = '' or rtrim(ltrim(p.PersonName)) is null, concat('NoFirstname-', a.ApplicantId), rtrim(ltrim(p.PersonName))) as 'candidate-firstName'
, iif(rtrim(ltrim(p.SurName)) = '' or rtrim(ltrim(p.SurName)) is null, concat('NoLastname-', a.ApplicantId), rtrim(ltrim(p.SurName))) as 'candidate-Lastname'
, case
	when ai.PrimaryEmailAddress is not NULL and ai.ApplicantId in (select ApplicantId from CandidateEmail) then ce.CandidateEmail
	else concat('CandidateID-',a.ApplicantId,'@noemail.com') end as 'candidate-email'
--, cpe.CanPersonalEmail as 'candidate-workEmail'
, case
	when lv.ValueName = 'Male' then 'MALE'
	when lv.ValueName = 'Female' then 'FEMALE'
	else '' end as 'candidate-gender'
, CanTitle.canTitle as 'candidate-title'
, m.Mobile as 'candidate-mobile'
, coalesce(pp.Num,m.Mobile) as 'candidate-phone'
, li.CanLinkedin as 'candidate-linkedin'
, iif(right(cadd.canaddress,1)= ',',left(replace(cadd.canaddress,',,',','),len(cadd.canaddress)-1),replace(cadd.canaddress,',,',',')) as 'candidate-address'
, tadd.City as 'candidate-city'
, case
	when tadd.Country like 'Afghan%' then 'AF'
	when tadd.Country like '%Africa%' then 'ZA'
	when tadd.Country like '%Aberdeen%' then 'GB'
	when tadd.Country like '%Abu%' then 'AE'
	when tadd.Country like 'Albani%' then 'AL'
	when tadd.Country like '%Americ%' then 'US'
	when tadd.Country like 'Andorr%' then 'AD'
	when tadd.Country like 'Algeria%' then 'DZ'
	when tadd.Country like 'Argentina%' then 'AR'
	when tadd.Country like 'Armenia%' then 'AM'
	when tadd.Country like 'Austra%' then 'AU'
	when tadd.Country like 'Austri%' then 'AT'
	when tadd.Country like 'Azerbaijan%' then 'AZ'
	when tadd.Country like 'Belarus%' then 'BY'
	when tadd.Country like 'Belgiu%' then 'BE'
	when tadd.Country like 'Brazil%' then 'BR'
	when tadd.Country like 'Britis%' then 'GB'
	when tadd.Country like 'Brunei%' then 'BN'
	when tadd.Country like 'Bucha%' then 'RO'
	when tadd.Country like 'Bulgari%' then 'BG'
	when tadd.Country like 'Burmes%' then 'MM'
	when tadd.Country like 'Cambod%' then 'KH'
	when tadd.Country like 'Cameroon%' then 'CM'
	when tadd.Country like 'Canad%' then 'CA'
	when tadd.Country like 'Chines%' then 'CN'
	when tadd.Country like 'China%' then 'CN'
	when tadd.Country like 'Colombi%' then 'CO'
	when tadd.Country like 'Columbi%' then 'CO'
	when tadd.Country like 'Congo%' then 'CG'
	when tadd.Country like 'Copenhagen%' then 'DK'
	when tadd.Country like 'Costa%' then 'CR'
	when tadd.Country like 'Croatia%' then 'HR'
	when tadd.Country like 'Cypr%' then 'CY'
	when tadd.Country like 'Czech%' then 'CZ'
	when tadd.Country like 'Danish%' then 'DK'
	when tadd.Country like 'Denmark%' then 'DK'
	when tadd.Country like 'Dubai%' then 'AE'
	when tadd.Country like 'Dutch%' then 'NL'
	when tadd.Country like 'East%' then 'ZA'
	when tadd.Country like 'Egypt%' then 'EG'
	when tadd.Country like 'Emiria%' then 'AE'
	when tadd.Country like 'Eritre%' then 'ER'
	when tadd.Country like 'Estoni%' then 'EE'
	when tadd.Country like 'Ethiop%' then 'ET'
	when tadd.Country like 'Europe%' then 'TR'
	when tadd.Country like 'Fijian%' then 'FJ'
	when tadd.Country like 'Filipi%' then 'PH'
	when tadd.Country like 'fili%' then 'PH'
	when tadd.Country like 'Finnish%' then 'FI'
	when tadd.Country like 'Finland%' then 'FI'
	when tadd.Country like 'Flemish%' then 'BE'
	when tadd.Country like 'French%' then 'FR'
	when tadd.Country like 'France%' then 'FR'
	when tadd.Country like 'Gabone%' then 'GA'
	when tadd.Country like 'German%' then 'DE'
	when tadd.Country like 'Georgia%' then 'GE'
	when tadd.Country like 'Ghanai%' then 'GH'
	when tadd.Country like 'Gree%' then 'GR'
	when tadd.Country like 'Hunga%' then 'HU'
	when tadd.Country like 'Iceland%' then 'IS'
	when tadd.Country like 'India%' then 'IN'
	when tadd.Country like 'Indone%' then 'ID'
	when tadd.Country like 'Irania%' then 'IR'
	when tadd.Country like 'Irish%' then 'IE'
	when tadd.Country like 'Ireland%' then 'IE'
	when tadd.Country like 'Isra%' then 'IL'
	when tadd.Country like 'Ital%' then 'IT'
	when tadd.Country like 'Jamaic%' then 'JM'
	when tadd.Country like 'Japane%' then 'JP'
	when tadd.Country like 'Jersey%' then 'JE'
	when tadd.Country like 'Kazakhstan%' then 'KZ'
	when tadd.Country like 'Keny%' then 'KE'
	when tadd.Country like 'Latvia%' then 'LV'
	when tadd.Country like 'Leban%' then 'LB'
	when tadd.Country like 'Lithua%' then 'LT'
	when tadd.Country like 'Libya%' then 'LY'
	when tadd.Country like 'Luxembourg%' then 'LU'
	when tadd.Country like 'Macedonia%' then 'MK'
	when tadd.Country like 'Malaga%' then 'MG'
	when tadd.Country like 'Malays%' then 'MY'
	when tadd.Country like 'Malt%' then 'MT'
	when tadd.Country like 'Mauritia%' then 'MU'
	when tadd.Country like 'Mexi%' then 'MX'
	when tadd.Country like 'Moldova%' then 'MD'
	when tadd.Country like 'Monaco%' then 'MC'
	when tadd.Country like 'Namibi%' then 'NA'
	when tadd.Country like 'Netherland%' then 'NL'
	when tadd.Country like 'New Zea%' then 'NZ'
	when tadd.Country like 'Nigeri%' then 'NG'
	when tadd.Country like 'Northern Irish' then 'IE'
	when tadd.Country like 'Norwa%' then 'NO'
	when tadd.Country like 'Oman%' then 'OM'
	when tadd.Country like 'Pakist%' then 'PK'
	when tadd.Country like 'Philip%' then 'PH'
	when tadd.Country like 'Phili%' then 'PH'
	when tadd.Country like 'Polish%' then 'PL'
	when tadd.Country like 'Poland%' then 'PL'
	when tadd.Country like 'Portu%' then 'PT'
	when tadd.Country like 'Qatar%' then 'QA'
	when tadd.Country like 'Romania%' then 'RO'
	when tadd.Country like 'Russia%' then 'RU'
	when tadd.Country like 'Serbia%' then 'RS'
	when tadd.Country like 'Saudi%' then 'SA'
	when tadd.Country like 'Scotland%' then 'GB'
	when tadd.Country like 'Slovakia%' then 'SK'
	when tadd.Country like 'Singap%' then 'SG'
	when tadd.Country like 'Sri%' then 'LK'
	when tadd.Country like 'South Africa%' then 'ZA'
	when tadd.Country like 'Spanish%' then 'ES'
	when tadd.Country like 'Spain%' then 'ES'
	when tadd.Country like 'Sri Lankan%' then 'LK'
	when tadd.Country like 'Sri lankan%' then 'LK'
	when tadd.Country like 'Sudan%' then 'SD'
	when tadd.Country like 'Swedish%' then 'SE'
	when tadd.Country like 'Sweden%' then 'SE'
	when tadd.Country like 'Swiss%' then 'CH'
	when tadd.Country like 'Switzerland%' then 'CH'
	when tadd.Country like 'Switverland%' then 'CH'
	when tadd.Country like 'Syria%' then 'SY'
	when tadd.Country like 'Taiwan%' then 'TW'
	when tadd.Country like 'Thai%' then 'TH'
	when tadd.Country like 'Trinida%' then 'TT'
	when tadd.Country like 'Turkey%' then 'TR'
	when tadd.Country like 'Turkmenistan%' then 'TM'
	when tadd.Country like 'Tunisia%' then 'TN'
	when tadd.Country like 'Vietna%' then 'VN'
	--when tadd.Country like 'Yugoslavia%' then 'YU'
	when tadd.Country like '%UNITED%ARAB%' then 'AE'
	when tadd.Country like '%UAE%' then 'AE'
	when tadd.Country like '%U.A.E%' then 'AE'
	when tadd.Country like '%UNITED%KINGDOM%' then 'GB'
	when tadd.Country like '%UK%' then 'GB'
	when tadd.Country like '%Ukrain%' then 'UA'
	when tadd.Country like '%UNITED%STATES%' then 'US'
	when tadd.Country like '%US%' then 'US'
	when tadd.Country like '%Venezu%' then 'VE'
	when tadd.Country like '%Vietnam%' then 'VN'
	when tadd.Country like '%Wales%' then 'GB'
else '' end as 'candidate-Country'
, tadd.PostCode as 'candidate-zipCode'
, convert(varchar(10),p.dob,120) as 'candidate-dob'
--, agv.Location as 'candidate-address'
, p.NationalityId
, case
	when n.Nationality like 'Austra%' then 'AU'
	when n.Nationality like 'Austri%' then 'AT'
	when n.Nationality like 'Belgia%' then 'BE'
	when n.Nationality like 'Brazil%' then 'BR'
	when n.Nationality like 'Britis%' then 'GB'
	when n.Nationality like 'Bucha%' then 'RO'
	when n.Nationality like 'Burmes%' then 'MM'
	when n.Nationality like 'Cambod%' then 'KH'
	when n.Nationality like 'Canadi%' then 'CA'
	when n.Nationality like 'Chines%' then 'CN'
	when n.Nationality like 'Colombi%' then 'CO'
	when n.Nationality like 'Costa%' then 'CR'
	when n.Nationality like 'Cypr%' then 'CY'
	when n.Nationality like 'Czech%' then 'CZ'
	when n.Nationality like 'Danish%' then 'DK'
	when n.Nationality like 'Denmark%' then 'DK'
	when n.Nationality like 'Dutch%' then 'NL'
	when n.Nationality like 'East%' then 'ZA'
	when n.Nationality like 'Emiria%' then 'AE'
	when n.Nationality like 'Eritre%' then 'ER'
	when n.Nationality like 'Estoni%' then 'EE'
	when n.Nationality like 'Ethiop%' then 'ET'
	when n.Nationality like 'Europe%' then 'TR'
	when n.Nationality like 'Fijian%' then 'FJ'
	when n.Nationality like 'Filipi%' then 'PH'
	when n.Nationality like 'fili%' then 'PH'
	when n.Nationality like 'Finnish%' then 'FI'
	when n.Nationality like 'Flemish%' then 'BE'
	when n.Nationality like 'French%' then 'FR'
	when n.Nationality like 'Gabone%' then 'GA'
	when n.Nationality like 'German%' then 'DE'
	when n.Nationality like 'Ghanai%' then 'GH'
	when n.Nationality like 'Gree%' then 'GR'
	when n.Nationality like 'Hunga%' then 'HU'
	when n.Nationality like 'Indian%' then 'IN'
	when n.Nationality like 'Indone%' then 'ID'
	when n.Nationality like 'Irania%' then 'IR'
	when n.Nationality like 'Irish%' then 'IE'
	when n.Nationality like 'Isra%' then 'IL'
	when n.Nationality like 'Ital%' then 'IT'
	when n.Nationality like 'Jamaic%' then 'JM'
	when n.Nationality like 'Japane%' then 'JP'
	when n.Nationality like 'Keny%' then 'KE'
	when n.Nationality like 'Leban%' then 'LB'
	when n.Nationality like 'Lithua%' then 'LT'
	when n.Nationality like 'Malaga%' then 'MG'
	when n.Nationality like 'Malays%' then 'MY'
	when n.Nationality like 'Malt%' then 'MT'
	when n.Nationality like 'Mauritian%' then 'MU'
	when n.Nationality like 'Mexi%' then 'MX'
	when n.Nationality like 'Namibi%' then 'NA'
	when n.Nationality like 'New Zea%' then 'NZ'
	when n.Nationality like 'Nigeri%' then 'NG'
	when n.Nationality like 'Northern Irish' then 'IE'
	when n.Nationality like 'Norwe%' then 'NO'
	when n.Nationality like 'Pakist%' then 'PK'
	when n.Nationality like 'Philip%' then 'PH'
	when n.Nationality like 'Phili%' then 'PH'
	--when n.Nationality like 'Yugoslavia%' then 'YU'
	when n.Nationality like '%UNITED%ARAB%' then 'AE'
	when n.Nationality like '%UAE%' then 'AE'
	when n.Nationality like '%U.A.E%' then 'AE'
	when n.Nationality like '%UNITED%KINGDOM%' then 'GB'
	when n.Nationality like '%UNITED%STATES%' then 'US'
	when n.Nationality like '%US%' then 'US'
else '' end as 'candidate-citizenship'
, cel.Company as 'candidate-employer1'
, case
	 when cel.JobTitle is null then a.JobTitle
	 when cel.JobTitle = '' then a.JobTitle
	 else cel.JobTitle end as 'candidate-jobTitle1'
, convert(varchar(10),cel.FromDate, 120) as 'candidate-startDate1'
, convert(varchar(10),cel.ToDate, 120) as 'candidate-endDate1'
, th.WorkHistory as 'candidate-workHistory'
, case 
when a.EmploymentTypeId = 6 then 'CONTRACT'
when a.EmploymentTypeId = 5 then 'TEMPORARY'
else 'PERMANENT' end as 'candidate-jobtype'
, a.CurrentBasic as 'candidate-CurrentSalary'
, a.Rate as 'candidate-ContractRate'
, cr.CVName as 'candidate-resume'
, case 
when a.RateUnit = 80 then 'HOURS'
when a.RateUnit = 81 then 'DAYS'
when a.RateUnit = 82 then 'WEEKS'
when a.RateUnit = 83 then 'MONTHS'
when a.RateUnit = 84 then 'YEARS'
else '' end as 'candidate-contractInterval'
, case
	when a.CurrencyId = 10 then 'GBP'
	when a.CurrencyId = 11 then 'USD'
	when a.CurrencyId = 12 then 'EUR'
	else '' end as 'candidate-currency'
, left(cc.AllComment,32000) as 'candidate-comments'
, left(concat('Candidate External ID: MP',a.ApplicantId
	,iif(p.Salutation = '' or p.Salutation is NULL,'',concat(char(10), char(10),'Salutation: ',p.Salutation))
	,iif(a.JobTitle = '' or a.JobTitle is NULL,'',concat(char(10), char(10),'Job Title: ',a.JobTitle))
	,iif(cpe.CanPersonalEmail = '' or cpe.CanPersonalEmail is NULL,'',concat(char(10), char(10),'Personal Email(s): ',cpe.CanPersonalEmail))
	,iif(allp.canPhone = '' or allp.canPhone is NULL,'',concat(char(10), char(10),'Primary Phone(s): ',allp.canPhone))
	,iif(allm.canMobile = '' or allm.canMobile is NULL,'',concat(char(10), char(10),'Mobile(s): ', allm.canMobile))
	,iif(ali.canAllLinkedIn = '' or ali.canAllLinkedIn is NULL,'',concat(char(10), char(10),'All Linkedin URL(s): ', ali.canAllLinkedIn))
	,iif(agv.Location = '' or agv.Location is NULL,'',concat(char(10), char(10),'Location: ',agv.Location))
	,iif(aad.canalladdress = '' or aad.canalladdress is NULL,'',concat(char(10), char(10),'All Address(es): ',replace(aad.canalladdress,',,',',')))
	,iif(agv.EmploymentType = '' or agv.EmploymentType is NULL,'',concat(char(10), char(10),'Employment Type(s): ',left(agv.EmploymentType,len(agv.EmploymentType)-1)))
	,iif(agv.Status = '' or agv.Status is NULL,'',concat(char(10), char(10),'Status: ',agv.Status))
	,iif(agv.StatusDate = '' or agv.StatusDate is NULL,'',concat(char(10), char(10),'Status Date: ',agv.StatusDate))
	,iif(a.CreatedOn = '' or a.CreatedOn is NULL,'',concat(char(10), char(10),'Start/Reg date: ',a.CreatedOn))
	,iif(agv.Source = '' or agv.Source is NULL,'',concat(char(10), char(10),'Source: ',agv.Source))
	,iif(cattm.AttributeMaster = '' or cattm.AttributeMaster is NULL,'',concat(char(10), char(10),'Attribute Master(s): ',cattm.AttributeMaster))
	,iif(catt.Attribute = '' or catt.Attribute is NULL,'',concat(char(10), char(10),'Attribute(s): ',catt.Attribute))
	,iif(agv.[Notice Period] = '' or agv.[Notice Period] is NULL,'',concat(char(10), char(10),'Notice Period: ',agv.[Notice Period]))
	,iif(a.Assessedby is NULL,'',concat(char(10), char(10),'Assessed by: ', u.UserName,' ', u.SurName,', Job Title: ', u.JobTitle))
	,iif(a.AssessmentDate = '' or a.AssessmentDate is NULL,'',concat(char(10), char(10),'Assessment date: ', a.AssessmentDate))
	,iif(a.MinBasic is NULL,'',concat(char(10), char(10),'Min Basic: ', a.MinBasic))
	,iif(a.CurrentBasic is NULL,'',concat(char(10), char(10),'Current Basic: ', a.CurrentBasic))
	,iif(a.MinPackage is NULL,'',concat(char(10), char(10),'Min Package: ', a.MinPackage))
	,iif(a.CurrentPackage is NULL,'',concat(char(10), char(10),'Current Package: ', a.CurrentPackage))
	,iif(a.Rate is NULL,'',concat(char(10), char(10),'Rate: ', a.Rate))
	,iif(cvi.CVInfo = '' or cvi.CVInfo is NULL,'',concat(char(10), char(10),'CV Information: ',char(10),cvi.CVInfo))
	,iif(p.Notes = '' or p.Notes is NULL,'',concat(char(10), char(10),'Notes: ',p.Notes))
	,iif(tcr.ResumeText = '' or tcr.ResumeText is NULL,'',concat(char(10), char(10), 'Resume: ' + tcr.ResumeText, ''))),32000) as 'candidate-note'

from Applicants a 
	left join Person p on a.ApplicantId = p.PersonID
	left join ListValues lv on lv.ListValueId = p.GenderValueId
	left join VW_APPLICANT_INFO ai on a.ApplicantId = ai.ApplicantId
	left join VW_APPLICANT_GRID_VIEW agv on a.ApplicantId = agv.ApplicantId
	left join CandidateEmail ce on a.ApplicantId = ce.ApplicantId
	left join CandidatePersonalEmail cpe on a.ApplicantId = cpe.ApplicantId
	left join Mobile m on a.ApplicantId = m.ApplicantId
	left join Phone pp on a.ApplicantId = pp.ApplicantId
    left join Nationality n on p.NationalityId = n.NationalityId
	left join Users u on a.AssessedBy = u.UserId
	left join Morpheus_CVInfo_production cvi on a.ApplicantId = cvi.ApplicantId
	left join CurrentEmployer cel on a.ApplicantId = cel.ApplicantId
	left join Temp_WorkHistory_Production th on a.ApplicantId = th.ApplicantId
	left join CanComment cc on a.ApplicantId = cc.ApplicantId
	left join CanAddress cadd on a.ApplicantId = cadd.ApplicantId
	left join tempAddress tadd on a.ApplicantId = tadd.ApplicantId
	left join AllAdress aad on a.ApplicantId = aad.ApplicantId
	left join Temp_CandidateResume tcr on a.ApplicantId = tcr.ApplicantId
	left join CanResumes cr on a.ApplicantId = cr.ApplicantId
	left join AllPhone allp on a.ApplicantId = allp.ApplicantId
	left join AllMobile allm on a.ApplicantId = allm.ApplicantId
	left join LinkedIn li on a.ApplicantId = li.ApplicantId
	left join AllLinkedIn ali on a.ApplicantId = ali.ApplicantId
	left join CanTitle on a.ApplicantId = CanTitle.ApplicantId
	left join CanAttribute catt on a.ApplicantId = catt.ApplicantId
	left join CanAttributeMaster cattm on a.ApplicantId = cattm.ApplicantId

order by a.ApplicantId
--select * from Applicants where PrimaryEmailAddressPhoneId is not null
--select a.ApplicantId, a.PrimaryEmailAddressPhoneId, p.Num
--from Applicants a left join Phones p on a.PrimaryEmailAddressPhoneId = p.PhoneId
--where PrimaryEmailAddressPhoneId is not null
--order by a.ApplicantId
--select * from Applicants where LocationId is not null
--select ApplicantId, Location from VW_APPLICANT_GRID_VIEW where Location is not null

--select a.ApplicantId, a.LocationId, lcn.Description
--from Applicants a left join Locations lcn on a.LocationId = lcn.LocationId
--where lcn.Description is not null
