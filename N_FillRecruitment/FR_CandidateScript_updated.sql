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
----Candidate Resumes
, CVName as (select ApplicantId,
 concat('CV',cvc.CVId,
 coalesce('_' + NULLIF(replace(replace(replace(replace(replace(replace(replace(CV.Description,'?',''),' ',''),'/','.'),'''',''),',','_'),'*','~'),'"','^'),''),''), FileExtension)
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
--------------Candidate Stored Document
----temp Candidate Stored Documents
, tempCanstoredDoc as(select ObjectId,a.ApplicantId, t.TemplateId,
 concat('StoredDoc',concat(t.TemplateId,'_'),
 replace(replace(tt.TemplateTypeName,'?',''),' ',''),
 coalesce('_' + NULLIF(replace(replace(replace(replace(replace(replace(replace(t.TemplateName,'?',''),' ',''),'/','.'),'''',''),',','_'),'*','~'),'"','^'), replace(replace(tt.TemplateTypeName,'?',''),' ','')),''),
 td.FileExtension) as StoredDocName
--concat('StoredDoc',concat(tpl.TemplateId,'_'),TemplateName,Coalesce('_' + NULLIF(Description, ''), ''),FileExtension) as StoredDocName,tpl.TemplateId
 from templateDocument td left join Templates t on td.TemplateId = t.TemplateId
	left join TemplateTypes tt on t.TemplateTypeId = tt.TemplateTypeId
	left join Applicants a on t.ObjectId = a.ApplicantId
 where a.ApplicantId is not null)
--select * from tempCanstoredDoc
-----Stored Document
, StoredDoc as (select ApplicantId, STUFF(
					(Select ',' + StoredDocName
					from tempCanstoredDoc 
					where ApplicantId = tcd.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'StoredDocName'
FROM tempCanstoredDoc as tcd
GROUP BY tcd.ApplicantId)
--select * from StoredDoc order by ApplicantId
----------------------------------Get all documents and resume by combining Stored doc and resumes
----join resume and stored doc to a table
, tempCanDocuments as (select * from CanResumes union all select * from StoredDoc)
--select * from tempCan where ApplicantId = 142
, CanDocuments as (select ApplicantId, STUFF(
					(Select ',' + CVName
					from tempCanDocuments 
					where ApplicantId = tcd.ApplicantId
    order by ApplicantId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'CanDocs'
FROM tempCanDocuments as tcd
GROUP BY tcd.ApplicantId)
--select * from CanDocuments
--, PersonalEmail as (SELECT ApplicantId, Num, ROW_NUMBER() OVER(PARTITION BY Num ORDER BY ApplicantId ASC) AS rn 
--from TempPersonalEmail where Num like '%_@_%.__%')
----select ApplicantId, count(ApplicantId) from TempPersonalEmail group by ApplicantId having count(ApplicantId) >1
--, CandidatePersonalEmail as (select ApplicantId
--, case	when rn = 1 then Num
--		else concat('duplicate',rn,'-',Num) end as CandidateEmail
--, rn
--from PersonalEMail)
--Mobile
--with TempMobile as (select a.ApplicantId, p.CommunicationTypeId, p.PhoneId, p.Num, ROW_NUMBER() OVER(PARTITION BY a.ApplicantId ORDER BY p.Num ASC) AS rn
--from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
--where p.CommunicationTypeId = 83)

--Mobile
, LatestMobile as (select a.ApplicantId, max(p.PhoneId) as MaxMobileId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 83
group by a.ApplicantId)

, Mobile as (select lm.ApplicantId, replace(p.Num,' ','') as Mobile
from LatestMobile lm left join Phones p on lm.MaxMobileId = p.PhoneId)
--select * from Mobile
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

-------------------------------------------PrimaryPhone
, LatestPhone as (select a.ApplicantId, max(p.PhoneId) as MaxPhoneId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 79
group by a.ApplicantId)

, Phone as (select lm.ApplicantId, replace(p.Num,' ','') as Num
from LatestPhone lm left join Phones p on lm.MaxPhoneId = p.PhoneId)
--, Phone as (select a.ApplicantId, p.CommunicationTypeId, p.Num
--from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
--where p.CommunicationTypeId = 79)
--Current Company: Get the latest company of the candidate

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
--------------------------------------------------------------------LINKEDIN PROFILE type 89: URL 
, LatestLinkedin89 as (select a.ApplicantId, max(p.PhoneId) as MaxPhoneId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 89 and p.Num like '%linkedin%'
group by a.ApplicantId)

, Linkedin89 as (select li89.ApplicantId, p.Num as LinkedIn89
 from LatestLinkedin89 li89 left join Phones p on li89.MaxPhoneId = p.PhoneId)

 ------------------------------------------LINKEDIN PROFILE type 91: social networking 
, LatestLinkedin91 as (select a.ApplicantId, max(p.PhoneId) as MaxPhoneId
from Applicants a
left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 91 and p.Num like '%linkedin%'
group by a.ApplicantId)

, Linkedin91 as (select li91.ApplicantId, p.Num as LinkedIn91
 from LatestLinkedin91 li91 left join Phones p on li91.MaxPhoneId = p.PhoneId)
 ----------------------------------------GET LINKEDIN BY COMBINE 2 TYPES, PREFER TO TYPE 89
 , LinkedIn as (select a.ApplicantId, coalesce(l89.LinkedIn89,l91.linkedin91) as CanLinkedin
  from Applicants a left join Linkedin89 l89 on a.ApplicantId = l89.ApplicantId
  left join Linkedin91 l91  on a.ApplicantId = l91.ApplicantId)
--	where l89.LinkedIn89 is not null or l91.linkedin91 is not null)

------------------------------------------------------------------------------------------All linkedin: add to note
, TempLinkedIn as (select a.ApplicantId, p.CommunicationTypeId, p.Num
	from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
	where (p.CommunicationTypeId = 89 or p.CommunicationTypeId = 91) and p.Num like '%linkedin%')

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
		  + coalesce('Consultant: ' + Consultantusername + char(10), '') + iif(Notes = '' or Notes is null,'',concat('Notes:',char(10),Notes))
          from  tempComment
          WHERE ApplicantId = tcmt.ApplicantId
		  order by CreatedOn desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS AllComment
FROM tempComment as tcmt
GROUP BY tcmt.ApplicantId)
----Candidate Address: get the latest address
, maxadd as (select a.ApplicantId, max(Ad.AddressId) as maxAddressId
from Applicants a
left join Address Ad on a.ApplicantId = Ad.ObjectId
group by a.ApplicantId)
----select * from minadd-----
, tempAddress as (select ma.ApplicantId, Ad.AddressId, Ad.Building, Ad.Street, Ad.District, Ad.City, Ad.PostCode
from maxadd as ma left join Address as Ad on Ad.AddressId = ma.maxAddressId)
--select * from tempAddress----
, CanAddress as (
	select ta.ApplicantId,
	ltrim(rtrim(concat(iif(ta.Building = '' or ta.Building is NULL,'',concat(ta.Building,', '))
	, iif(ta.Street = '' or ta.Street is NULL,'',concat(ta.Street,', '))
	, iif(ta.District = '' or ta.District is NULL,'',concat(ta.District,', '))
	, iif(ta.City = '' or ta.City is NULL,'',concat(ta.City,', '))
	, iif(ta.Postcode = '' or ta.Postcode is NULL,'',ta.Postcode)))) as 'canaddress'
	from tempAddress ta)
------------Candidate all address
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
			+ Coalesce(', ' + NULLIF(ad.Postcode, ''), '')
			, 1, 1, '') ) as 'canalladdress'
from Applicants a left join Address ad on a.ApplicantId = Ad.ObjectId)

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

-------------------------------------------------------------MAIN SCRIPT
select concat('FR', a.ApplicantId) as 'candidate-externalId'
, iif(rtrim(ltrim(p.PersonName)) = '' or rtrim(ltrim(p.PersonName)) is null, concat('NoFirstname-', a.ApplicantId), rtrim(ltrim(p.PersonName))) as 'candidate-firstName'
, iif(rtrim(ltrim(p.SurName)) = '' or rtrim(ltrim(p.SurName)) is null, concat('NoLastname-', a.ApplicantId), rtrim(ltrim(p.SurName))) as 'candidate-Lastname'
, case
	when ai.PrimaryEmailAddress is not NULL and ai.ApplicantId in (select ApplicantId from CandidateEmail) then ce.CandidateEmail
	else concat('candidate_ID-',a.ApplicantId,'@noemail.com') end as 'candidate-email'
--, cpe.CanPersonalEmail as 'candidate-workEmail'
, case
	when lv.ValueName = 'Male' then 'MALE'
	when lv.ValueName = 'Female' then 'FEMALE'
	else '' end as 'candidate-gender'
, CanTitle.canTitle as 'candidate-title'
, m.Mobile as 'candidate-mobile'
, coalesce(pp.Num,m.Mobile) as 'candidate-phone'
, li.CanLinkedin as 'candidate-linkedin'
, cadd.canaddress as 'candidate-address'
, tadd.City as 'candidate-city'
, tadd.PostCode as 'candidate-zipCode'
, convert(varchar(10),p.dob,120) as 'candidate-dob'
--, agv.Location as 'candidate-address'
, n.Nationality
, case
	when n.Nationality like 'Afghan%' then 'AF'
	when n.Nationality like '%Africa%' then 'ZA'
	when n.Nationality like 'Albani%' then 'AL'
	when n.Nationality like '%Americ%' then 'US'
	when n.Nationality like 'Andorr%' then 'AD'
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
	when n.Nationality like 'Polish%' then 'PL'
	when n.Nationality like 'Portu%' then 'PT'
	when n.Nationality like 'Russia%' then 'RU'
	when n.Nationality like 'Serbia%' then 'RS'
	when n.Nationality like 'Singap%' then 'SG'
	when n.Nationality like 'Sri%' then 'LK'
	when n.Nationality like 'South Africa%' then 'ZA'
	when n.Nationality like 'Spanish%' then 'ES'
	when n.Nationality like 'Sri Lankan%' then 'LK'
	when n.Nationality like 'Sri lankan%' then 'LK'
	when n.Nationality like 'Swedish%' then 'SE'
	when n.Nationality like 'Swiss%' then 'CH'
	when n.Nationality like 'Taiwan%' then 'TW'
	when n.Nationality like 'Thai%' then 'TH'
	when n.Nationality like 'Trinida%' then 'TT'
	when n.Nationality like 'Turk%' then 'TR'
	when n.Nationality like 'Vietna%' then 'VN'
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
when a.EmploymentTypeId = 5 then 'TEMPORARY_TO_PERMANENT'
else 'PERMANENT' end as 'candidate-jobtype'
, a.CurrentBasic as 'candidate-CurrentSalary'
, a.Rate as 'candidate-ContractRate'
, case 
when a.RateUnit = 80 then 'HOURS'
when a.RateUnit = 81 then 'DAYS'
when a.RateUnit = 82 then 'WEEKS'
when a.RateUnit = 83 then 'MONTHS'
when a.RateUnit = 84 then 'YEARS'
else '' end as 'candidate-contractInterval'
, case
when a.CurrencyId = 10 then 'GBP'
when a.CurrencyId = 12 then 'EUR'
else '' end as 'candidate-currency'
, cd.CanDocs as 'candidate-resume'
, left(cc.AllComment,32000) as 'candidate-comments'
, left(concat('Candidate External ID: FR',a.ApplicantId,char(10)
	,iif(p.Salutation = '' or p.Salutation is NULL,'',concat('Salutation: ',p.Salutation,char(10)))
	,iif(a.JobTitle = '' or a.JobTitle is NULL,'',concat('Job Title: ',a.JobTitle,char(10)))
	,iif(cpe.CanPersonalEmail = '' or cpe.CanPersonalEmail is NULL,'',concat('Personal Email(s): ',cpe.CanPersonalEmail,char(10)))
	,iif(allp.canPhone = '' or allp.canPhone is NULL,'',concat('Primary Phone(s): ',allp.canPhone,char(10)))
	,iif(allm.canMobile = '' or allm.canMobile is NULL,'',concat('Mobile(s): ', allm.canMobile,char(10)))
	,iif(ali.canAllLinkedIn = '' or ali.canAllLinkedIn is NULL,'',concat('All Linkedin URL(s): ', ali.canAllLinkedIn,char(10)))
	,iif(agv.Location = '' or agv.Location is NULL,'',concat('Location: ',agv.Location,char(10)))
	,iif(aad.canalladdress = '' or aad.canalladdress is NULL,'',concat('All Address(es): ',aad.canalladdress,char(10)))
	,iif(agv.EmploymentType = '' or agv.EmploymentType is NULL,'',concat('Employment Type(s): ',left(agv.EmploymentType,len(agv.EmploymentType)-1),char(10)))
	,iif(agv.Status = '' or agv.Status is NULL,'',concat('Status: ',agv.Status,char(10)))
	,iif(agv.StatusDate = '' or agv.StatusDate is NULL,'',concat('Status Date: ',agv.StatusDate,char(10)))
	,iif(a.CreatedOn = '' or a.CreatedOn is NULL,'',concat('Start/Reg date: ',a.CreatedOn,char(10)))
	,iif(agv.Source = '' or agv.Source is NULL,'',concat('Source: ',agv.Source,char(10)))
	,iif(agv.[Notice Period] = '' or agv.[Notice Period] is NULL,'',concat('Notice Period: ',agv.[Notice Period],char(10)))
	,iif(a.Assessedby is NULL,'',concat('Assessed by: ', u.UserName,' ', u.SurName,', Job Title: ', u.JobTitle,char(10)))
	,iif(a.AssessmentDate = '' or a.AssessmentDate is NULL,'',concat('Assessment date: ', a.AssessmentDate,char(10)))
	,iif(a.MinBasic is NULL,'',concat('Min Basic: ', a.MinBasic,char(10)))
	,iif(a.CurrentBasic is NULL,'',concat('Current Basic: ', a.CurrentBasic,char(10)))
	,iif(a.MinPackage is NULL,'',concat('Min Package: ', a.MinPackage,char(10)))
	,iif(a.CurrentPackage is NULL,'',concat('Current Package: ', a.CurrentPackage,char(10)))
	,iif(a.Rate is NULL,'',concat('Rate: ', a.Rate,char(10)))
	,iif(cvi.CVInfo = '' or cvi.CVInfo is NULL,'',concat('CV Information: ',char(10),cvi.CVInfo,char(10)))
	,iif(p.Notes = '' or p.Notes is NULL,'',concat('Notes: ',p.Notes,char(10)))
	,coalesce (char(10) + 'Resume: ' + tcr.ResumeText, '')),32000) as 'candidate-note'

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
	left join Temp_CVInfo cvi on a.ApplicantId = cvi.ApplicantId
	left join CurrentEmployer cel on a.ApplicantId = cel.ApplicantId
	left join Temp_WorkHistory th on a.ApplicantId = th.ApplicantId
	left join CanComment cc on a.ApplicantId = cc.ApplicantId
	left join CanAddress cadd on a.ApplicantId = cadd.ApplicantId
	left join tempAddress tadd on a.ApplicantId = tadd.ApplicantId
	left join AllAdress aad on a.ApplicantId = aad.ApplicantId
	left join Temp_CandidateResume tcr on a.ApplicantId = tcr.ApplicantId
	left join CanDocuments cd on a.ApplicantId = cd.ApplicantId
	left join AllPhone allp on a.ApplicantId = allp.ApplicantId
	left join AllMobile allm on a.ApplicantId = allm.ApplicantId
	left join LinkedIn li on a.ApplicantId = li.ApplicantId
	left join AllLinkedIn ali on a.ApplicantId = ali.ApplicantId
	left join CanTitle on a.ApplicantId = CanTitle.ApplicantId
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
