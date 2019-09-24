with candidateAllEmails as (select ct.intCandidateTelecomId, ct.intCandidateId, vchValue as 'original-email'
							, case 
								when CHARINDEX(',',vchValue) = 1 then replace(vchValue,',','')
								when CHARINDEX(',',vchValue) <> 0 then left(vchValue,CHARINDEX(',',vchValue)-1)
								when CHARINDEX('/',vchValue) = 1 then replace(vchValue,'/','')
								when CHARINDEX('/',vchValue) <> 0 then left(vchValue,CHARINDEX('/',vchValue)-1)
								when CHARINDEX(';',vchValue) <> 0 then left(vchValue,CHARINDEX(';',vchValue)-1)
								when CHARINDEX('-',vchValue) = 1 then right(vchValue,len(vchValue)-1)
								else vchValue end as email
							, vchForename, vchMiddlename, vchSurname, vchDescription,
		ROW_NUMBER() OVER(PARTITION BY ct.intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom  ct left join dCandidate c on ct.intCandidateId = c.intCandidateId
						left join dPerson p on c.intPersonId = p.intPersonId
where vchValue like '%_@_%.__%')

, Email_EditFormat as (
SELECT intCandidateId, vchDescription
	 , ltrim(rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'''',''),'$',''),':',''),'?',''),'~',''),' ',''),'|',''),'[',''),']',''),'mailto',''))) as email
from candidateAllEmails
where  CHARINDEX(',',email) = 0 and CHARINDEX('/',email) = 0 and rn =1)

, EmailDupRegconition as (
SELECT intCandidateId, vchDescription, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY intCandidateId ASC) AS rn 
from Email_EditFormat)


, CandidateMainEmail as (select intCandidateId
, case	when rn = 1 then email
		else concat(rn,'_',email) end as CandidateEmail
, rn
from EmailDupRegconition)

--------------------------------------CANDIDATE RESUMES
, CVName as (
select intCandidateId, dtInserted, concat('CV',intCandidateCVId,'_',convert(date,dtInserted),
 --coalesce('_' + NULLIF(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(vchCVName,'?',''),' ',''),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),''),''), vchFileType)
 iif(right(vchCVName,4)=vchFileType or right(vchCVName,5)=vchFileType,concat('_',replace(vchCVName,'/','.')),iif(vchFileType= '.', concat('_',vchCVName,'.docx'),concat('_',replace(vchCVName,'/','.'),vchFileType)))) as CVFullName
from dCandidateCV)
, CanResumes as (select intCandidateId, STUFF(
					(Select ',' + replace(replace(CVFullName,' ','_'),'%','_')
					from CVName 
					where intCandidateId = cvn.intCandidateId
    order by intCandidateId asc, dtInserted desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'CVName'
FROM CVName as cvn
GROUP BY cvn.intCandidateId)

--, tempCanAttachment as(
--SELECT intCandidateId, ac.intAttachmentId--, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn
--		,case when vchFileType like '.eml' then e.msgfilename 
--		else
--		 concat(ac.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		 end as attachmentName
--from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
--							 left join email e on a.intAttachmentId = e.AttachmentID
--where vchFileType not in ('.mp4')
--union  --union with email files got from candidate events
--select ec.intCandidateId, ae.intAttachmentId, em.msgfilename as attachmentName--, a.vchAttachmentName
--from lEventCandidate ec left join dEvent e on ec.intEventId = e.intEventId
--				--left join dCandidate c on ec.intCandidateId = c.intCandidateId
--				left join lAttachmentEvent ae on ec.intEventId = ae.intEventId
--				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
--				left join email em on ae.intAttachmentId = em.AttachmentID
--where em.AttachmentID is not null)

--, canAttachment as (SELECT intCandidateId, 
--     STUFF(
--         (SELECT ',' + replace(attachmentName,'%','_')
--          from  tempCanAttachment
--          WHERE intCandidateId =ca.intCandidateId
--    order by intCandidateId asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          ,1,1, '')  AS canAttachments
--FROM tempCanAttachment as ca
--GROUP BY ca.intCandidateId)

--, tempCanDocuments as (select * from CanResumes union all select * from canAttachment)
----select * from tempCanDocuments
----select * from tempCan where ApplicantId = 142
--, CanDocuments as (select intCandidateId, STUFF(
--					(Select ',' + CVName
--					from tempCanDocuments 
--					where intCandidateId = tcd.intCandidateId
--    order by intCandidateId asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          , 1, 1, '')  AS 'CanDocs'
--FROM tempCanDocuments as tcd
--GROUP BY tcd.intCandidateId)

------------------------------Candidate consultants as owners
, temp_Consultant as (select intcandidateId, u.vchEmail as canOwner
from lConsultantCandidate cc 
		left join dUser u on cc.intConsultantId = u.intUserId)
, CanOwners as (select intCandidateId, STUFF(
					(Select ',' + canOwner
					from temp_Consultant 
					where intCandidateId = tc.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'canOwners'
FROM temp_Consultant as tc
GROUP BY tc.intCandidateId)

, temp_homePhone as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as vchValue
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 1)

, CanHomePhone as (select intCandidateId, vchValue from temp_homePhone where rn = 1)

----------------------------------Candidate Mobile
, temp_Mobile as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as vchValue
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 5 and vchValue not like '%@%')

--Get all mobile to put to Notes
, temp_allMobile as (select intCandidateId, STUFF(
					(Select '; ' + replace(replace(vchValue,char(0x0002),''),char(0x0001),'')
					from temp_Mobile 
					where intCandidateId = tm.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'allMobile'
FROM temp_Mobile as tm
GROUP BY tm.intCandidateId)

, CanMobile as (select intCandidateId, vchValue from temp_Mobile where rn =1)

--------------------------------Get Work phone
, temp_WorkPhone as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as vchValue
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 2)

, workPhone as (
select intCandidateId, STUFF(
					(Select '|' + replace(replace(vchValue,char(0x0002),''),char(0x0001),'')
					from temp_WorkPhone 
					where intCandidateId = twp.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'vchValue'
FROM temp_WorkPhone as twp
GROUP BY twp.intCandidateId)

----CANDIDATE ADDRESS: 
, tempLocation as (select ca.intCandidateId, ca.tintCandidateAddressTypeId, ca.vchAddressLine1, ca.vchAddressLine2 ,ca.vchAddressLine3
	, ca.vchTown, ca.vchCounty,ca.sintCountryId, rc.vchCountryName, rc.vchCountryCode, ca.vchPostcode, ca.vchDescription
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY tintCandidateAddressTypeId ASC) AS rn
	, Stuff(
			  Coalesce(' ' + NULLIF(ltrim(rtrim(vchAddressLine1)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchAddressLine2)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchAddressLine3)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchTown)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchCounty)), ''), '')
			+ Coalesce(', ' + NULLIF(rc.vchCountryName, ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchPostCode)), ''), '')
			, 1, 1, '') as 'fullAddress' 
from dCandidateAddress ca left join refCountry rc on ca.sintCountryId = rc.sintCountryId)

, candidateLocation as (select * from tempLocation where rn=1 and fullAddress is not null)

-------------------------------------------------------------MAIN SCRIPT
--insert into ImportCandidate
,mainscript as (select c.intCandidateId as 'candidate-externalId', p.intPersonId  as PersonId--just for reference afterward
, iif(rtrim(ltrim(p.vchForename)) = '' or rtrim(ltrim(p.vchForename)) is null, concat('NoFirstname-', c.intCandidateId), rtrim(ltrim(p.vchForename))) as 'candidate-firstName'
, iif(rtrim(ltrim(p.vchSurName)) = '' or rtrim(ltrim(p.vchSurName)) is null, concat('NoLastname-', c.intCandidateId), rtrim(ltrim(p.vchSurName))) as 'candidate-Lastname'
, iif(rtrim(ltrim(p.vchMiddlename)) = '' or rtrim(ltrim(p.vchMiddlename)) is null, '', rtrim(ltrim(p.vchMiddlename))) as 'candidate-Middlename'
, iif(cme.CandidateEmail = '' or cme.CandidateEmail is NULL,concat(c.intCandidateId,'@noemail.com'),cme.CandidateEmail) as 'candidate-email'
--, iif(we.workEmail = '' or we.workEmail is NULL,'',we.workEmail) as 'candidate-workEmail'--a lot of email has incorrect format, so if these candidates are skipped importing, remove work email
, cast(iif(p.dDOB is null or p.dDOB = '','',convert(varchar(10),p.dDOB,120)) as datetime) as 'candidate-dob'
, upper(t.vchTitleName) as 'candidate-title'
, case
	when p.tintGenderId = 1 then 'MALE'
	when p.tintGenderId = 2 then 'FEMALE'
	else '' end as 'candidate-gender'
--, replace(coalesce(pp.vchValue, cm.vchValue, hp.vchValue),',','') as 'candidate-phone'
, isnull(replace(cm.vchvalue,',',''),'') as 'candidate-mobile'
, isnull(wp.vchValue,'') as 'candidate-workPhone'
, isnull(hp.vchValue,'') as 'candidate-homephone'
, iif(p.vchLinkedInUrl like '%linkedin%',p.vchLinkedInUrl,'') as 'candidate-linkedin'
--, s.skype as 'candidate-skype'
--, left(c.vchComment,32000) as 'candidate-comments'--no more support importing comment
, isnull(iif(right(cl.fullAddress,1)= ',',left(replace(cl.fullAddress,',,',','),len(cl.fullAddress)-1),replace(cl.fullAddress,',,',',')),'') as 'candidate-address'
, iif(cl.vchTown = '' or cl.vchTown is null, '', cl.vchTown) as 'candidate-city'
, iif(cl.vchCounty = '' or cl.vchCounty is null, '', cl.vchCounty) as 'candidate-state'
, iif(cl.vchPostcode = '' or cl.vchPostcode is null, '', cl.vchPostcode) as 'candidate-zipCode'
, iif(cl.sintCountryId <> 0, cl.vchCountryCode, iif(cl.fullAddress like '%London%' or cl.fullAddress like '%Oxford%' or cl.fullAddress like '%UK%', 'GB', iif(cl.fullAddress like '%New York%' or cl.fullAddress like '%NYC%','US',''))) as 'candidate-Country'
, isnull(co.canOwners,'') as 'candidate-owners'
, case
	when rn.vchNationalityName like '%Afghan%' then 'AF'
	when rn.vchNationalityName like '%Algeri%' then 'DZ'
	when rn.vchNationalityName like '%Africa%' then 'ZA'
	when rn.vchNationalityName like '%Albani%' then 'AL'
	when rn.vchNationalityName like '%America%' then 'US'
	when rn.vchNationalityName like '%Andorr%' then 'AD'
	when rn.vchNationalityName like '%Argentin%' then 'AR'
	when rn.vchNationalityName like '%Austra%' then 'AU'
	when rn.vchNationalityName like '%Austri%' then 'AT'
	when rn.vchNationalityName like '%Belgia%' then 'BE'
	when rn.vchNationalityName like '%Brazil%' then 'BR'
	when rn.vchNationalityName like 'Britis%' then 'GB'
	when rn.vchNationalityName like 'Bucha%' then 'RO'
	when rn.vchNationalityName like '%Bulgari%' then 'BG'
	when rn.vchNationalityName like 'Burmes%' then 'MM'
	when rn.vchNationalityName like 'Cambod%' then 'KH'
	when rn.vchNationalityName like 'Canadi%' then 'CA'
	when rn.vchNationalityName like 'Chines%' then 'CN'
	when rn.vchNationalityName like 'Colombi%' then 'CO'
	when rn.vchNationalityName like 'Costa%' then 'CR'
	when rn.vchNationalityName like '%Cypr%' then 'CY'
	when rn.vchNationalityName like '%Czech%' then 'CZ'
	when rn.vchNationalityName like '%Danish%' then 'DK'
	when rn.vchNationalityName like 'Denmark%' then 'DK'
	when rn.vchNationalityName like '%Dutch%' then 'NL'
	when rn.vchNationalityName like 'East%' then 'ZA'
	when rn.vchNationalityName like '%Egypt%' then 'EG'
	when rn.vchNationalityName like 'Emiria%' then 'AE'
	when rn.vchNationalityName like 'Eritre%' then 'ER'
	when rn.vchNationalityName like 'Estoni%' then 'EE'
	when rn.vchNationalityName like 'Ethiop%' then 'ET'
	when rn.vchNationalityName like 'Europe%' then 'TR'
	when rn.vchNationalityName like 'Fijian%' then 'FJ'
	when rn.vchNationalityName like 'Filipi%' then 'PH'
	when rn.vchNationalityName like 'fili%' then 'PH'
	when rn.vchNationalityName like 'Finnish%' then 'FI'
	when rn.vchNationalityName like 'Flemish%' then 'BE'
	when rn.vchNationalityName like 'French%' then 'FR'
	when rn.vchNationalityName like 'Gabone%' then 'GA'
	when rn.vchNationalityName like 'German%' then 'DE'
	when rn.vchNationalityName like '%Georgi%' then 'GE'
	when rn.vchNationalityName like 'Ghanai%' then 'GH'
	when rn.vchNationalityName like 'Gree%' then 'GR'
	when rn.vchNationalityName like 'Hunga%' then 'HU'
	when rn.vchNationalityName like 'Indian%' then 'IN'
	when rn.vchNationalityName like 'Indone%' then 'ID'
	when rn.vchNationalityName like 'Irania%' then 'IR'
	when rn.vchNationalityName like 'Iraq%' then 'IQ'
	when rn.vchNationalityName like 'Irish%' then 'IE'
	when rn.vchNationalityName like 'Isra%' then 'IL'
	when rn.vchNationalityName like 'Ital%' then 'IT'
	when rn.vchNationalityName like 'Jamaic%' then 'JM'
	when rn.vchNationalityName like 'Japane%' then 'JP'
	when rn.vchNationalityName like 'Keny%' then 'KE'
	when rn.vchNationalityName like 'Leban%' then 'LB'
	when rn.vchNationalityName like 'Lithua%' then 'LT'
	when rn.vchNationalityName like 'Malaga%' then 'MG'
	when rn.vchNationalityName like 'Malays%' then 'MY'
	when rn.vchNationalityName like 'Malt%' then 'MT'
	when rn.vchNationalityName like 'Mauritian%' then 'MU'
	when rn.vchNationalityName like 'Mexi%' then 'MX'
	when rn.vchNationalityName like 'Moroc%' then 'MA'
	when rn.vchNationalityName like 'Namibi%' then 'NA'
	when rn.vchNationalityName like 'New Zea%' then 'NZ'
	when rn.vchNationalityName like 'Nigeri%' then 'NG'
	when rn.vchNationalityName like 'Northern Irish' then 'IE'
	when rn.vchNationalityName like 'Norwe%' then 'NO'
	when rn.vchNationalityName like 'Pakist%' then 'PK'
	when rn.vchNationalityName like 'Philip%' then 'PH'
	when rn.vchNationalityName like 'Phili%' then 'PH'
	when rn.vchNationalityName like 'Polish%' then 'PL'
	when rn.vchNationalityName like 'Portu%' then 'PT'
	when rn.vchNationalityName like 'Romani%' then 'RO'
	when rn.vchNationalityName like 'Russia%' then 'RU'
	when rn.vchNationalityName like 'Senegal%' then 'SN'
	when rn.vchNationalityName like 'Serbia%' then 'RS'
	when rn.vchNationalityName like 'Singap%' then 'SG'
	when rn.vchNationalityName like 'Slovaki%' then 'SK'
	when rn.vchNationalityName like '%South Korea%' then 'KR'
	when rn.vchNationalityName like 'Sri%' then 'LK'
	when rn.vchNationalityName like 'South Africa%' then 'ZA'
	when rn.vchNationalityName like 'Spanish%' then 'ES'
	when rn.vchNationalityName like 'Sri Lanka%' then 'LK'
	when rn.vchNationalityName like 'Sri lanka%' then 'LK'
	when rn.vchNationalityName like 'Swedish%' then 'SE'
	when rn.vchNationalityName like 'Swiss%' then 'CH'
	when rn.vchNationalityName like 'Taiwan%' then 'TW'
	when rn.vchNationalityName like '%Ukrain%' then 'UA'
	when rn.vchNationalityName like 'Thai%' then 'TH'
	when rn.vchNationalityName like 'Trinida%' then 'TT'
	when rn.vchNationalityName like 'Turk%' then 'TR'
	when rn.vchNationalityName like 'Vietna%' then 'VN'
	--when rn.vchNationalityName like 'Yugoslavia%' then 'YU'
	when rn.vchNationalityName like '%UNITED%ARAB%' then 'AE'
	when rn.vchNationalityName like '%UAE%' then 'AE'
	when rn.vchNationalityName like '%U.A.E%' then 'AE'
	when rn.vchNationalityName like '%UNITED%KINGDOM%' then 'GB'
	when rn.vchNationalityName like '%UNITED%STATES%' then 'US'
	when rn.vchNationalityName like '%US%' then 'US'
	when rn.vchNationalityName like '%Zimbab%' then 'ZW'
else '' end as 'candidate-citizenship'
, coalesce(pce.vchExplicitJobTitle,c.vchJobTitle) as 'candidate-jobTitle1'
, isnull(coalesce(pce.vchExplicitCompanyName, pce.vchCompanyName),'') as 'candidate-employer1'
--, cwh.WorkHistory as 'candidate-workHistory'
--, cr.CVName as 'candidate-resumes'
--, iif(len(cd.CanDocs)>32000,'',cd.CanDocs) as 'candidate-resume'
--, catt.canAttributes as 'candidate-skills'
--, ccb.callBackInfo : just for testing
, left(
	concat('Candidate External ID: ',c.intCandidateId
	, concat(char(10), char(10),'Voyager Candidate Code: ',c.vchStandardRefCode)
	,iif(rcs.vchCandidateStatusName = '' or rcs.vchCandidateStatusName is NULL,'',concat(char(10), char(10),'Candidate Custom Status: ',rcs.vchCandidateStatusName))--use this for njf search and njf gtp
	,concat(char(10), char(10),'Actively looking? ', replace(replace(bitActivelyLooking,0,'No'),1,'Yes'))--use this for njf contracts db
	,iif(p.vchKnownAs = '' or p.vchKnownAs is null,'',concat(char(10), char(10),'Known As: ',p.vchKnownAs))
	,iif(rc2.vchCountryName = '' or rc2.vchCountryName is NULL,'',concat(char(10), char(10),'Place Of Birth: ',rc2.vchCountryName))
	--,iif(tahp.allHomePhone = '' or tahp.allHomePhone is NULL,'',concat(char(10), char(10),'Home Phone(s): ',tahp.allHomePhone))
	,iif(tam.allMobile = '' or tam.allMobile is NULL,'',concat(char(10), char(10),'Mobile(s): ', tam.allMobile))
	--,iif(he.homeEmail = '' or he.homeEmail is NULL,'',concat(char(10), char(10),'Home Email(s): ',he.homeEmail))
	--,iif(owe.workEmail = '' or owe.workEmail is NULL,'',concat(char(10), char(10),'Work Email: ',owe.workEmail))
	--,iif(f.fax = '' or f.fax is NULL,'',concat(char(10), char(10),'Fax: ', f.fax))
	--,iif(w.web = '' or w.web is NULL,'',concat(char(10), char(10),'Website: ', w.web))
	,iif(c.vchBasedIn = '' or c.vchBasedIn is NULL,'',concat(char(10), char(10),'Based In: ', c.vchBasedIn))
	--,concat(char(10), char(10),'Relocated Flag: ', c.bitRelocate)delete from importcandidate
	,iif(rms.vchMaritalStatusName = '' or rms.vchMaritalStatusName is NULL,'',concat(char(10), char(10),'Marital Status: ', rms.vchMaritalStatusName))
	--,iif(csrc.sources = '' or csrc.sources is NULL,'',concat(char(10), char(10),'Candidate Source(s): ',csrc.sources))
	,iif(npi.vchNoticePeriodIntervalName = '','',concat(char(10), char(10),'Notice Period Interval: ',npi.vchNoticePeriodIntervalName))
	,iif(c.tintNoticePeriodQuantity = 0,'',concat(char(10), char(10),'Notice Period Quantity: ', c.tintNoticePeriodQuantity))
	--,iif(ccb.CallBackInfo = '' or ccb.CallBackInfo is NULL,'',concat(char(10), char(10),'Call Back Records: ', char(10),ccb.CallBackInfo))
	,iif(c.vchSummary = '' or c.vchSummary is NULL,'',concat(char(10), char(10), 'Summary: ',char(10), c.vchSummary, ''))
	,iif(c.vchComment = '' or c.vchComment is NULL,'',concat(char(10), char(10), 'Comment: ',char(10), c.vchComment, ''))
	),32000) as 'candidate-note'--, vchForename, vchSurname
from dCandidate c
				left join dPerson p on c.intPersonId = p.intPersonId
				left join CandidateMainEmail cme on c.intCandidateId = cme.intCandidateId
				left join refTitle t on p.tintTitleId = t.tintTitleId
				left join candidateLocation cl on c.intCandidateId = cl.intCandidateId
				--left join CandiddateCallBack ccb on c.intCandidateId = ccb.intCandidateId
				--left join CanResumes cr on c.intCandidateId = cr.intCandidateId--select distinct bitActivelyLooking from dcandidate--select * from dcandidatesystemstatus
				--left join localCanDocuments cd on c.intCandidateId = cd.intCandidateId
				--left join Temp_Candidate_WorkHistory cwh on c.intCandidateId = cwh.intCandidateId
				left join CanOwners co on c.intCandidateId = co.intCandidateId
				left join refCandidateStatus rcs on c.tintCustomStatusId = rcs.tintCandidateStatusId--njf contract has no status field
				left join refNationality rn on p.sintNationality1Id =rn.sintNationalityId
				-- left join refCountry rc on p.sintNationality1Id = rc.sintCountryId
				-- left join refCountry rc1 on p.sintNationality4Id = rc1.sintCountryId
				left join refCountry rc2 on p.sintCountryOfOriginId = rc2.sintCountryId
				left join WorkPhone wp on c.intCandidateId = wp.intCandidateId --and wp.rn = 1 --refer from sTelecom--There is only 1 candidate has 2 work phone so combine as 1
				left join CanHomePhone hp on c.intCandidateId = hp.intCandidateId
				--left join temp_allHomePhone tahp on c.intCandidateId = tahp.intCandidateId
				left join CanMobile cm on c.intCandidateId = cm.intCandidateId
				left join temp_allMobile tam on c.intCandidateId = tam.intCandidateId
				--left join tempPreferredPhone pp on c.intCandidateId = pp.intCandidateId
				left join refMaritalStatus rms on p.tintMaritalStatusId = rms.tintMaritalStatusId
				--left join CanSources csrc on c.intCandidateId = csrc.intCandidateId
				left join refNoticePeriodInterval npi on c.tintNoticePeriodIntervalId = npi.tintNoticePeriodIntervalId
				left join dPersonCurrentEmployment pce on p.intPersonId = pce.intPersonId
				--left join CanAttributes catt on c.intCandidateId = catt.intCandidateId
				--left join Skype s on c.intCandidateId = s.intCandidateId
				--left join Fax f on c.intCandidateId = f.intCandidateId
				--left join Web w on c.intCandidateId = w.intCandidateId
				--left join HomeEmail he on c.intCandidateId = he.intCandidateId
				--left join WorkEmail we on c.intCandidateId = we.intCandidateId
				--left join OriginalWorkEmail owe on c.intCandidateId = owe.intCandidateId
)
,test10 as (select a.intCandidateId
,iif(a.tintContractualLegalLawfulBasisId = '' or a.tintContractualLegalLawfulBasisId is null,'','Contractual') as Contractual_Legal_Lawful_Basis
,a.datContractualLegalLawfulBasisExpiry
,b.vchLawfulBasisName
,d.vchLawfulBasisStatus 
,a.datExpiry
,e.vchLawfulBasisReason
,iif(a.bitIsManuallyRestricted = 0,concat('Restrict Recruitment Processing: ','No'),concat('Restrict Recruitment Processing: ','Yes')) as 'Restrict Recruitment Processing'
,f.vchRestrictionReason
,a.datManuallyRestrictedUntil
from dCandidatePrivacy a
left join sLawfulBasis b on a.tintLawfulBasisId = b.tintLawfulBasisId
left join lLawfulBasisLawfulBasisStatus c on a.intLawfulBasisLawfulBasisStatusId = c.intLawfulBasisLawfulBasisStatusId
left join sLawfulBasisStatus d on c.tintLawfulBasisStatusId = d.tintLawfulBasisStatusId
left join refLawfulBasisReason e on a.intLawfulBasisReasonId = e.intLawfulBasisReasonId
left join refRestrictionReason f on a.intManuallyRestrictedReasonId = f.intRestrictionReasonId)


,test11 as (select 
intCandidateId,
concat(
nullif(concat('Contractual Legal Lawful Basis: ', Contractual_Legal_Lawful_Basis,(char(13)+char(10))),concat('Contractual Legal Lawful Basis: ',(char(13)+char(10))))
,nullif(concat('Contractual Legal Lawful Basis Expiration Date: ', datContractualLegalLawfulBasisExpiry,(char(13)+char(10))),concat('Contractual Legal Lawful Basis Expiration Date: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Name: ', vchLawfulBasisName,(char(13)+char(10))),concat('Lawful Basis Name: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Status: ', vchLawfulBasisStatus,(char(13)+char(10))),concat('Lawful Basis Status: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Expiration Date: ', datExpiry,(char(13)+char(10))),concat('Lawful Basis Expiration Date: ',(char(13)+char(10))))
,nullif(concat('Lawful Basis Reason: ', vchLawfulBasisReason,(char(13)+char(10))),concat('Lawful Basis Reason: ',(char(13)+char(10))))
,[Restrict Recruitment Processing],(char(13)+char(10))
,nullif(concat('Restriction Reason: ', vchRestrictionReason,(char(13)+char(10))),concat('Restriction Reason: ',(char(13)+char(10))))
,nullif(concat('Restriction Until: ', datManuallyRestrictedUntil,(char(13)+char(10))),concat('Restriction Until: ',(char(13)+char(10))))
) as 'Note2'
from test10)


select concat(a.[candidate-note],
(char(13)+char(10)),b.Note2) as note_final,a.* from mainscript a left join test11 b on a.[candidate-externalId] = b.intCandidateId