--CANDIDATE DUPLICATE MAIL REGCONITION
with candidateAllEmails as (select ct.intCandidateTelecomId, ct.intCandidateId, vchValue as 'original-email'
							, case 
								when CHARINDEX(',',vchValue) = 1 then replace(vchValue,',','')
								when CHARINDEX(',',vchValue) <> 0 then left(vchValue,CHARINDEX(',',vchValue)-1)
								when CHARINDEX('/',vchValue) = 1 then replace(vchValue,'/','')
								when CHARINDEX('/',vchValue) <> 0 then left(vchValue,CHARINDEX('/',vchValue)-1)
								when CHARINDEX(';',vchValue) <> 0 then left(vchValue,CHARINDEX(';',vchValue)-1)
								when CHARINDEX('-',vchValue) = 1 then right(vchValue,len(vchValue)-1)
								--when CHARINDEX('.',vchValue) = 1 then right(vchValue,len(vchValue)-1)--replace(vchValue,'.','')--
								else vchValue end as email
							, vchForename, vchMiddlename, vchSurname, vchDescription,
		ROW_NUMBER() OVER(PARTITION BY ct.intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom  ct left join dCandidate c on ct.intCandidateId = c.intCandidateId
						left join dPerson p on c.intPersonId = p.intPersonId
where vchValue like '%_@_%.__%')-- and CHARINDEX('-',vchValue) =1)

--select * from candidateAllEmails where intCandidateId = 41--Email = 'a.novikovs@gmail.com'--intCandidateId = 39740
-----------Edit email format
, Email_EditFormat as (
SELECT intCandidateId, vchDescription
	 , ltrim(rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'''',''),'$',''),':',''),'?',''),'~',''),' ',''),'|',''),'[',''),']',''),'mailto',''))) as email
from candidateAllEmails
where  CHARINDEX(',',email) = 0 and CHARINDEX('/',email) = 0 and rn =1)

, EmailDupRegconition as (
SELECT intCandidateId, vchDescription, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY intCandidateId ASC) AS rn 
from Email_EditFormat)

--select * from EmailDupRegconition where Email = 'a.novikovs@gmail.com'--rn>1--intCandidateId = 38603

, CandidateMainEmail as (select intCandidateId
, case	when rn = 1 then email
		else concat('NJFGTP_',rn,'_',email) end as CandidateEmail
, rn
from EmailDupRegconition)
--select * from CandidateMainEmail where CandidateEmail like '%a.novikovs@gmail.com'

--NOTE: must remove the first dot in the email of this candidate in the excel file: select CandidateEmail, right(CandidateEmail,len(CandidateEmail)-1) from CandidateEmail where intCandidateId = 39740--rn>1

-------------------------------------------------------------CALL BACKS
, tempCallBack as (
	select cb.intCandidateId, intCandidateCallBackId, cb.vchCallBackDetail, cb.bitActive, cb.datCallBackDate, cbt.vchCallBackTypeName
						, ROW_NUMBER() OVER(PARTITION BY cb.intCandidateId ORDER BY intCandidateCallBackId ASC) AS rn
	from dCandidateCallBack cb left join dCandidate c on cb.intCandidateId = c.intCandidateId
									left join refCallBackType cbt on cb.tintCallBackTypeId = cbt.tintCallBackTypeId
	where cb.bitActive = 1 and c.intCandidateId  is not null)

--select * from  tempCallBack
, tempCallBack1 as (select *, 
			concat(
	  iif(datCallBackDate is NULL,'',concat('--Call Back Date: ',datCallBackDate,char(10)))
	, iif(vchCallBackDetail = '' or vchCallBackDetail is NULL,'',concat('  Detail: ',vchCallBackDetail,char(10)))
	, iif(vchCallBackTypeName = '' or vchCallBackTypeName is NULL,'',concat('  Type: ',vchCallBackTypeName,char(10)))
	--, iif(bitActive = '' or bitActive is NULL,'',concat('  Flag (Active): ',bitActive,char(10)))
	--, concat('Entity: Contact', char(10))
	) as callBackInfo
from tempCallBack)

, CandiddateCallBack as (SELECT intCandidateId, 
     STUFF(
         (SELECT char(10) + callBackInfo
          from  tempCallBack1
          WHERE intCandidateId =cb1.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS callBackInfo
FROM tempCallBack1 as cb1
GROUP BY cb1.intCandidateId)
--select * from CandiddateCallBack

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
--SELECT intCandidateId, ac.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn,
--		 concat(ac.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		 as attachmentName
--from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
--where vchFileType not in ('.mp4'))-- and intCandidateId = 42147)

, tempCanAttachment as(
SELECT intCandidateId, ac.intAttachmentId--, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn
		,case when vchFileType like '.eml' then e.msgfilename 
		else
		 concat(ac.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 end as attachmentName
from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
							 left join email e on a.intAttachmentId = e.AttachmentID
where vchFileType not in ('.mp4')
union  --union with email files got from candidate events
select ec.intCandidateId, ae.intAttachmentId, em.msgfilename as attachmentName--, a.vchAttachmentName
from lEventCandidate ec left join dEvent e on ec.intEventId = e.intEventId
				--left join dCandidate c on ec.intCandidateId = c.intCandidateId
				left join lAttachmentEvent ae on ec.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
where em.AttachmentID is not null)

, canAttachment as (SELECT intCandidateId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempCanAttachment
          WHERE intCandidateId =ca.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS canAttachments
FROM tempCanAttachment as ca
GROUP BY ca.intCandidateId)

, tempCanDocuments as (select * from CanResumes union all select * from canAttachment)
--select * from tempCanDocuments
--select * from tempCan where ApplicantId = 142
, CanDocuments as (select intCandidateId, STUFF(
					(Select ',' + CVName
					from tempCanDocuments 
					where intCandidateId = tcd.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'CanDocs'
FROM tempCanDocuments as tcd
GROUP BY tcd.intCandidateId)
--select * from 
--select * from CanResumes
--select * from dCandidateCV
-----------------------------Get Attributes to add to Skill?
, temp_Attributes as (select intCandidateId, ac.intAttributeId, a.vchAttributeName, a.vchDescription, ROW_NUMBER() OVER(PARTITION BY ac.intCandidateId ORDER BY ac.intAttributeId ASC) AS rn
from lAttributeCandidate ac left join refAttribute a on ac.intAttributeId = a.intAttributeId)

, CanAttributes as (select intCandidateId, STUFF(
					(Select '; ' + vchAttributeName
					from temp_Attributes 
					where intCandidateId = ta.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'canAttributes'
FROM temp_Attributes as ta
GROUP BY ta.intCandidateId)
--select * from CanAttributes
------------------------------------------ Get Candidate Sources to Notes: Some candidates have more than 1 sources
, temp_Souces as (select intCandidateId, cs.sintSourceId, vchSourceName, cs.sdtAdded, cs.dtInserted, ROW_NUMBER() OVER(PARTITION BY cs.intCandidateId ORDER BY cs.sintSourceId ASC) AS rn
from dCandidateSource cs left join refSource s on cs.sintSourceId = s.sintSourceId
where cs.sintSourceId <> 0)

--, temp_Souces as (select intCandidateId, cs.sintSourceId, cs.sdtAdded, cs.dtInserted, ROW_NUMBER() OVER(PARTITION BY cs.intCandidateId ORDER BY cs.sintSourceId ASC) AS rn
--		, vchSourceName originalsource
--		, case 
--			when vchSourceName like 'Referral' then 'Referral'
--			when vchSourceName in ('Company','NJF Search') then 'Company Website'
--			when vchSourceName like 'Database' then 'Regenerated from Database'
--			when vchSourceName in ('By EMail','By Fax','By Post','Google','Google Search') then 'Headhunted'
--			when vchSourceName like 'LinkedIn' then 'Sourced - LinkedIn Free Account'
--			when vchSourceName like 'Sourced - LinkedIn Recruiter' then 'Sourced - LinkedIn Recruiter'
--			when vchSourceName like 'LinkedIn Job Response' then 'LinkedIn Job Response'
--			when vchSourceName like 'eFinancial Job Response' then 'eFinancial Job Response'
--			when vchSourceName like 'efinancialcareers' then 'Sourced - eFinancial Paid Account'
--			when vchSourceName like 'Bloomberg' then 'Bloomberg'
--			else 'Other' end as vchSourceName
--from dCandidateSource cs left join refSource s on cs.sintSourceId = s.sintSourceId
--where cs.sintSourceId <> 0)

, CanSources as (select intCandidateId, STUFF(
					(Select '; ' + vchSourceName
					from temp_Souces 
					where intCandidateId = ta.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'sources'
FROM temp_Souces as ta
GROUP BY ta.intCandidateId)
--select * from CanSources
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
--select * from CanOwners
----------------------------------Candidate Home Phone
, temp_homePhone as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as vchValue
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 1)

--Get all mobile to put to Notes
, temp_allHomePhone as (select intCandidateId, STUFF(
					(Select '; ' + replace(replace(vchValue,char(0x0002),''),char(0x0001),'')
					from temp_homePhone 
					where intCandidateId = thp.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS 'allHomePhone'
FROM temp_homePhone as thp
GROUP BY thp.intCandidateId)

, CanHomePhone as (select intCandidateId, vchValue from temp_homePhone where rn = 1)
--select * from CanHomePhone
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
--select * from CanMobile

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

---------------------------------Get Preferred phone to put to primary phone
, tempPreferredPhone as (select c.intCandidateId, iif(ct.vchValue <> '', concat(ct.vchValue,vchExtension), ct.vchValue) as vchValue
from dCandidate c left join dCandidateTelecom ct on c.intPreferredTelecomId = ct.intCandidateTelecomId
where intPreferredTelecomId is not null)

--------------------------------Get Skype
, Skype as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as skype
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 9)

--------------------------------Get Fax to Note
, Fax as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as fax
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 7)

--------------------------------Get Web to Note
, tempWeb as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as web
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 8)

, Web as (
select intCandidateId, STUFF(
					(Select ', ' + replace(replace(web,char(0x0002),''),char(0x0001),'')
					from tempWeb 
					where intCandidateId = tw.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS web
FROM tempWeb as tw
GROUP BY tw.intCandidateId)

----------------------GET HOME EMAILS TO NOTES
, tempHomeEmail as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as email
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 6)

, HomeEmail as (
select intCandidateId, STUFF(
					(Select ', ' + replace(replace(replace(email,char(0x0002),''),char(0x0001),''),'  ','')
					from tempHomeEmail 
					where intCandidateId = the.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS homeEmail
FROM tempHomeEmail as the
GROUP BY the.intCandidateId)

----------------------GET WORK EMAILS TO NOTES (ORIGINAL DATA FROM DB)
, tempOriginalWorkEmail as (
select intCandidateTelecomId, intCandidateId, vchDescription, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as email
	, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn
from dCandidateTelecom 
where tintTelecomId = 10)

, OriginalWorkEmail as (
select intCandidateId, STUFF(
					(Select ', ' + replace(replace(replace(email,char(0x0002),''),char(0x0001),''),'  ','')
					from tempOriginalWorkEmail 
					where intCandidateId = the.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS workEmail
FROM tempOriginalWorkEmail as the
GROUP BY the.intCandidateId)

---------------------GET WORK EMAILS TO WORK EMAIL FIELD
, tempWorkEmail as (
select ct.intCandidateTelecomId, ct.intCandidateId, vchValue as 'original-email'
							, case 
								when CHARINDEX(',',vchValue) = 1 then replace(vchValue,',','')
								when CHARINDEX(',',vchValue) <> 0 then left(vchValue,CHARINDEX(',',vchValue)-1)
								when CHARINDEX('/',vchValue) = 1 then replace(vchValue,'/','')
								when CHARINDEX('/',vchValue) <> 0 then left(vchValue,CHARINDEX('/',vchValue)-1)
								when CHARINDEX(';',vchValue) <> 0 then left(vchValue,CHARINDEX(';',vchValue)-1)
								when CHARINDEX('-',vchValue) = 1 then right(vchValue,len(vchValue)-1)
								when right(vchValue,1) = '.' then left(vchValue,len(vchValue)-1)
								--when CHARINDEX('.',vchValue) = 1 then right(vchValue,len(vchValue)-1)--replace(vchValue,'.','')--
								else vchValue end as email
							, vchForename, vchMiddlename, vchSurname, vchDescription,
		ROW_NUMBER() OVER(PARTITION BY ct.intCandidateId ORDER BY intCandidateTelecomId ASC) AS rn--NO CANDIDATE HAVE MORE THAN 1 WORK EMAILS
from dCandidateTelecom  ct left join dCandidate c on ct.intCandidateId = c.intCandidateId
						left join dPerson p on c.intPersonId = p.intPersonId
where vchValue like '%_@_%.__%' and tintTelecomId = 10)

--select * from tempWorkEmail where rn>1
, WorkEmail as (
select intCandidateId, STUFF(
					(Select ', ' + ltrim(rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(email,'''',''),'$',''),':',''),'?',''),'~',''),' ',''),'|',''),'[',''),']',''),'mailto',''),'(',''),')',''),'<',''),'---thisone',''),'----',''),'!',''),' - new email add?',''))) as workEmail
					from tempWorkEmail 
					where intCandidateId = twe.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS workEmail
FROM tempWorkEmail as twe
GROUP BY twe.intCandidateId)

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
--insert into importCandidate
select concat('NJFGTP', c.intCandidateId) as 'candidate-externalId', p.intPersonId  as PersonId--just for reference afterward
, iif(rtrim(ltrim(p.vchForename)) = '' or rtrim(ltrim(p.vchForename)) is null, concat('NoFirstname-', c.intCandidateId), rtrim(ltrim(p.vchForename))) as 'candidate-firstName'
, iif(rtrim(ltrim(p.vchSurName)) = '' or rtrim(ltrim(p.vchSurName)) is null, concat('NoLastname-', c.intCandidateId), rtrim(ltrim(p.vchSurName))) as 'candidate-Lastname'
, iif(rtrim(ltrim(p.vchMiddlename)) = '' or rtrim(ltrim(p.vchMiddlename)) is null, '', rtrim(ltrim(p.vchMiddlename))) as 'candidate-Middlename'
, iif(cme.CandidateEmail = '' or cme.CandidateEmail is NULL,concat('NJFGTP_ID',c.intCandidateId,'@noemail.com'),cme.CandidateEmail) as 'candidate-email'
, iif(we.workEmail = '' or we.workEmail is NULL,'',we.workEmail) as 'candidate-workEmail'--a lot of email has incorrect format, so if these candidates are skipped importing, remove work email
, convert(varchar(10),p.dDOB,120) as 'candidate-dob'
, upper(t.vchTitleName) as 'candidate-title'
, case
	when p.tintGenderId = 1 then 'MALE'
	when p.tintGenderId = 2 then 'FEMALE'
	else '' end as 'candidate-gender'
, replace(coalesce(pp.vchValue, cm.vchValue, hp.vchValue),',','') as 'candidate-phone'
, replace(cm.vchvalue,',','') as 'candidate-mobile'
, wp.vchValue as 'candidate-workPhone'
, hp.vchValue as 'candidate-homephone'
, iif(p.vchLinkedInUrl like '%linkedin%',p.vchLinkedInUrl,'') as 'candidate-linkedin'
, s.skype as 'candidate-skype'
--, left(c.vchComment,32000) as 'candidate-comments'--no more support importing comment
, iif(right(cl.fullAddress,1)= ',',left(replace(cl.fullAddress,',,',','),len(cl.fullAddress)-1),replace(cl.fullAddress,',,',',')) as 'candidate-address'
, iif(cl.vchTown = '' or cl.vchTown is null, '', cl.vchTown) as 'candidate-city'
, iif(cl.vchCounty = '' or cl.vchCounty is null, '', cl.vchCounty) as 'candidate-state'
, iif(cl.vchPostcode = '' or cl.vchPostcode is null, '', cl.vchPostcode) as 'candidate-zipCode'
, iif(cl.sintCountryId <> 0, cl.vchCountryCode, iif(cl.fullAddress like '%London%' or cl.fullAddress like '%Oxford%' or cl.fullAddress like '%UK%', 'GB', iif(cl.fullAddress like '%New York%' or cl.fullAddress like '%NYC%','US',''))) as 'candidate-Country'
, co.canOwners as 'candidate-owners'
--, coalesce(rc.vchCountryCode,rc1.vchCountryCode) as 'candidate-citizenship': wrong table to join: get the correct table refNationality
--select distinct sintNationalityId, sintNationality1Id, vchNationalityName
--from refNationality left join dPerson p on sintNationalityId = sintNationality1Id
--where sintNationality1Id is not null
--order by sintNationalityId
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
, coalesce(pce.vchExplicitCompanyName, pce.vchCompanyName) as 'candidate-employer1'
, cwh.WorkHistory as 'candidate-workHistory'
--, cr.CVName as 'candidate-resumes'
, iif(len(cd.CanDocs)>32000,'',cd.CanDocs) as 'candidate-resume'
, catt.canAttributes as 'candidate-skills'
--, ccb.callBackInfo : just for testing
, left(
	concat('Candidate External ID (NJF GTP): NJFGTP',c.intCandidateId
	, concat(char(10), char(10),'Voyager Candidate Code: ',c.vchStandardRefCode)
	,iif(rcs.vchCandidateStatusName = '' or rcs.vchCandidateStatusName is NULL,'',concat(char(10), char(10),'Candidate Status: ',rcs.vchCandidateStatusName))--use this for njf search and njf gtp
	--,iif(c.bitActivelyLooking <> 1,'',concat(char(10), char(10),'Candidate Status: Actively looking'))--use this for njf contracts db
	,iif(p.vchKnownAs = '' or p.vchKnownAs is null,'',concat(char(10), char(10),'Known As: ',p.vchKnownAs))
	,iif(rc2.vchCountryName = '' or rc2.vchCountryName is NULL,'',concat(char(10), char(10),'Place Of Birth: ',rc2.vchCountryName))
	,iif(tahp.allHomePhone = '' or tahp.allHomePhone is NULL,'',concat(char(10), char(10),'Home Phone(s): ',tahp.allHomePhone))
	,iif(tam.allMobile = '' or tam.allMobile is NULL,'',concat(char(10), char(10),'Mobile(s): ', tam.allMobile))
	,iif(he.homeEmail = '' or he.homeEmail is NULL,'',concat(char(10), char(10),'Home Email(s): ',he.homeEmail))
	,iif(owe.workEmail = '' or owe.workEmail is NULL,'',concat(char(10), char(10),'Work Email: ',owe.workEmail))
	,iif(f.fax = '' or f.fax is NULL,'',concat(char(10), char(10),'Fax: ', f.fax))
	,iif(w.web = '' or w.web is NULL,'',concat(char(10), char(10),'Website: ', w.web))
	,iif(c.vchBasedIn = '' or c.vchBasedIn is NULL,'',concat(char(10), char(10),'Based In: ', c.vchBasedIn))
	--,concat(char(10), char(10),'Relocated Flag: ', c.bitRelocate)
	,iif(rms.vchMaritalStatusName = '' or rms.vchMaritalStatusName is NULL,'',concat(char(10), char(10),'Marital Status: ', rms.vchMaritalStatusName))
	,iif(csrc.sources = '' or csrc.sources is NULL,'',concat(char(10), char(10),'Candidate Source(s): ',csrc.sources))
	,iif(npi.vchNoticePeriodIntervalName = '','',concat(char(10), char(10),'Notice Period Interval: ',npi.vchNoticePeriodIntervalName))
	,iif(c.tintNoticePeriodQuantity = 0,'',concat(char(10), char(10),'Notice Period Quantity: ', c.tintNoticePeriodQuantity))
	,iif(ccb.CallBackInfo = '' or ccb.CallBackInfo is NULL,'',concat(char(10), char(10),'Call Back Records: ', char(10),ccb.CallBackInfo))
	,iif(c.vchSummary = '' or c.vchSummary is NULL,'',concat(char(10), char(10), 'Summary: ',char(10), c.vchSummary, ''))
	,iif(c.vchComment = '' or c.vchComment is NULL,'',concat(char(10), char(10), 'Comment: ',char(10), c.vchComment, ''))
	),32000) as 'candidate-note'--, vchForename, vchSurname
from dCandidate c
				left join dPerson p on c.intPersonId = p.intPersonId
				left join CandidateMainEmail cme on c.intCandidateId = cme.intCandidateId
				left join refTitle t on p.tintTitleId = t.tintTitleId
				left join candidateLocation cl on c.intCandidateId = cl.intCandidateId
				left join CandiddateCallBack ccb on c.intCandidateId = ccb.intCandidateId
				--left join CanResumes cr on c.intCandidateId = cr.intCandidateId
				left join localCanDocuments cd on c.intCandidateId = cd.intCandidateId
				left join Temp_Candidate_WorkHistory cwh on c.intCandidateId = cwh.intCandidateId
				left join CanOwners co on c.intCandidateId = co.intCandidateId
				left join refCandidateStatus rcs on c.tintCandidateStatusId = rcs.tintCandidateStatusId--njf contract has no status field
				left join refNationality rn on p.sintNationality1Id =rn.sintNationalityId
				-- left join refCountry rc on p.sintNationality1Id = rc.sintCountryId
				-- left join refCountry rc1 on p.sintNationality4Id = rc1.sintCountryId
				left join refCountry rc2 on p.sintCountryOfOriginId = rc2.sintCountryId
				left join WorkPhone wp on c.intCandidateId = wp.intCandidateId --and wp.rn = 1 --refer from sTelecom--There is only 1 candidate has 2 work phone so combine as 1
				left join CanHomePhone hp on c.intCandidateId = hp.intCandidateId
				left join temp_allHomePhone tahp on c.intCandidateId = tahp.intCandidateId
				left join CanMobile cm on c.intCandidateId = cm.intCandidateId
				left join temp_allMobile tam on c.intCandidateId = tam.intCandidateId
				left join tempPreferredPhone pp on c.intCandidateId = pp.intCandidateId
				left join refMaritalStatus rms on p.tintMaritalStatusId = rms.tintMaritalStatusId
				left join CanSources csrc on c.intCandidateId = csrc.intCandidateId
				left join refNoticePeriodInterval npi on c.tintNoticePeriodIntervalId = npi.tintNoticePeriodIntervalId
				left join dPersonCurrentEmployment pce on p.intPersonId = pce.intPersonId
				left join CanAttributes catt on c.intCandidateId = catt.intCandidateId
				left join Skype s on c.intCandidateId = s.intCandidateId
				left join Fax f on c.intCandidateId = f.intCandidateId
				left join Web w on c.intCandidateId = w.intCandidateId
				left join HomeEmail he on c.intCandidateId = he.intCandidateId
				left join WorkEmail we on c.intCandidateId = we.intCandidateId
				left join OriginalWorkEmail owe on c.intCandidateId = owe.intCandidateId
--where c.intCandidateId in (select intCandidateId from temp_Can) --and cd.CanDocs is not null
--where cme.CandidateEmail = 'a.novikovs@gmail.com'
--where c.intCandidateId = 1197
--where len(cd.CanDocs)>32000
--where vchforename = 'Ajay' and vchSurname = 'Pandey'--this guy has skype info
--where c.intCandidateId in (48445,31880,44007,15490,44296,16796,38402,44798,44982,38455,15944,45834,45826,10987,22607,11200,52754,53354,19816,40589,44976,45050,44823,3334,44314,45794,22999,51044,2732,52304,44988)
--order by c.intCandidateId