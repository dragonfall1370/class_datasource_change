with tempContacts as (select c.intContactId, c.vchStandardRefCode, c.intPersonId, intCompanyTierContactId, ctc.intCompanyTierId, ct.intCompanyId, ctc.vchJobTitle, c.datLastContacted
		, ctc.vchNote, ctc.bitActive, ctc.intPreferredTelecomId, ROW_NUMBER() OVER(PARTITION BY ctc.intContactId ORDER BY intCompanyId ASC) AS rn
from dContact c left join lCompanyTierContact ctc on c.intContactId = ctc.intContactId
				left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
where ctc.intContactId is not null)
--select * from tempContacts tc where tc.intCompanyId in (5559,6254) or tc.intCompanyTierContactId in (33316,80764,42925,63799,66515,64760,64117,63444,22063,65710,71772,72976,72624,73326,76366,79166,74484,73031,83887)

--GET CONTACT EMAILS FROM DCOMPANYTIERCONTACTTELECOM
, tempContactEmail0 as (select intContactId, vchValue
from dCompanyTierContactTelecom
where (vchDescription = 'Email1' or vchDescription = 'Email2') and vchValue like '%_@_%.%' and intCompanyTierContactTelecomId <> 60173)

, tempContactEmail1 as (SELECT intContactId, 
     STUFF(
         (SELECT ',' + vchValue--replace & and / with , on NJF GTP, replace space with , on NJF Contracts
          from  tempContactEmail0
          WHERE intContactId = T.intContactId
    order by intContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS vchValue
FROM tempContactEmail0 as T
GROUP BY T.intContactId)

--select * from tempContactEmail1
--GET CONTACT EMAILS from dContactTelecom
, tempContactEmail2 as(
select intContactId, intContactTelecomId, replace(replace(replace(replace(replace(replace(vchValue,':',''),'''',''),'+','-'),' ',''),'>',''),',','') as vchValue
, ROW_NUMBER() OVER(PARTITION BY intContactId ORDER BY intContactTelecomId ASC) AS rn
, ROW_NUMBER() OVER(PARTITION BY vchValue ORDER BY intContactTelecomId ASC) AS rn1
from dContactTelecom
where tintTelecomId = 10 and intContactId not in (select intContactId from tempContactEmail1)
)

--COMBINE EMAILS OF CONTACTS HAVE MUTLIPLE EMAIL ADDRESSES --2 CONTACTS
, tempContactEmail3 as (SELECT intContactId, 
     STUFF(
         (SELECT ',' + vchValue
          from  tempContactEmail2
          WHERE intContactId = T.intContactId
    order by intContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS vchValue
FROM tempContactEmail2 as T
GROUP BY T.intContactId)

--UNION 2 TABLES
, tempcontactEmail4 as (select * from tempContactEmail1 union all select * from tempContactEmail3)

, temptempcontactEmail4_1 as (select *
	,replace(replace(replace(replace(replace(replace(iif(CHARINDEX('[',vchValue)>0,right(vchValue,len(vchValue)-CHARINDEX('[',vchValue)),vchValue),']',''),'mailto:',''),'<',''),'>',''),'/',','),' ','') as vchValue1 
from tempcontactEmail4
)

--RECOGNITE CONTACTS HAVE THE SAME EMAIL ADDRESSES
, tempContactEmail5 as (select tc.intCompanyTierContactId, tce.intContactId, tce.vchValue1 as email, ROW_NUMBER() OVER(PARTITION BY tce.vchValue1 ORDER BY tce.intContactId ASC) AS rn
from temptempcontactEmail4_1 tce left join tempContacts tc on tce.intContactId = tc.intContactId)

, ContactEmail as (select intCompanyTierContactId, intContactId,
case 
when rn=1 then email
else concat('NJFS_',rn,'_',(email))
end as Email
from tempContactEmail5)
--select * from ContactEmail

----------------------------------GET MOBILE
--GET CONTACT MOBILES FROM DCOMPANYTIERCONTACTTELECOM
, tempMobile1 as (select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as mobile
from dCompanyTierContactTelecom
where vchDescription = 'Mobile Phone')

--GET CONTACT MOBILES FROM dContactTelecom
, tempMobile2 as(
select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as mobile
from dContactTelecom
where tintTelecomId = 5 and intContactId not in (select intContactId from tempMobile1)
)

--UNION 2 TABLES
, tempMobile3 as (select * from tempMobile1 union all select * from tempMobile2)

--LINK MOBILE WITH companytiercontactId
, ContactMobile as (select tc.intCompanyTierContactId, tce.*
from tempMobile3 tce left join tempContacts tc on tce.intContactId = tc.intContactId)

----------------------------------check phone
--, dupPhone as (select tc.intCompanyTierContactId, ctct.intContactId, 
--vchDescription,iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as phone, intCompanyTierContactTelecomId, ctct.intCompanyTierId id1, tc.intCompanyTierId, ROW_NUMBER() OVER(PARTITION BY ctct.intContactId ORDER BY tc.intCompanyTierContactId ASC) AS rn
--from dCompanyTierContactTelecom ctct left join tempContacts tc on ctct.intContactId = tc.intContactId
--where vchDescription = 'DDI' and ctct.intCompanyTierId = tc.intCompanyTierId)
--select * from dupPhone where rn>1
--select * from tempContacts where intCompanyId = '' or intCompanyId is null

----------------------------------GET DDI
--GET DDI from dCompanyTierContactTelecom
, tempDDI1 as (select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as phone
from dCompanyTierContactTelecom
where vchDescription = 'DDI')

--GET DDI from dContactTelecom
, tempDDI2 as(
select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as phone
from dContactTelecom
where tintTelecomId = 3 and intContactId not in (select intContactId from tempDDI1)
)

--UNION 2 TABLES
, tempDDI3 as (select * from tempDDI1 union all select * from tempDDI2)

--JOIN DDI with companytiercontactId
, ContactDDI as (select tc.intCompanyTierContactId, ddi.*
from tempDDI3 ddi left join tempContacts tc on ddi.intContactId = tc.intContactId)

----------------------------Get Work phone to Note
, ContactWorkPhone as (select tc.intCompanyTierContactId, ct.intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as workPhone
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 2 and vchValue not like '%@%')

----------------------------Get Home phone to Note
, ContactHomePhone as (select tc.intCompanyTierContactId, ct.intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as homePhone
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 1)

----------------------------Get Home Email to Note
, ContactHomeEmail as (select tc.intCompanyTierContactId, ct.intContactId,vchValue as homeEmail
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 6)

----------------------------------GET Fax to Note
--GET Fax from dCompanyTierContactTelecom
, tempFax1 as (select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as fax
from dCompanyTierContactTelecom
where vchDescription = 'Fax')

--GET Fax from dContactTelecom
, tempFax2 as(
select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as fax
from dContactTelecom
where tintTelecomId = 7 and intContactId not in (select intContactId from tempFax1)
)

--UNION 2 TABLES
, tempFax3 as (select * from tempFax1 union all select * from tempFax2)

--JOIN Fax with companyTierContactId
, ContactFax as (select tc.intCompanyTierContactId, Fax.*
from tempFax3 Fax left join tempContacts tc on Fax.intContactId = tc.intContactId)

----------------------------Get Skype--NJF search has no contact has skype
, ContactSkype as (select tc.intCompanyTierContactId, ct.intContactId,vchValue as skype
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 9)

----------------------------Get Website to Note
, ContactWebsite as (select tc.intCompanyTierContactId, ct.intContactId,vchValue as web
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 8)

----------------------------Get contact attribute to add to Note
, tempAttribute as (select intCompanyTierContactId, actc.intContactId, actc.intCompanyTierId, actc.intAttributeId, ra.vchAttributeName, actc.sintAttributeScoreId, ras.vchAttributeScoreName, ras.vchDescription
from lAttributeCompanyTierContact actc left join lCompanyTierContact tc on actc.intContactId = tc.intContactId and actc.intCompanyTierId = tc.intCompanyTierId
		left join refAttribute ra on actc.intAttributeId = ra.intAttributeId
		left join refAttributeScore ras on actc.sintAttributeScoreId = ras.sintAttributeScoreId)
, tempAttribute1 as (select *, 
			concat(
	  iif(vchAttributeName = '' or vchAttributeName is NULL,'',concat('--Attribute Name: ',vchAttributeName,char(10)))
	, iif(vchAttributeScoreName = '' or vchAttributeScoreName is NULL,'',concat('  Score: ',vchAttributeScoreName, iif(vchDescription = '' or vchDescription is null, '',concat(' (',vchDescription,')')),char(10)))) as attinfo
from tempAttribute)
, contactAttribute as (SELECT intCompanyTierContactId, 
     STUFF(
         (SELECT char(10) + attinfo
          from  tempAttribute1
          WHERE intCompanyTierContactId =ta1.intCompanyTierContactId
    order by intCompanyTierContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS attinfo
FROM tempAttribute1 as ta1
GROUP BY ta1.intCompanyTierContactId)

----------Get call back info: due date, for, details, type, preferred telecom, flag, against, entity (contact)
, tempCallBack as (select intCompanyTierContactId, cb.intContactId, cb.intCompanyTierId, cb.vchCallBackDetail, cb.bitActive, cb.datCallBackDate, cbt.vchCallBackTypeName
						, ROW_NUMBER() OVER(PARTITION BY intCompanyTierContactId ORDER BY intCompanyTierContactCallBackId ASC) AS rn
from dCompanyTierContactCallBack cb left join lCompanyTierContact tc on cb.intContactId = tc.intContactId and cb.intCompanyTierId = tc.intCompanyTierId
									left join refCallBackType cbt on cb.tintCallBackTypeId = cbt.tintCallBackTypeId
		)

--select distinct intCompanyTierContactId from tempJobTitle
, tempCallBack1 as (select *, 
			concat(
	  iif(datCallBackDate is NULL,'',concat('--Call Back Date: ',datCallBackDate,char(10)))
	, iif(vchCallBackDetail = '' or vchCallBackDetail is NULL,'',concat('  Detail: ',vchCallBackDetail,char(10)))
	, iif(vchCallBackTypeName = '' or vchCallBackTypeName is NULL,'',concat('  Type: ',vchCallBackTypeName,char(10)))
	, iif(bitActive = '' or bitActive is NULL,'',concat('  Flag (Active): ',bitActive,char(10)))
	, concat('Entity: Contact', char(10))
	) as callBackInfo
from tempCallBack)

, contactCallBack as (SELECT intCompanyTierContactId, 
     STUFF(
         (SELECT char(10) + callBackInfo
          from  tempCallBack1
          WHERE intCompanyTierContactId =cb1.intCompanyTierContactId
    order by intCompanyTierContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS callBackInfo
FROM tempCallBack1 as cb1
GROUP BY cb1.intCompanyTierContactId)

-----Get contact consultant
, contactConsultant as (select intCompanyTierContactId, u.vchEmail as consultantEmail
from lConsultantCompanyTierContact cctc 
		left join lCompanyTierContact tc on cctc.intContactId = tc.intContactId and cctc.intCompanyTierId = tc.intCompanyTierId
		left join dUser u on cctc.intConsultantId = u.intUserId)

-----------------------Get Contact Location
--get location for each company Tier
, TierPhone1 as (select intCompanyTierTelecomId, intCompanyTierId, iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber) as vchNumber
from dCompanyTierTelecom where vchNumber is not null and vchNumber <> '' and (vchDescription = 'Location Tel No' or vchDescription = ''))

, TierPhone as (SELECT intCompanyTierId, 
     STUFF(
         (SELECT ',' + vchNumber
          from  TierPhone1
          WHERE intCompanyTierId = T.intCompanyTierId
    order by intCompanyTierId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS vchNumber
FROM TierPhone1 as T
GROUP BY T.intCompanyTierId)

, tempLoc1 as (select  intCompanyTierContactId, vchNumber
from lCompanyTierContact ctc left join TierPhone ctct on ctc.intCompanyTierId = ctct.intCompanyTierId)
-- and ctc.intContactId = ctct.intContactId where ctct.vchDescription = 'DDI')

, tempLoc2 as (select ctc.intCompanyTierContactId, ctc.intCompanyTierId, ctc.intContactId, ct.vchCompanyTierName, ct.vchTown, vchNumber as phone
from lCompanyTierContact ctc left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
							 left join tempLoc1 tl on ctc.intCompanyTierContactId = tl.intCompanyTierContactId)

, contactLocation as (select intCompanyTierContactId, intCompanyTierId,
			concat(
				   iif(intCompanyTierId = '' or intCompanyTierId is NULL,'',concat('--Location ID: ',intCompanyTierId,char(10)))
				 , iif(vchCompanyTierName = '' or vchCompanyTierName is NULL,'',concat('  Location Name: ',vchCompanyTierName,char(10)))
				 , iif(vchTown = '' or vchTown is NULL,'',concat('  City: ',vchTown,char(10)))
				 , iif(phone = '' or phone is NULL,'',concat('  Location Phone: ',phone,char(10)))
				) as locationInfo
from tempLoc2)

-----------------Get Attachment
--, tempConAttachment as(
--SELECT intCompanyTierContactId, actc.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCompanyTierContactId ORDER BY actc.intAttachmentId ASC) AS rn,
--		 concat(actc.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		 as attachmentName
--from lAttachmentCompanyTierContact actc left join dAttachment a on actc.intAttachmentId = a.intAttachmentId
--	 left join lCompanyTierContact ctc on actc.intContactId = ctc.intContactId and actc.intCompanyTierId = ctc.intCompanyTierId
--	 where vchFileType not in ('.mp4'))

, tempConAttachment as(
SELECT intCompanyTierContactId, actc.intAttachmentId--, ROW_NUMBER() OVER(PARTITION BY intCompanyTierContactId ORDER BY actc.intAttachmentId ASC) AS rn
		,case when vchFileType like '.eml' then e.msgfilename 
		else 
		 concat(actc.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		end as attachmentName
from lAttachmentCompanyTierContact actc left join dAttachment a on actc.intAttachmentId = a.intAttachmentId
	 left join lCompanyTierContact ctc on actc.intContactId = ctc.intContactId and actc.intCompanyTierId = ctc.intCompanyTierId
	 left join email e on a.intAttachmentId = e.AttachmentID
	 where vchFileType not in ('.mp4')
union
select ctc.intCompanyTierContactId, ae.intAttachmentId, em.msgfilename as attachmentName
from lEventCompanyTierContact ectc left join dEvent e on ectc.intEventId = e.intEventId
				left join lCompanyTierContact ctc on ectc.intContactId = ctc.intContactId and ectc.intCompanyTierId = ctc.intCompanyTierId
				--left join dCandidate c on ectc.intCandidateId = c.intCandidateId
				left join lAttachmentEvent ae on ectc.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
where em.AttachmentID is not null)

, conAttachment as (SELECT intCompanyTierContactId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempConAttachment
          WHERE intCompanyTierContactId =ca.intCompanyTierContactId
    order by intCompanyTierContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS contactAttachments
FROM tempConAttachment as ca
GROUP BY ca.intCompanyTierContactId)

---------MAIN SCRIPT
--, main as (
--insert into importContact 
select 
iif(tc.intCompanyId = '' or tc.intCompanyId is NULL,'NJFS9999999',concat('NJFS',tc.intCompanyId)) as 'contact-companyId'
, tc.intCompanyId as '(OriginalCompanyID)'
, com.vchCompanyName as '(OriginalCompanyName)'
, concat('NJFS',tc.intCompanyTierContactId) as 'contact-externalId'
, iif(p.vchForename = '' or p.vchForename is NULL,concat('NoFirstname-', tc.intCompanyTierContactId),p.vchForename) as 'contact-firstName'
, iif(p.vchSurname = '' or p.vchSurname is NULL,concat('NoLastName-', tc.intCompanyTierContactId),p.vchSurname) as 'contact-lastName'
, iif(p.vchMiddlename = '' or p.vchMiddlename is NULL,'',p.vchMiddleName) as 'contact-middleName'
, iif(p.vchLinkedInUrl like '%linkedin%',p.vchLinkedInUrl,'') as 'contact-linkedin'
, vchJobTitle as 'contact-jobTitle'
, ce.Email as 'contact-email'
, cs.skype as 'contact-skype'
, coalesce(cm.mobile,ddi.phone,cwp.workPhone) as 'contact-phone'--prior to mobile, then ddi, then workphone for contact's primary phone
, ccst.consultantEmail as 'contact-owners'
, iif(len(cam.contactAttachments)>32000,'',cam.contactAttachments) as 'contact-document'
--, left(ctc.AllComment,32000) as 'contact-comment'
, left(
	concat('Contact External ID: NJFS',tc.intCompanyTierContactId,char(10)
	, concat(char(10),'Voyager Contact Code: ',tc.vchStandardRefCode,char(10))
	, iif(rt.vchTitleName = '' or rt.vchTitleName is NULL,'',concat(char(10),'Title: ',rt.vchTitleName,char(10)))
	, iif(p.vchKnownAs = '' or p.vchKnownAs is NULL,'',concat(char(10),'Known as: ',p.vchKnownAs,char(10)))
	, iif(ddi.phone = '' or ddi.phone is NULL,'',concat(char(10),'DDI: ',ddi.phone,char(10)))
	, iif(cwp.workPhone = '' or cwp.workPhone is NULL,'',concat(char(10),'Work Phone: ',cwp.workPhone,char(10)))
	, iif(chp.homePhone = '' or chp.homePhone is NULL,'',concat(char(10),'Home Phone: ',chp.homePhone,char(10)))
	, iif(che.homeEmail = '' or che.homeEmail is NULL,'',concat(char(10),'Home Email Address: ',che.homeEmail,char(10)))
	, iif(cf.fax = '' or cf.fax is NULL,'',concat(char(10),'Fax: ',cf.fax,char(10)))
	, iif(cw.web = '' or cw.web is NULL,'',concat(char(10),'Website: ',cw.web,char(10)))
	, iif(tc.datLastContacted is NULL,'',concat(char(10),'Last Contacted Date: ',tc.datLastContacted,char(10)))
	, iif(tc.bitActive = 0,concat(char(10),'Active/Inactive: Inactive (0)',char(10)), concat(char(10),'Active/Inactive: Active (1)',char(10)))
	, iif(cl.locationInfo = '' or cl.locationInfo is null, '', concat(char(10),'LOCATION INFO:',char(10),cl.locationInfo))
	, iif(ca.attInfo = '' or ca.attInfo is null, '', concat(char(10),'ATTRIBUTE INFO:',char(10),ca.attInfo,char(10)))
	, iif(cjt.jobTitleInfo = '' or cjt.jobTitleInfo is null, '', concat(char(10),'JOB TITLES INFO:',char(10),cjt.jobTitleInfo,char(10)))
	, iif(ccb.callBackInfo = '' or ccb.callBackInfo is null, '', concat(char(10),'CALL BACK INFO:',char(10),ccb.callBackInfo,char(10)))
	, iif(tc.vchNote = '' or tc.vchNote is NULL,'',concat(char(10),'Notes: ',char(10),tc.vchNote))
	),32000) as 'contact-note'
from tempContacts tc left join dPerson p on tc.intPersonId = p.intPersonId
					 left join dCompany com on tc.intCompanyId = com.intCompanyId
					 left join ContactEmail ce on tc.intCompanyTierContactId = ce.intCompanyTierContactId
					 --left join dupPhone dp on tc.intCompanyTierContactId = dp.intCompanyTierContactId
					 left join refTitle rt on p.tintTitleId = rt.tintTitleId
					 left join Temp_contactAttribute ca on tc.intCompanyTierContactId = ca.intCompanyTierContactId
					 left join Temp_ContactJobTitle1 cjt on tc.intCompanyTierContactId = cjt.intCompanyTierContactId
					 left join contactCallBack ccb on tc.intCompanyTierContactId = ccb.intCompanyTierContactId
					 left join contactConsultant ccst on tc.intCompanyTierContactId = ccst.intCompanyTierContactId
					 left join contactLocation cl on tc.intCompanyTierContactId = cl.intCompanyTierContactId
					 left join conAttachment cam on tc.intCompanyTierContactId = cam.intCompanyTierContactId
					 left join ContactWorkPhone cwp on tc.intCompanyTierContactId = cwp.intCompanyTierContactId
					 left join ContactHomePhone chp on tc.intCompanyTierContactId = chp.intCompanyTierContactId
					 left join ContactHomeEmail che on tc.intCompanyTierContactId = che.intCompanyTierContactId
					 left join ContactDDI ddi on tc.intCompanyTierContactId = ddi.intCompanyTierContactId
					 left join ContactMobile cm on tc.intCompanyTierContactId = cm.intCompanyTierContactId
					 left join ContactSkype cs on tc.intCompanyTierContactId = cs.intCompanyTierContactId
					 left join ContactFax cf on tc.intCompanyTierContactId = cf.intCompanyTierContactId
					 left join ContactWebsite cw on tc.intCompanyTierContactId = cw.intCompanyTierContactId
--where len(cam.contactAttachments) > 32000
--where tc.intCompanyTierContactId = 71583-- and
--ce.Email like '%Katja.Braumoeller@drkw.com'
--where tc.intCompanyId in (5559,6254) or tc.intCompanyTierContactId in (33316,80764,42925,63799,66515,64760,64117,63444,22063,65710,71772,72976,72624,73326,76366,79166,74484,73031,83887)
--where tc.intCompanyId in (2,455) and p.vchforename = 'Abhi' and p.vchsurname = 'Shroff'
--tc.intCompanyTierContactId in (33316,80764, 72905, 33316)--,42925,63799,66515,64760,64117,63444,22063,65710,71772,72976,72624,73326,76366,79166,74484,73031,83887)
--where cam.contactAttachments is not null
UNION ALL
select 'NJFS9999999','','','NJFS9999999','NJF Search - Default','Contact','','','','','','','','','This is default contact from Data Import'