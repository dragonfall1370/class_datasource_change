with tempContacts as (select c.intContactId, c.vchStandardRefCode, c.intPersonId, intCompanyTierContactId, ctc.intCompanyTierId, ct.intCompanyId, ctc.vchJobTitle, c.datLastContacted, c.bitFestiveGreetingsCard
		, ctc.vchNote, ctc.bitActive, ctc.intPreferredTelecomId, ROW_NUMBER() OVER(PARTITION BY ctc.intContactId ORDER BY intCompanyId ASC) AS rn
from dContact c left join lCompanyTierContact ctc on c.intContactId = ctc.intContactId
				left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
where ctc.intContactId is not null)

, ContactWorkPhone as (select tc.intCompanyTierContactId, ct.intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as workPhone
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 2 and vchValue not like '%@%')

, ContactHomePhone as (select tc.intCompanyTierContactId, ct.intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as homePhone
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 3)

, ContactworkEmail as (select tc.intCompanyTierContactId, ct.intContactId,vchValue as workEmail
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 10)

, ContactWebsite as (select tc.intCompanyTierContactId, ct.intContactId,vchValue as web
from dContactTelecom ct left join tempContacts tc on ct.intContactId = tc.intContactId
where tintTelecomId = 8)

, tempCallBack as (select intCompanyTierContactId, cb.intContactId, cb.intCompanyTierId, cb.vchCallBackDetail, cb.bitActive, cb.datCallBackDate, cbt.vchCallBackTypeName
						, ROW_NUMBER() OVER(PARTITION BY intCompanyTierContactId ORDER BY intCompanyTierContactCallBackId ASC) AS rn
from dCompanyTierContactCallBack cb left join lCompanyTierContact tc on cb.intContactId = tc.intContactId and cb.intCompanyTierId = tc.intCompanyTierId
									left join refCallBackType cbt on cb.tintCallBackTypeId = cbt.tintCallBackTypeId)

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

, contactConsultant as (select intCompanyTierContactId, u.vchEmail as consultantEmail
from lConsultantCompanyTierContact cctc 
		left join lCompanyTierContact tc on cctc.intContactId = tc.intContactId and cctc.intCompanyTierId = tc.intCompanyTierId
		left join dUser u on cctc.intConsultantId = u.intUserId)

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


---------MAIN SCRIPT-------------
,test as (select 
iif(tc.intCompanyId = '' or tc.intCompanyId is NULL,'0',tc.intCompanyId) as 'contact-companyId'
, tc.intCompanyId as '(OriginalCompanyID)'
, com.vchCompanyName as '(OriginalCompanyName)'
, tc.intCompanyTierContactId as 'contact-externalId'
, che.workEmail as 'email'
, cwp.workPhone as 'contact-phone'
, iif(p.vchForename = '' or p.vchForename is NULL,concat('NoFirstname-', tc.intCompanyTierContactId),p.vchForename) as 'contact-firstName'
, iif(p.vchSurname = '' or p.vchSurname is NULL,concat('NoLastName-', tc.intCompanyTierContactId),p.vchSurname) as 'contact-lastName'
, iif(p.vchMiddlename = '' or p.vchMiddlename is NULL,'',p.vchMiddleName) as 'contact-middleName'
, iif(p.vchLinkedInUrl like '%linkedin%',p.vchLinkedInUrl,'') as 'contact-linkedin'
, vchJobTitle as 'contact-jobTitle'
, ccst.consultantEmail as 'contact-owners'
--, left(ctc.AllComment,32000) as 'contact-comment'
, left(
	concat('Contact External ID: ',tc.intCompanyTierContactId,char(10)
	, concat(char(10),'Voyager Contact Code: ',tc.vchStandardRefCode,char(10))
	, iif(tc.bitFestiveGreetingsCard = '0','',concat(char(10),'Festive Greetings Card Flag: ','Yes',char(10)))
	, iif(rt.vchTitleName = '' or rt.vchTitleName is NULL,'',concat(char(10),'Title: ',rt.vchTitleName,char(10)))
	, iif(p.vchKnownAs = '' or p.vchKnownAs is NULL,'',concat(char(10),'Known as: ',p.vchKnownAs,char(10)))
	, iif(cwp.workPhone = '' or cwp.workPhone is NULL,'',concat(char(10),'Work Phone: ',cwp.workPhone,char(10)))
	, iif(chp.homePhone = '' or chp.homePhone is NULL,'',concat(char(10),'Home Phone: ',chp.homePhone,char(10)))
	, iif(che.workEmail = '' or che.workEmail is NULL,'',concat(char(10),'Home Email Address: ',che.workEmail,char(10)))
	, iif(cw.web = '' or cw.web is NULL,'',concat(char(10),'Website: ',cw.web,char(10)))
	, iif(tc.datLastContacted is NULL,'',concat(char(10),'Last Contacted Date: ',tc.datLastContacted,char(10)))
	, iif(tc.bitActive = 0,concat(char(10),'Active/Inactive: Inactive (0)',char(10)), concat(char(10),'Active/Inactive: Active (1)',char(10)))
	, iif(cl.locationInfo = '' or cl.locationInfo is null, '', concat(char(10),'LOCATION INFO:',char(10),cl.locationInfo))
	, iif(ccb.callBackInfo = '' or ccb.callBackInfo is null, '', concat(char(10),'CALL BACK INFO:',char(10),ccb.callBackInfo,char(10)))
	, iif(tc.vchNote = '' or tc.vchNote is NULL,'',concat(char(10),'Notes: ',char(10),tc.vchNote))
	),32000) as 'contact-note'
	, ROW_NUMBER() over (partition by che.workEmail order by che.workEmail) as rn
from tempContacts tc left join dPerson p on tc.intPersonId = p.intPersonId
					 left join dCompany com on tc.intCompanyId = com.intCompanyId
					 --left join dupPhone dp on tc.intCompanyTierContactId = dp.intCompanyTierContactId
					 left join refTitle rt on p.tintTitleId = rt.tintTitleId
					 left join contactCallBack ccb on tc.intCompanyTierContactId = ccb.intCompanyTierContactId
					 left join contactConsultant ccst on tc.intCompanyTierContactId = ccst.intCompanyTierContactId
					 left join contactLocation cl on tc.intCompanyTierContactId = cl.intCompanyTierContactId
					 left join ContactWorkPhone cwp on tc.intCompanyTierContactId = cwp.intCompanyTierContactId
					 left join ContactHomePhone chp on tc.intCompanyTierContactId = chp.intCompanyTierContactId
					 left join ContactworkEmail che on tc.intCompanyTierContactId = che.intCompanyTierContactId
					 left join ContactMobile cm on tc.intCompanyTierContactId = cm.intCompanyTierContactId
					 left join ContactWebsite cw on tc.intCompanyTierContactId = cw.intCompanyTierContactId)


					 select iif(email = '' or email is null,'',iif(rn = 1,email,concat(rn,'-',email))) as 'contact-email',* from test

