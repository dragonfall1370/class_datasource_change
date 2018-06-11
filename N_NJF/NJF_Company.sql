--GET COMPANY SWITCHBOARD
with compSwitchboard as(
select intCompanyId, vchValue, replace(iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber), char(0x0003),'') as switchBoard
, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY vchValue ASC) AS rn
from dCompanyTelecom 
where vchDescription = 'switchboard' or (tintTelecomId  = 4 and vchDescription <> 'Tel' and vchValue <> ''))

--Get Phone from dCompanyTelecom and dCompanyTierTelecom
--get phone from dCompanyTelecom
, tphone1 as (select intCompanyId, vchValue, replace(iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber), char(0x0003),'') as vchNumber
from dCompanyTelecom where vchDescription = 'Tel' or vchDescription = 'JK''s Number')
--where vchNumber not like '%http%' and replace(vchNumber, char(0x0003),'') <> '' and vchNumber not like '%@%' and vchNumber not like '%www%' and vchNumber not like '%.com%' and vchDescription <> 'Fax')
--Get phone from dCompanyTierTelecom
, tphone2 as (select ct.intCompanyId, vchValue, iif(vchExtension <> '', concat(vchNumber,vchExtension, ' (',ct.vchCompanyTierName, ')'), concat(vchNumber, ' (', ct.vchCompanyTierName, ')')) as vchNumber --ctt.intCompanyTierId, intCompanyTierTelecomId
				,  ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY vchNumber ASC) AS rn
from dCompanyTierTelecom ctt left join dCompanyTier ct on ctt.intCompanyTierId = ct.intCompanyTierId
where vchNumber is not null and vchNumber <> '' and (ctt.vchDescription = 'Location Tel No' or ctt.vchDescription = '')  and (ct.vchCompanyTierName = 'Main' or ct.vchCompanyTierName = 'Head Office'))
--Merge phone from 2 table
--, tphone3 as (select * from tphone1 union all select * from tphone2)
-- remove duplicate phone from the table after merging
--, tphone as (select distinct intCompanyId, vchNumber from tphone3)

--select distinct intCompanyId from  tphone
--combine phone
, mainTierPhone as (SELECT intCompanyId, 
     STUFF(
         (SELECT ',' + vchNumber
          from  tphone2
          WHERE intCompanyId = T.intCompanyId
    order by intCompanyId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS vchNumber
FROM tphone2 as T
GROUP BY T.intCompanyId)

, Phone as (select c.intCompanyId, coalesce(t1.vchNumber, mtp.vchNumber) as Num
from dCompany c left join tphone1 t1 on c.intCompanyId = t1.intCompanyId
left join mainTierPhone mtp on c.intCompanyId = mtp.intCompanyId)
------------------------------------------------------------------------------------------------
--Get Website
, web as (select intCompanyId,
	case when left(vchValue,2) = '//' then right(vchValue,len(vchValue)-2)
		 when left(vchValue,1) = '/' then right(vchValue,len(vchValue)-1)
	else vchValue end as web--, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY intCompanyTelecomId ASC) AS rn
from dCompanyTelecom
where vchValue <> ''and vchValue not like '%@%'  and (vchValue like '%.co%' or vchValue like '%www%'))

--GET FAX
, Fax as (select intCompanyId, vchValue as fax
from dCompanyTelecom where vchDescription = 'Fax')

--Duplicate Company Name
, dup as (select intCompanyId, vchCompanyName, ROW_NUMBER() OVER(PARTITION BY vchCompanyName ORDER BY intCompanyId ASC) AS rn
from dCompany)

-----------------------Get all company Location to Note
, TierPhone1 as (select intCompanyTierTelecomId, intCompanyTierId, iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber) as vchNumber
from dCompanyTierTelecom ctt
where vchNumber is not null and vchNumber <> '' and (vchDescription = 'Location Tel No' or vchDescription = ''))

, TierPhone as (SELECT intCompanyTierId, 
     STUFF(
         (SELECT ',' + vchNumber
          from  TierPhone1
          WHERE intCompanyTierId = T.intCompanyTierId
    order by intCompanyTierId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS Num
FROM TierPhone1 as T
GROUP BY T.intCompanyTierId)

, tempLocationNote as (select ct.intCompanyTierId, ct.intCompanyId--, ct.vchCompanyTierName, ct.vchNote as Description, replace(vchNumber, char(0x0003),'')
		, concat(
				iif(ct.vchCompanyTierName = '' or ct.vchCompanyTierName is null, '',concat('--Location Name: ', iif(right(ct.vchCompanyTierName,1) = ':',left(ct.vchCompanyTierName,len(ct.vchCompanyTierName)-1),ct.vchCompanyTierName))),
				iif(ctt.Num = '' or ctt.Num is null,'',concat(char(10),'   Phone: ',ctt.Num)),
				iif(ct.vchDescription = '' or ct.vchDescription is null, '', concat(char(10), '   Description: ',ct.vchDescription))
				) as 'locationName' 
from dCompanyTier ct left join TierPhone ctt on ct.intCompanyTierId = ctt.intCompanyTierId)
--select distinct intCompanyId from tempLocationNote
--select * from tempLocationNote where intCompanyId = 6254
, LocationNote as (SELECT intCompanyId, 
     STUFF(
         (SELECT char(10) + char (10) + replace(replace(replace(replace(locationName, char(0x0017),''),char(0x0003),''),char(0x001B),''),char(0x0002),'')
          from  tempLocationNote
          WHERE intCompanyId =tln.intCompanyId
    order by intCompanyId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,2,1, '')  AS locationNote
FROM tempLocationNote as tln
GROUP BY tln.intCompanyId)
--select * from LocationNote where intCompanyId = 6254
---------------------------------Get company consultants as Onwers
, tempConsultant as (select cc.intConsultantId, cc.intCompanyId, u.vchEmail
from lconsultantCompany cc left join dUser u on cc.intConsultantId = u.intUserId)

, CompOwner as (select intCompanyId,
		STUFF(
			(SELECT ',' + vchEmail
			 from  tempConsultant
			 WHERE intCompanyId =tc.intCompanyId
			 order by intCompanyId asc
			 FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
			  ,1,1, '')  AS ownerEmails
FROM tempConsultant as tc
GROUP BY tc.intCompanyId)


-----------------------------Get Company and Company Tier Attachment
, tempCompAttachment as(
select act.intAttachmentId, ct.intCompanyId
from lAttachmentCompanyTier act left join dCompanyTier ct on act.intCompanyTierId = ct.intCompanyTierId
union
select intAttachmentId, intCompanyId
from lAttachmentCompany)

, tempCompEvent as (
select ec.intEventId, ct.intCompanyId, ec.intInsertedById, ec.dtInserted
from lEventCompanyTier ec left join dCompanyTier ct on ec.intCompanyTierId = ct.intCompanyTierId
union
select intEventId, intCompanyId, intInsertedById, dtInserted
from lEventCompany)
--select * from tempCompAttachment
--, tempCompAttachment1 as(
--SELECT intCompanyId, ca.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY ca.intAttachmentId ASC) AS rn,
--		 concat(ca.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		 as attachmentName
--from tempCompAttachment ca left join dAttachment a on ca.intAttachmentId = a.intAttachmentId
--where vchFileType not in ('.eml','.mp4'))

, tempCompAttachment1 as(
SELECT intCompanyId, ca.intAttachmentId--, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY ca.intAttachmentId ASC) AS rn
	,case when vchFileType like '.eml' then e.msgfilename 
		 else
		 concat(ca.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 end as attachmentName
from tempCompAttachment ca left join dAttachment a on ca.intAttachmentId = a.intAttachmentId
							left join email e on a.intAttachmentId = e.AttachmentID
where vchFileType not in ('.mp4')
union
select
ec.intCompanyId, ae.intAttachmentId, em.msgfilename as attachmentName
from tempCompEvent ec left join dEvent e on ec.intEventId = e.intEventId
				left join lAttachmentEvent ae on ec.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
where em.AttachmentID is not null)

, compAttachment as (SELECT intCompanyId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempCompAttachment1
          WHERE intCompanyId =ca.intCompanyId
    order by intCompanyId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS companyAttachments
FROM tempCompAttachment1 as ca
GROUP BY ca.intCompanyId)

---Main Script---
insert into importCompany
select
  concat('NJFS',C.intCompanyId) as 'company-externalId'
, C.vchCompanyName as '(OriginalName)'
, iif(C.intCompanyId in (select intCompanyId from dup where dup.rn > 1)
	, iif(dup.vchCompanyName = '' or dup.vchCompanyName is NULL,concat('NJF Search - Default Company - ID',dup.intCompanyId),concat('NJFS-',dup.rn,' - ',dup.vchCompanyName))
	, iif(C.vchCompanyName = '' or C.vchCompanyName is null,concat('NJF Search - Default Company - ID',C.intCompanyId),C.vchCompanyName)) as 'company-name'
, iif(w.web = '' or w.web is NULL,'',left(w.web,99)) as 'company-website'
, iif(Phone.Num = '' or Phone.Num is NULL,'',Phone.Num) as 'company-phone'
, CSB.switchBoard as 'company-switchBoard'
, iif(f.fax = '' or f.fax is NULL,'',f.fax) as 'company-fax'
, iif(co.ownerEmails = '' or co.ownerEmails is NULL,'',co.ownerEmails) as 'company-owners'
, iif(len(ca.companyAttachments)>32000,'',ca.companyAttachments) as 'company-document'
, left(Concat(
			'Company External ID: NJFS', C.intCompanyId,char(10),
			concat(char(10),'Voyager Company Code: ',C.vchStandardRefCode,char(10)),
			iif(rc.vchCompanyTypeName = '' or rc.vchCompanyTypeName is NULL,'',Concat(char(10), 'Company Type: ', rc.vchCompanyTypeName, char(10))),
			iif(pon.intPONumberId = '' or pon.intPONumberId is NULL,'',Concat(char(10), 'PO Number: ', pon.vchPONumber, char(10))),
			--iif(C.tintVATCodeId = '' or C.tintVATCodeId is NULL,'',Concat(char(10),  'Company Financials:')),
			iif(C.vchCompanyRegNo = '' or C.vchCompanyRegNo is NULL,'',Concat(char(10), 'Registered No. ', C.vchCompanyRegNo)),
			iif(C.vchVATNumber = '' or C.vchVATNumber is NULL,'',Concat(char(10), 'VAT No. ', C.vchVATNumber)),
			iif(C.tintVATCodeId = '' or C.tintVATCodeId is NULL,'',Concat(char(10), 'VAT Code: ', vatc.vchVATCodeName)),
			iif(vatc.vchDescription = '' or vatc.vchDescription is NULL,'',Concat(' (', vatc.vchDescription, ' - ',vatc.decVATRate, ')',char(10))),
			iif(C.vchNonUKTaxCode = '' or C.vchNonUKTaxCode is NULL,'',Concat(char(10), 'Non-UK Tax Code: ', C.vchNonUKTaxCode, char(10))),
			iif(C.vchNote = '' or C.vchNote is NULL,'',Concat(char(10),'Other Notes: ',char(10),C.vchNote,char(10))),
			iif(ln.locationNote = '' or ln.locationNote is NULL,'',Concat(char(10),'LOCATION INFO: ',ln.locationNote))),32000)
			as 'company-note'
from dCompany C
			left join dup on C.intCompanyId = dup.intCompanyId
			left join web w on C.intCompanyId = w.intCompanyId
			left join Phone on C.intCompanyId = Phone.intCompanyId
			left join Fax f on C.intCompanyId = f.intCompanyId
			left join CompOwner co on C.intCompanyId = co.intCompanyId
			left join refCompanyType rc on C.tintCompanyTypeId = rc.tintCompanyTypeId
			left join dPONumber pon on C.intCompanyId = pon.intCompanyId
			left join refVATCode vatc on C.tintVATCodeId = vatc.tintVATCodeId
			left join LocationNote ln on C.intCompanyId = ln.intCompanyId
			left join compSwitchboard csb on c.intCompanyId = csb.intCompanyId
			left join compAttachment ca on c.intCompanyId = ca.intCompanyId
--where len(companyAttachments) > 32000--like '%.msg%'
--where c.intCompanyId in (2,455)--,1887,4546,6397,1436,6251,1670,2054,6275,499,5081,6676,5508,592,6504,6803,6488,2)
--order by c.intCompanyId				
UNION ALL
select 'NJFS9999999','','NJF Search - Default Company','','','','','','','This is Default Company from Data Import'


