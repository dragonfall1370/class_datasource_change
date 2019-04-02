with compSwitchboard as(
select intCompanyId, vchValue, replace(iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber), char(0x0003),'') as switchBoard
, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY vchValue ASC) AS rn
from dCompanyTelecom 
where vchDescription = 'switchboard' or (tintTelecomId  = 4 and vchDescription <> 'Tel' and vchValue <> ''))

, tphone1 as (select intCompanyId, vchValue, replace(iif(vchExtension <> '', concat(vchNumber,vchExtension), vchNumber), char(0x0003),'') as vchNumber
from dCompanyTelecom where vchDescription = 'Tel' or vchDescription = 'JK''s Number')

, tphone2 as (select ct.intCompanyId, vchValue, iif(vchExtension <> '', concat(vchNumber,vchExtension, ' (',ct.vchCompanyTierName, ')'), concat(vchNumber, ' (', ct.vchCompanyTierName, ')')) as vchNumber --ctt.intCompanyTierId, intCompanyTierTelecomId
				,  ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY vchNumber ASC) AS rn
from dCompanyTierTelecom ctt left join dCompanyTier ct on ctt.intCompanyTierId = ct.intCompanyTierId
where vchNumber is not null and vchNumber <> '' and (ctt.vchDescription = 'Location Tel No' or ctt.vchDescription = '')  and (ct.vchCompanyTierName = 'Main' or ct.vchCompanyTierName = 'Head Office'))

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

, Phone as (select a.intCompanyId, coalesce(t1.vchNumber, mtp.vchNumber) as Num
from dCompany a left join tphone1 t1 on a.intCompanyId = t1.intCompanyId
left join mainTierPhone mtp on a.intCompanyId = mtp.intCompanyId)

, web as (select intCompanyId,
	case when left(vchValue,2) = '//' then right(vchValue,len(vchValue)-2)
		 when left(vchValue,1) = '/' then right(vchValue,len(vchValue)-1)
	else vchValue end as web--, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY intCompanyTelecomId ASC) AS rn
from dCompanyTelecom
where vchValue <> ''and vchValue not like '%@%'  and (vchValue like '%.co%' or vchValue like '%www%'))

, dup as (select intCompanyId, vchCompanyName, ROW_NUMBER() OVER(PARTITION BY vchCompanyName ORDER BY intCompanyId ASC) AS rn
from dCompany)


, tempConsultant as (select ca.intConsultantId, ca.intCompanyId, u.vchEmail
from lconsultantCompany ca left join dUser u on ca.intConsultantId = u.intUserId)

, CompOwner as (select intCompanyId,
		STUFF(
			(SELECT ',' + vchEmail
			 from  tempConsultant
			 WHERE intCompanyId =ta.intCompanyId
			 order by intCompanyId asc
			 FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
			  ,1,1, '')  AS ownerEmails
FROM tempConsultant as ta
GROUP BY ta.intCompanyId)

,comaddress1 as (select a.intCompanyId,
concat(a.vchaddressline1, ' ' , a.vchAddressLine2 , ' ', a.vchAddressLine3, ' ',
a.vchTown, ' ', a.vchCounty, ' ', a.vchPostcode) as address
from dCompanyTier a)

,comaddress2 as (select *,ROW_NUMBER() over (partition by intcompanyid order by intcompanyid) as rn from comaddress1)

,mainaddress as (select * from comaddress2 where rn = 1)

------------Main Script-----------------

select
  a.intCompanyId as 'company-externalId'
, iif(a.intCompanyId in (select intCompanyId from dup where dup.rn > 1)
	, iif(dup.vchCompanyName = '' or dup.vchCompanyName is NULL,concat('NJF Search - Default Company - ID',dup.intCompanyId),concat(dup.rn,' - ',dup.vchCompanyName))
	, iif(a.vchCompanyName = '' or a.vchCompanyName is null,concat('NJF Search - Default Company - ID',a.intCompanyId),a.vchCompanyName)) as 'company-name'
	, iif(b.address = '' or b.address is null,'',b.address) as 'company-address'
, iif(w.web = '' or w.web is NULL,'',left(w.web,99)) as 'company-website'
, iif(Phone.Num = '' or Phone.Num is NULL,'',Phone.Num) as 'company-phone'
, iif(CSB.switchBoard is null or CSB.switchboard = '','',CSB.switchBoard) as 'company-switchBoard'
, iif(co.ownerEmails = '' or co.ownerEmails is NULL,'',co.ownerEmails) as 'company-owners'
, left(Concat(
			'Company External ID: ', a.intCompanyId,char(10),
			concat(char(10),'Voyager Company Code: ',a.vchStandardRefCode,char(10)),
			iif(ra.vchCompanyTypeName = '' or ra.vchCompanyTypeName is NULL,'',Concat(char(10), 'Company Type: ', ra.vchCompanyTypeName, char(10))),
			iif(pon.intPONumberId = '' or pon.intPONumberId is NULL,'',Concat(char(10), 'PO Number: ', pon.vchPONumber, char(10))),
			--iif(a.tintVATCodeId = '' or a.tintVATCodeId is NULL,'',Concat(char(10),  'Company Financials:')),
			iif(a.vchCompanyRegNo = '' or a.vchCompanyRegNo is NULL,'',Concat(char(10), 'Registered No. ', a.vchCompanyRegNo)),
			iif(a.vchVATNumber = '' or a.vchVATNumber is NULL,'',Concat(char(10), 'VAT No. ', a.vchVATNumber)),
			iif(a.tintVATCodeId = '' or a.tintVATCodeId is NULL,'',Concat(char(10), 'VAT Code: ', vata.vchVATCodeName)),
			iif(vata.vchDescription = '' or vata.vchDescription is NULL,'',Concat(' (', vata.vchDescription, ' - ',vata.decVATRate, ')',char(10))),
			iif(a.vchNonUKTaxCode = '' or a.vchNonUKTaxCode is NULL,'',Concat(char(10), 'Non-UK Tax Code: ', a.vchNonUKTaxCode, char(10))),
			iif(a.vchNote = '' or a.vchNote is NULL,'',Concat(char(10),'Other Notes: ',char(10),a.vchNote,char(10)))),32000)
			as 'company-note'
from dCompany a
			left join dup on a.intCompanyId = dup.intCompanyId
			left join web w on a.intCompanyId = w.intCompanyId
			left join Phone on a.intCompanyId = Phone.intCompanyId
			left join CompOwner co on a.intCompanyId = co.intCompanyId
			left join refCompanyType ra on a.tintCompanyTypeId = ra.tintCompanyTypeId
			left join dPONumber pon on a.intCompanyId = pon.intCompanyId
			left join refVATCode vata on a.tintVATCodeId = vata.tintVATCodeId
			left join compSwitchboard csb on a.intCompanyId = csb.intCompanyId
			left join mainaddress b on a.intCompanyId = b.intCompanyId