------------------------------------------------------------------website: Done
with  minweb as (select o.ObjectID, min(p.PhoneId) as minwebID
from Objects o left join Phones p on o.ObjectID = p.ObjectId
where o.ObjectTypeId = 2 and p.CommunicationTypeId = 89
group by o.ObjectID)
--select * from minweb
, web as (select mw.ObjectID, p.PhoneId, p.Num
from minweb as mw left join Phones as p on mw.minwebID = p.PhoneId)
--select Num, count(Num) from web group by Num having count(Num)>1
--order by web.ObjectID
---------------------------------------------------------------------Other website: will be added to Note
, tWeb as (SELECT p.PhoneId, o.ObjectID, p.Num, ROW_NUMBER() OVER(PARTITION BY o.ObjectID ORDER BY p.PhoneId ASC) AS rn
FROM Objects o
left join Phones p on o.ObjectID = p.ObjectId
where o.ObjectTypeId = 2 and p.CommunicationTypeId = 89
and p.Num like '%.%'
)
,otherweb as (select ObjectID, Num from tWeb
				where rn >1)
--select * from otherweb
--order by ObjectID
----------------------------------------------------------------------Address:
, minadd as (select C.ClientID, min(A.AddressId) as minAddressId
from Clients C
left join Address A on C.ClientID = A.ObjectId
group by C.ClientID)
----select * from minadd-----
, loc1 as (select ma.ClientID, A.AddressId, A.Building, A.Street, A.District, A.City, A.PostCode
from minadd as ma left join Address as A on A.AddressId = ma.minAddressId)
-----select * from loc1----
, loc as (
	select loc1.ClientID, 
	ltrim(rtrim(concat(iif(loc1.Building = '' or loc1.Building is NULL,'',concat(loc1.Building,', '))
	, iif(loc1.Street = '' or loc1.Street is NULL,'',concat(loc1.Street,', '))
	, iif(loc1.District = '' or loc1.District is NULL,'',concat(loc1.District,', '))
	, iif(loc1.City = '' or loc1.City is NULL,'',concat(loc1.City,', '))
	, iif(loc1.Postcode = '' or loc1.Postcode is NULL,'',loc1.Postcode)))) as 'locationName'
	from loc1)
---------select * from loc	
----------------------------------------------------------------------Other address: will be added to Note
, TempAdd as (select o.ObjectID, A.AddressId, A.Building, A.Street, A.District, A.City, A.PostCode, 
					ROW_NUMBER() OVER(PARTITION BY o.ObjectID ORDER BY A.AddressID ASC) AS rn
from Objects o left join Address as A on o.ObjectID = A.ObjectId)
-----select * from loc1----
, otherAddress as (
	select ObjectID, 
	ltrim(rtrim(concat(iif(Building = '' or Building is NULL,'',concat(Building,', '))
	, iif(Street = '' or Street is NULL,'',concat(Street,', '))
	, iif(District = '' or District is NULL,'',concat(District,', '))
	, iif(City = '' or City is NULL,'',concat(City,', '))
	, iif(Postcode = '' or Postcode is NULL,'',Postcode)))) as 'otherAddress'
	from TempAdd where rn>1)
--select ObjectId, count(ObjectId) from otherAddress group by ObjectId-- having Count(ObjectId)>1
-- => if a company has more than 2 other address, have to merge these address together using stuff
----------------------------------------------------------------------Communication Type > Phone: join with clients through Phone.ObjectID: Done
, tphone as (select o.ObjectID, p.Num
from Objects as o left join Phones as p on o.ObjectID = p.ObjectID
where o.ObjectTypeId = 2 and p.CommunicationTypeId = 79)

, Phone as (SELECT ObjectID, 
     STUFF(
         (SELECT ',' + Num
          from  tphone
          WHERE ObjectID = T.ObjectID
    order by ObjectID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS Num
FROM tphone as T
GROUP BY T.ObjectID)
--select * from Phone
----------------------------------------------------------------------Communication Type > Phone Office: will be added to SwitchBoard field
, tempOfficePhone as (select o.ObjectID, p.Num
from Objects as o left join Phones as p on o.ObjectID = p.ObjectID
where o.ObjectTypeId = 2 and p.CommunicationTypeId = 87)

, officePhone as (SELECT ObjectID, 
     STUFF(
         (SELECT '; ' + Num
          from  tempOfficePhone
          WHERE ObjectID = tof.ObjectID
    order by ObjectID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS switchboard
FROM tempOfficePhone as tof
GROUP BY tof.ObjectID)
--select * from officePhone
-----------------------------------------------------------------------Communication Type > Email (Office): will be add to Notes
, tmail as (select o.ObjectID, p.Num
from Objects as o left join Phones as p on o.ObjectID = p.ObjectID
where o.ObjectTypeId = 2 and p.CommunicationTypeId = 85 and p.Num like '%@%')

, Email as (select ObjectID,
			STUFF((SELECT ',' + Num
					from tmail
					where ObjectID = TE.ObjectID
					order by ObjectID asc
					for xml path (''), type).value('.', 'nvarchar(max)'), 1,1, '') as Num
			from tmail as TE
			group by TE.ObjectID)		
--select * from Email
----------------------------------------------------------------------------------location: will be added to Notes
, adloc as (select Clients.ClientID, Locations.Description
from Clients left join Locations on Clients.LocationId = Locations.LocationId)
----select * from adloc
----------------------------------------------------------------------------------Status
, compstatus as (select Clients.ClientID, ClientStatus.Description
from Clients left join ClientStatus on Clients.StatusId = ClientStatus.ClientStatusId)
----select * from compstatus
----------source
,compsource as (select Clients.ClientID, Sources.Description
from Clients left join Sources on Clients.SourceId = Sources.SourceId)
-----select * from compsource
-------------Candidate Stored Document
----temp Candidate Stored Documents
, tempComStoredDoc as(select ObjectId, c.ClientID, t.TemplateId,
 concat('StoredDoc',concat(t.TemplateId,'_'),
 replace(replace(tt.TemplateTypeName,'?',''),' ',''),
 coalesce('_' + NULLIF(replace(replace(replace(replace(replace(t.TemplateName,'?',''),' ',''),'/','.'),'''',''),',','_'), replace(replace(tt.TemplateTypeName,'?',''),' ','')),''),
 td.FileExtension) as StoredDocName
--concat('StoredDoc',concat(tpl.TemplateId,'_'),TemplateName,Coalesce('_' + NULLIF(Description, ''), ''),FileExtension) as StoredDocName,tpl.TemplateId
from templateDocument td left join Templates t on td.TemplateId = t.TemplateId
	left join TemplateTypes tt on t.TemplateTypeId = tt.TemplateTypeId
	left join Clients c on t.ObjectId = c.ClientID
where c.ClientId is not null) 
--t.ClientId is not null)
--select * from tempCanstoredDoc
-----Stored Document
, CompStoredDoc as (select ClientID, STUFF(
					(Select ',' + StoredDocName
					from tempComStoredDoc 
					where ClientID = tcd.ClientID
    order by ClientID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'StoredDocName'
FROM tempComStoredDoc as tcd
GROUP BY tcd.ClientID)
--select * from StoredDoc
---DUPLICATION REGCONITION
, dup as (SELECT ClientID, Company, ROW_NUMBER() OVER(PARTITION BY Company ORDER BY ClientID ASC) AS rn 
FROM Clients)
----select * from dup
---Main Script---
select
  concat('FR',C.ClientID) as 'company-externalId'
, C.Company as '(OriginalName)'
, iif(C.ClientID in (select ClientID from dup where dup.rn > 1)
	, iif(dup.Company = '' or dup.Company is NULL,concat('Default Company-',dup.ClientID),concat(dup.Company,'-DUPLICATE-',dup.ClientID))
	, iif(C.Company = '' or C.Company is null,concat('Default Company-',C.ClientID),C.Company)) as 'company-name'
, iif(web.Num = '' or web.Num is NULL,'',left(web.Num,99)) as 'company-website'
, iif(loc.locationName = '' or loc.locationName is NULL,'',loc.locationName) as 'company-locationName'
, iif(loc.locationName = '' or loc.locationName is NULL,'',loc.locationName) as 'company-locationAddress'
, iif(loc1.City = '' or loc1.City is NULL,'',loc1.City) as 'company-locationCity'
, iif(loc1.District = '' or loc1.District is NULL,'',loc1.District) as 'company-locationDistrict'
, iif(loc1.PostCode = '' or loc1.PostCode is NULL,'',loc1.PostCode) as 'company-locationZipCode'
, iif(op.switchboard = '' or op.switchboard is NULL,'',concat('Phone (Office): ', op.switchboard)) as 'company-switchBoard'
, iif(Phone.Num = '' or Phone.Num is NULL,'',Phone.Num) as 'company-phone'
, csd.StoredDocName as 'company-document'
, left(Concat('Company External ID: ', C.ClientID,char(10),
			iif(Email.Num = '' or Email.Num is NULL,'',Concat('Company Email (Office): ', Email.Num, char(10))),
			iif(oa.otherAddress = '' or oa.otherAddress is NULL,'',Concat('Company Other Address: ', oa.otherAddress, char(10))),
			iif(adloc.Description = '' or adloc.Description is NULL,'',Concat('Location: ', adloc.Description, char(10))),
			iif(otherweb.Num = '' or otherweb.Num is NULL,'',Concat('Company Other Website: ', otherweb.Num, char(10))),
			iif(cs.Description = '' or cs.Description is NULL,'',Concat('Company Status: ', cs.Description, char(10))),
			iif(compsource.Description = '' or compsource.Description is NULL,'',Concat('Company Source: ', compsource.Description, char(10))),  
			iif(cast(C.DefaultTermPerc as varchar(max)) = '' or C.DefaultTermPerc is NULL,'',Concat('Company Term: ',cast(C.DefaultTermPerc as varchar(max)), char(10))),
			iif(C.Notes = '' or C.Notes is NULL,'',Concat(char(10),'Other Notes: ',C.Notes))),32000)
		as 'company-note'
from Clients as C
				left join dup on C.ClientID = dup.ClientID
				left join web on C.ClientID = web.ObjectID
				left join loc on C.ClientID = loc.ClientID
				left join loc1 on C.ClientID = loc1.ClientID
				left join Phone on C.ClientID = Phone.ObjectID
				left join Email on C.ClientID = Email.ObjectID
				left join compstatus cs on C.ClientID = cs.ClientID
				left join compsource on C.ClientID = compsource.ClientID
				left join adloc on C.ClientID = adloc.ClientID
				left join otherweb on C.ClientID = otherweb.ObjectID
				left join otherAddress oa on C.ClientID = oa.ObjectID
				left join officePhone op on C.ClientID = op.ObjectID
				left join CompStoredDoc csd on c.ClientID = csd.ClientID
UNION ALL
select 'FR9999999','','Default Company','','','','','','','','','','This is Default Company from Data Import'
