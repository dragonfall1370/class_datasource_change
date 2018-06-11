------------------------------------------------------------------website: Done
with  minweb as (select o.ObjectID, min(p.PhoneId) as minwebID
from Objects o left join Phones p on o.ObjectID = p.ObjectId
where o.ObjectTypeId = 2 and p.CommunicationTypeId = 89
and p.Num like '%.%'
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
, loc1 as (select ma.ClientID, A.AddressId, A.Building, A.Street, A.District, A.City, A.PostCode, A.CountyValueId, lv.ValueName as County, A.CountryValueId, lv1.ValueName as Country
from minadd as ma left join Address as A on A.AddressId = ma.minAddressId
					left join ListValues lv on a.CountyValueId = lv.ListValueId
					left join ListValues lv1 on a.CountryValueId = lv1.ListValueId)
--select * from loc1----
, loc as (
	select loc1.ClientID, 
	ltrim(rtrim(concat(iif(loc1.Building = '' or loc1.Building is NULL,'',concat(loc1.Building,', '))
	, iif(loc1.Street = '' or loc1.Street is NULL,'',concat(loc1.Street,', '))
	, iif(loc1.District = '' or loc1.District is NULL,'',concat(loc1.District,', '))
	, iif(loc1.City = '' or loc1.City is NULL,'',concat(loc1.City,', '))
	, iif(loc1.County = '' or loc1.County is NULL,'',concat(loc1.County,', '))
	, iif(loc1.Country = '' or loc1.Country is NULL,'',concat(loc1.Country,', '))
	, iif(loc1.Postcode = '' or loc1.Postcode is NULL,'',loc1.Postcode)))) as 'locationName'
	from loc1)
---------select * from loc	
----------------------------------------------------------------------All addresses: will be added to Note
, TempAdd as (
select o.ObjectID, A.AddressId, A.Building, A.Street, A.District, A.City, A.PostCode, a.AddressTypeId, at.Description,
		A.CountyValueId, lv.ValueName as County, A.CountryValueId, lv1.ValueName as Country,
		ROW_NUMBER() OVER(PARTITION BY o.ObjectID ORDER BY A.AddressID ASC) AS rn
from Objects o left join Address as A on o.ObjectID = A.ObjectId
				left join AddressTypes at on a.AddressTypeId = at.AddressTypeId
				left join ListValues lv on a.CountyValueId = lv.ListValueId
				left join ListValues lv1 on a.CountryValueId = lv1.ListValueId
where o.ObjectTypeId = 2)
-----select * from loc1----
, AllAddress1 as (
	select ObjectID, 
	ltrim(rtrim(concat(
	  iif(AddressId = '' or AddressId is NULL,'',concat(Description,': '))
	, iif(Building = '' or Building is NULL,'',concat(Building,', '))
	, iif(Street = '' or Street is NULL,'',concat(Street,', '))
	, iif(District = '' or District is NULL,'',concat(District,', '))
	, iif(City = '' or City is NULL,'',concat(City,', '))
	, iif(County = '' or County is NULL,'',concat(County,', '))
	, iif(Country = '' or Country is NULL,'',concat(Country,', '))
	, iif(Postcode = '' or Postcode is NULL,'',Postcode)))) as 'allAddress'
	from TempAdd)-- where rn>1)
, allAddress as (SELECT ObjectID, 
     STUFF(
         (SELECT char(10) + allAddress
          from  AllAddress1
          WHERE ObjectID = oa1.ObjectID
    order by ObjectID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'allAddress'
FROM AllAddress1 as oa1
GROUP BY oa1.ObjectID)
--select * from otherAddress where ObjectID = 3117
--------------------------------------------------------Client's Consultants: Add to Note
, TempConsultant as (
select cc.ClientId, cc.UserId, u.UserName, u.Surname, cc.UserRelationshipId, ur.Description, cc.CommissionPerc,
	ROW_NUMBER() OVER(PARTITION BY cc.ClientId ORDER BY cc.ClientConsultantId ASC) AS rn
from ClientConsultants cc
left join Users u on cc.UserId = u.UserId
left join UserRelationships ur on cc.UserRelationshipId = ur.UserRelationshipId)
--select * from TempConsultant----
, ClientConsultant1 as (
	select ClientId, 
	ltrim(rtrim(concat(
	  iif(ClientId is NULL,'',concat(rn,'. '))
	, iif(UserName = '' or UserName is NULL,'',iif(Surname = '' or Surname is NULL,concat(UserName, ', '),concat(UserName, ' ')))
	, iif(Surname = '' or Surname is NULL,'',concat(Surname,', '))
	, iif(Description = '' or Description is NULL,'',concat('Relationship: ',Description,', '))
	, iif(CommissionPerc is NULL,'',concat('Commission Perc: ',CommissionPerc)))))
	 as 'consultant'
	from TempConsultant)-- where rn>1)
, ClientConsultant as (SELECT ClientId, 
     STUFF(
         (SELECT char(10) + consultant
          from  ClientConsultant1
          WHERE ClientId = cc.ClientId
    order by ClientId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'consultant'
FROM ClientConsultant1 as cc
GROUP BY cc.ClientId)
--select * from ClientConsultant
--------------------------------------------------------Client's Attributes: Add to Note
, TempClientAttribute as (
select oa.ObjectID, oa.ObjectAttributeId, oa.AttributeId, a.Description, a.Notes,
ROW_NUMBER() OVER(PARTITION BY oa.ObjectId ORDER BY oa.ObjectAttributeId ASC) AS rn
--distinct (oa.ObjectID)
from ObjectAttributes oa left join Attributes a on oa.AttributeId = a.AttributeId
left join Clients c on oa.ObjectID = c.ClientId
where c.ClientId is not null)
--select * from TempClientAttribute
, ClientAttribute1 as (
	select ObjectId, 
	ltrim(rtrim(concat(rn,'. '
	, iif(Description = '' or Description is NULL,'',concat(Description, ', '))
	, iif(Notes = '' or Notes is NULL,'',concat('Note: ',Notes)))))
	 as 'attribute'
	from TempClientAttribute)-- where rn>1)
, ClientAttributes as (SELECT ObjectID, 
     STUFF(
         (SELECT char(10) + attribute
          from  ClientAttribute1
          WHERE ObjectID = ca.ObjectId
    order by ObjectID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'attribute'
FROM ClientAttribute1 as ca
GROUP BY ca.ObjectID)
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
--, compstatus as (select Clients.ClientID, ClientStatus.Description
--from Clients left join ClientStatus on Clients.StatusId = ClientStatus.ClientStatusId)
------select * from compstatus
----------source
,compsource as (select Clients.ClientID, Sources.Description
from Clients left join Sources on Clients.SourceId = Sources.SourceId)
-----select * from compsource

---DUPLICATION REGCONITION
, dup as (SELECT ClientID, Company, ROW_NUMBER() OVER(PARTITION BY Company ORDER BY ClientID ASC) AS rn 
FROM Clients)
----select * from dup
---Main Script---
select
  concat('MP',C.ClientID) as 'company-externalId'
, C.Company as '(OriginalName)'
, iif(C.ClientID in (select ClientID from dup where dup.rn > 1)
	, iif(dup.Company = '' or dup.Company is NULL,concat('Default Company-',dup.ClientID),concat(dup.Company,'-DUPLICATE-',dup.ClientID))
	, iif(C.Company = '' or C.Company is null,concat('Default Company-',C.ClientID),C.Company)) as 'company-name'
, iif(web.Num = '' or web.Num is NULL,'',left(web.Num,99)) as 'company-website'
, iif(loc.locationName = '' or loc.locationName is NULL,'',ltrim(loc.locationName)) as 'company-locationName'
, iif(loc.locationName = '' or loc.locationName is NULL,'',ltrim(loc.locationName)) as 'company-locationAddress'
, iif(loc1.City = '' or loc1.City is NULL,'',loc1.City) as 'company-locationCity'
, iif(loc1.District = '' or loc1.District is NULL,'',loc1.District) as 'company-locationDistrict'
, iif(loc1.PostCode = '' or loc1.PostCode is NULL,'',loc1.PostCode) as 'company-locationZipCode'
, case
	when loc1.Country like 'Austra%' then 'AU'
	when loc1.Country like 'Austri%' then 'AT'
	when loc1.Country like 'Belgi%' then 'BE'
	when loc1.Country like 'Canadi%' then 'CA'
	when loc1.Country like 'China%' then 'CN'
	when loc1.Country like 'Finland%' then 'FI'
	when loc1.Country like 'Flemish%' then 'BE'
	when loc1.Country like 'France%' then 'FR'
	when loc1.Country like 'German%' then 'DE'
	when loc1.Country like 'Holland%' then 'NL'
	when loc1.Country like 'Ireland%' then 'IE'
	when loc1.Country like 'Nether%' then 'NL'
	when loc1.Country like 'Northern Irish' then 'IE'
	when loc1.Country like 'Scotland%' then 'GB'
	when loc1.Country like 'Sweden%' then 'SE'
	when loc1.Country like 'Switzerland%' then 'CH'
	when loc1.Country like 'Turk%' then 'TR'
	when loc1.Country like 'UK%' then 'GB'
	when loc1.Country like 'USA%' then 'US'
else '' end as 'company-locationCountry'
, iif(Phone.Num = '' or Phone.Num is NULL,'',replace(Phone.Num,'/ ',',')) as 'company-phone'
, iif(op.switchboard = '' or op.switchboard is NULL,'',concat(op.switchboard,' (Office)')) as 'company-switchBoard'
, left(Concat(
			'Company External ID: MP', C.ClientID,char(10),
			iif(Email.Num = '' or Email.Num is NULL,'',Concat(char(10), 'Email (Office): ', Email.Num, char(10))),
			iif(oa.allAddress = '' or oa.allAddress is NULL,'',Concat(char(10), 'All Address(es): ',char(10), oa.allAddress, char(10))),
			iif(adloc.Description = '' or adloc.Description is NULL,'',Concat(char(10), 'Location: ', adloc.Description, char(10))),
			iif(otherweb.Num = '' or otherweb.Num is NULL,'',Concat(char(10), 'Other Website: ', otherweb.Num, char(10))),
			iif(cs.Description = '' or cs.Description is NULL,'',Concat(char(10), 'Status: ', cs.Description, char(10))),
			iif(compsource.Description = '' or compsource.Description is NULL,'',Concat(char(10), 'Source: ', compsource.Description, char(10))),  
			iif(c.VatNo = '' or c.VatNo is NULL,'',Concat(char(10), 'Vat No. ', c.VatNo, char(10))),
			iif(c.RegNo = '' or c.RegNo is NULL,'',Concat(char(10), 'Reg No. ', c.RegNo, char(10))),
			iif(cc.consultant = '' or cc.consultant is NULL,'',Concat(char(10), 'Consultant(s): ',char(10), cc.consultant, char(10))),
			iif(cast(C.DefaultTermPerc as varchar(max)) = '' or C.DefaultTermPerc is NULL,'',Concat(char(10), 'Term: ',cast(C.DefaultTermPerc as varchar(max)), char(10))),
			iif(ca.attribute = '' or ca.attribute is NULL,'',Concat(char(10), 'Attribute(s): ',char(10), ca.attribute, char(10))),
			iif(C.Notes = '' or C.Notes is NULL,'',Concat(char(10),'Other Notes: ',char(10),C.Notes))),32000)
			as 'company-note'
from Clients as C
				left join dup on C.ClientID = dup.ClientID
				left join web on C.ClientID = web.ObjectID
				left join loc on C.ClientID = loc.ClientID
				left join loc1 on C.ClientID = loc1.ClientID
				left join Phone on C.ClientID = Phone.ObjectID
				left join Email on C.ClientID = Email.ObjectID
				left join ClientStatus cs on C.StatusId = cs.ClientStatusId
				left join compsource on C.ClientID = compsource.ClientID
				left join adloc on C.ClientID = adloc.ClientID
				left join otherweb on C.ClientID = otherweb.ObjectID
				left join allAddress oa on C.ClientID = oa.ObjectID
				left join officePhone op on C.ClientID = op.ObjectID
				left join ClientConsultant cc on C.ClientID = cc.ClientId
				left join ClientAttributes ca on c.ClientID = ca.ObjectID
UNION ALL
select 'MP9999999','','Default Company','','','','','','','','','','This is Default Company from Data Import'

