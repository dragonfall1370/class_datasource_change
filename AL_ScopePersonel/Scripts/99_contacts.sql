drop table if exists RF_Contacts
declare @ObjectTypeId int
--1	APP	Applicant => candidate
--2	CLNT	Client => company
--3	CCT	Contact => contact
--4	PER	Person => 
set @ObjectTypeId = 3
;with
CanObjs as (
	select
	ObjectId
	, LocationId
	, CreatedUserId
	, CreatedOn
	from Objects
	where ObjectTypeId =  @ObjectTypeId
)

, CanContacts1 as (
	select
	ObjectId
	, iif(CommunicationTypeId = 78, lower(trim(isnull(Num, ''))), '') as Email
	, iif(CommunicationTypeId = 79, lower(trim(isnull(Num, ''))), '') as Phone
	, iif(CommunicationTypeId = 80, lower(trim(isnull(Num, ''))), '') as Fax
	, iif(CommunicationTypeId = 81, lower(trim(isnull(Num, ''))), '') as PhoneDay
	, iif(CommunicationTypeId = 82, lower(trim(isnull(Num, ''))), '') as PhoneEvening
	, iif(CommunicationTypeId = 83, lower(trim(isnull(Num, ''))), '') as Mobile
	, iif(CommunicationTypeId = 85, lower(trim(isnull(Num, ''))), '') as EmailOffice
	, iif(CommunicationTypeId = 86, lower(trim(isnull(Num, ''))), '') as EmailPersonal
	, iif(CommunicationTypeId = 87, lower(trim(isnull(Num, ''))), '') as PhoneOffice
	, iif(CommunicationTypeId = 88, lower(trim(isnull(Num, ''))), '') as PhoneHome
	, iif(CommunicationTypeId = 89, lower(trim(isnull(Num, ''))), '') as Website
	, iif(CommunicationTypeId = 90, lower(trim(isnull(Num, ''))), '') as SocialNetworking
	from Phones
)

, CanContacts2 as (
	select
	ObjectId
	, max(Email) as Email
	, max(Phone) as Phone
	, max(Fax) as Fax
	, max(PhoneDay) as PhoneDay
	, max(PhoneEvening) as PhoneEvening
	, max(Mobile) as Mobile
	, max(EmailOffice) as EmailOffice
	, max(EmailPersonal) as EmailPersonal
	, max(PhoneOffice) as PhoneOffice
	, max(PhoneHome) as PhoneHome
	, max(Website) as Website
	, max(SocialNetworking) as SocialNetworking
	from CanContacts1
	group by ObjectId
)

, CanAddress1 as (
	select
	ObjectId
	, AddressId
	from (
		select
		ObjectId
		, AddressId
		, max(rn) as rn
		from (
			select ObjectId
			, AddressId
			, row_number() over(partition by ObjectId order by ObjectId asc, UpdatedOn desc) as rn
			from [Address] --71046
			where
			--AddressTypeId = 57 -- 60
			AddressTypeId = 58 -- 32237
			and ObjectId in (select ObjectId from Objects where ObjectTypeId = @ObjectTypeId)
		) abc
		group by ObjectId, AddressId
	) def
	where def.rn = 1
)

, CanAddress2 as (
	select
	x.ObjectId
	, y.Building
	, y.Street
	, y.District
	, y.City
	, y.CountyValueId
	, y.CountryValueId
	, y.PostCode
	from CanAddress1 x
	left join [Address] y on x.AddressId = y.AddressId
)

select
x.*
, a.Personname
, a.Surname
, a.Salutation
, a.TitleValueId
, a.GenderValueId
, a.MaritalStatusValueId
--, b.
--, c.
--, d.
--, f.Building
--, f.Street
--, f.District
--, f.City
--, f.CountyValueId
--, f.CountryValueId
--, f.PostCode
--, e.Nationality
, a.Dob
, a.Notes
, a.Photograph
, y.Description as LocationName
, y.Code
, y.Latitude
, y.Longitude
, y.X
, y.Y
, z.Email
, z.Phone
, z.Fax
, z.PhoneDay
, z.PhoneEvening
, z.Mobile
, z.EmailOffice
, z.EmailPersonal
, z.PhoneOffice
, z.PhoneHome
, z.Website
, z.SocialNetworking

into RF_Contacts

from CanObjs x
left join Locations y on x.LocationId = y.LocationId
left join CanContacts2 z on x.ObjectID = z.ObjectID
left join Person a on x.ObjectID = a.PersonID
--left join TitleValue
--left join GenderValue
--left join MaritalStatusValue
left join Nationality e on a.NationalityId = e.NationalityId
left join CanAddress2 f on x.ObjectID = f.ObjectId

select * from RF_Contacts

--SELECT [ObjectID]
--      ,[ObjectTypeId]
--      ,[FileAs]
--      ,[FlagText]
--      ,[LocationId]
--      ,[CreatedUserId]
--      ,[CreatedOn]
--      ,[UpdatedUserId]
--      ,[UpdatedTimestamp]
--      ,[SourceId]
--  FROM [scope].[dbo].[Objects]
--  where ObjectTypeId =  1
--  and ObjectID = 117222

  --select * from Person
  --where PersonId = 117222

  --select * from Attributes
  --where Description like '%mar%'

--  select * from Locations
--  where LocationId = 47037

--  select * from Phones
--where objectID = 117222

--select count(*) from [Address] -- 71046
----where objectID = 106352-- 117222
--where ObjectID in (
--	47617
--, 51013
--, 73014
--, 78397
--, 86799
--, 91948
--, 113534
--)
--order by ObjectId

--select ObjectId
--, AddressId from 
--(select
--ObjectId
--, AddressId
--, max(rn) as rn from (
--select ObjectId
--, AddressId
--, row_number() over(partition by ObjectId order by ObjectId asc, UpdatedOn desc) as rn
--from [Address] --71046
--where
----AddressTypeId = 57 -- 60
--AddressTypeId = 58 -- 32237
--and ObjectId in (select ObjectId from Objects where ObjectTypeId = 1)
--) abc
--group by ObjectId, AddressId
--) def
--where def.rn = 1
--select 32237 + 60 -- 32297

--select * from AddressTypes

--AddressTypeId	SystemCode	Description
--57	ADDR_TYP_OFFICE	Office Address
--58	ADDR_TYP_HOME	Home Address
--60	ADDR_TYP_LOCAL	Local Address

--ObjectTypeId	SystemCode	Description
--1	APP	Applicant
--2	CLNT	Client
--3	CCT	Contact
--4	PER	Person

--select * from CommunicationTypes
--CommunicationTypeId	SystemCode	Description
--78	COMM_TYP_EMAIL	Email
--79	COMM_TYP_PHONE	Phone
--80	COMM_TYP_FAX	Fax
--81	COMM_TYP_DAYPHONE	Phone (Day)
--82	COMM_TYP_EVENPHONE	Phone (Evening)
--83	COMM_TYP_MOBILE	Mobile
--85	COMM_TYP_OFFICE_EMAIL	Email (Office)
--86	COMM_TYP_PERSONAL_EMAIL	Email (Personal)
--87	COMM_TYP_OFFICE_PHONE	Phone (Office)
--88	COMM_TYP_HOME_PHONE	Phone (Home)
--89	COMM_TYP_URL	Website
--90	COMM_TYP_URL	Social Networking