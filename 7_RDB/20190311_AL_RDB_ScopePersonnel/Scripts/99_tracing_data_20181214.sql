SELECT * FROM CommunicationTypes
SELECT * FROM Phones
SELECT * FROM Objects
SELECT * FROM ObjectTypes
--ObjectTypeId	SystemCode	Description
--1	APP	Applicant
--2	CLNT	Client
--3	CCT	Contact
--4	PER	Person

-- populate data for Company

select * from Locations

--SELECT * FROM CommunicationTypes
--SELECT * FROM Phones
--SELECT * FROM Objects
--SELECT * FROM ObjectTypes
--ObjectTypeId	SystemCode	Description
--1	APP	Applicant
--2	CLNT	Client
--3	CCT	Contact
--4	PER	Person

-- populate data for Company

--select * from Locations

;with

ComIdx as(
	select ObjectID as ComId, FileAs as ComName, LocationId, SourceId from [Objects] where ObjectTypeId = 2
)

, ComPhones as (
	select
	x.ComId
	, y.CommunicationTypeId
	, iif([dbo].[ufn_CheckEmailAddress](trim(isnull(Num, ''))) = 1, trim(isnull(Num, '')), '') as Email
	, iif([dbo].[ufn_CheckEmailAddress](trim(isnull(Num, ''))) = 0, [dbo].[ufn_RefinePhoneNumber_V2](trim(isnull(Num, ''))), '') as Phone
	--, trim(isnull(Num, '')) as PhoneOrEmail
	, z.SystemCode as CommunicatonTypeCode
	, z.Description as CommunicatonTypeName
	from ComIdx x
	left join (select ObjectID, CommunicationTypeId, Num from Phones) y on x.ComId = y.ObjectID
	left join (select CommunicationTypeId, SystemCode, Description from CommunicationTypes) z on y.CommunicationTypeId = z.CommunicationTypeId
)

, Locs as (
	select
	LocationId
	, Code
	, Description
	, X
	, Y
	, Latitude
	, Longitude
	from Locations
)

, Srcs as (
	select
	SourceId
	, Description
	from [Sources]
)

SELECT
x.ComId
, ComName
, y.Description as ComLocName
, y.Code as ComPostCode
, y.Latitude
, y.Longitude
, y.X
, y.Y
, z.Description as ComSource
, a.CommunicatonTypeCode
, a.CommunicatonTypeName
, a.Email
, a.Phone
FROM ComIdx x
left join Locs y on x.LocationId = y.LocationId
left join Srcs z on x.SourceId = z.SourceId
left join ComPhones a on x.ComId = a.ComId
order by x.ComId

