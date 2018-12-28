;with
TmpTab1 as (
  select distinct [Client ID] as ComId
  from [All contacts with ID]
  where [Client ID] not in (
	select [Client ID] from [Clients List with ID]
  )
)

, TmpTab2 as (
	select
		--concat('_DefCom_', ComId) as externalId
		ComId as externalId
		, concat('[Default Company ', ComId, ']') as name
		, 'UK' as locationName
		, 'UK' as locationAddress
		, '' as locationCity
		, '' as locationState
		, 'GB' as locationCountry
		, '' as locationZipCode
		, '' as phone
	from TmpTab1
)

--select * from TmpTab2

insert into dbo.VC_Com (
	[company-externalId]
	,[company-name]
	,[company-locationName]
	,[company-locationAddress]
	,[company-locationCity]
	,[company-locationState]
	,[company-locationCountry]
	,[company-locationZipCode]
	,[company-phone]
)
select * from TmpTab2

--delete from dbo.VC_Com
--where [company-externalId] like '_DefCom_%'