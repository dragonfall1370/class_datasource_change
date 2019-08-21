drop table if exists VC_Com

select

cast(x.ID as varchar(255)) as [company-externalId]

, trim(isnull(x.Name, '')) as [company-name]

, trim(isnull(x.LocName, '')) as [company-locationName]

, trim(isnull(x.Address, '')) as [company-locationAddress]

, '' as [company-locationCity]
--, db-field-not-found as [company-locationDistrict]

, '' as [company-locationState]

, 'GB' as [company-locationCountry]

, trim(isnull(y.locations_code, '')) as [company-locationZipCode]
--, db-field-not-found as [company-nearestTrainStation]
--, db-field-not-found as [company-headQuarter]

, trim(isnull(y.details_phone, '')) as [company-phone]

, trim(isnull(y.details_fax, '')) as [company-fax]
--, db-field-not-found as [company-switchBoard]

, dbo.ufn_RefineWebAddress(
	isnull(y.details_website, '')
	--case(y.web_addr)
	--	when 'N/A' then ''
	--	when '0' then ''
	--	else trim( '/ ' from y.web_addr)
	--end
) as [company-website]

, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(nullif(trim(isnull(y.users_createdemailaddress, '')), 'admin@company.com'), 'freddie@scopepersonnel.co.uk'), '')) as [company-owners]

--01-Dec-2017 12:40:42
, concat(
	concat('External ID: ', x.ID)
	
	, nullif(
		concat(char(10), 'Company status: ', y.clientstatus_statusid_description)
		, concat(char(10), 'Company status: ', '')
	)
	, nullif(
		nullif(
			concat(char(10), 'Default Term Percentage: ', y.defaulttermperc)
			, concat(char(10), 'Default Term Percentage: ', '')
		)
		, concat(char(10), 'Default Term Percentage: ', '0.00')
	)
	, nullif(
		concat(char(10), 'Source: ', y.sources_description)
		, concat(char(10), 'Source: ', '')
	)
	, nullif(
		concat(char(10), 'Email: ', y.details_email)
		, concat(char(10), 'Email: ', '')
	)
	, nullif(
		concat(char(10), 'Mobile phone: ', y.details_mobile)
		, concat(char(10), 'Mobile phone: ', '')
	)
	, nullif(
		concat(char(10), 'Phone day: ', y.details_phoneday)
		, concat(char(10), 'Phone day: ', '')
	)
	, nullif(
		concat(char(10), 'Phone evening: ', y.details_phoneevening)
		, concat(char(10), 'Phone evening: ', '')
	)
	, nullif(
		concat(char(10), 'Email office: ', y.details_emailoffice)
		, concat(char(10), 'Email office: ', '')
	)
	, nullif(
		concat(
			char(10), 'Created on: '
			, FORMAT(dateadd(day, -2, cast(y.createdon as datetime)), 'dd-MMM-yyyy H:mm:ss', 'en-gb')
			, char(10)
			, char(10)
		)
		, concat(char(10), 'Created on: ', char(10), char(10))
	)
	, nullif(
		concat(
			char(10), 'Last updated: '
			, FORMAT(dateadd(day, -2, cast(y.updatedon as datetime)), 'dd-MMM-yyyy H:mm:ss', 'en-gb')
			, char(10)
			, char(10)
		)
		, concat(char(10), 'Last updated: ', char(10), char(10))
	)

	, nullif(
		concat('·················································'
			, char(10)
			, 'Notes:'
			, char(10)
			, '·················································'
			, char(10)
			, y.notes
		)
		, concat('·················································'
			, char(10)
			, 'Notes:'
			, char(10)
			, '·················································'
			, char(10)
		)
	)
) as [company-note]

--, trim(isnull(convert(varchar(255), cast(y.entered as datetime), 120), '')) as [company-note1]

, x.Docs as [company-document]

into VC_Com

from
VC_ComIdx x
left join [RF_Clients_Complete] y on x.ID = y.ClientID
--left join VC_ComWebAdrFix z on x.ID = z.ComId
--where x.Docs is not null
--where y.clnt_id = 77628
order by [company-name]

select
*
--distinct [company-owners]
from VC_Com
order by cast([company-externalId] as int)
--where
--[company-externalId] = 10049--14066
--[company-website] like 'x%'

--select
----top(1)
----*
--[company-externalId] as ComId
--, [company-locationCountry] ComCountry
--from VC_Com
----where
----[company-owners] = 'Darren'
--where [company-locationCountry] <> 'GB' and [company-locationCountry] <> ''
--order by [company-externalId]

--update VC_Com
--set [company-owners] = 'darren.buckley@aj-chambers.com'
--where
--[company-owners] = 'Darren'

--select * from VC_Con
--where [contact-companyId] not in (
--	select [company-externalId]
--	from VC_Com
--)
--alter table [dbo].[VC_Com]
--alter column [company-externalId] nvarchar(255)
--go
--insert into VC_Com values (
--'_DefCom000'
--, '[Default Company]'
--, 'UK'
--, '', '', '', '', 'GB', '', '', '', '', 'External ID: _DefCom000', ''
--)

--update VC_Com
--set [company-locationCountry] = 'GB'
--where [company-locationCountry] = 'UK'