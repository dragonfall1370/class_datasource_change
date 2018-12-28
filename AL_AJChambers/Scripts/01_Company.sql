drop table if exists VC_Com

select

cast(x.ID as varchar(255)) as [company-externalId]

, trim(isnull(x.Name, '')) as [company-name]

, trim(isnull(x.LocName, '')) as [company-locationName]

, trim(isnull(x.Address, '')) as [company-locationAddress]

, trim(isnull(y.city, '')) as [company-locationCity]
--, db-field-not-found as [company-locationDistrict]

, trim(isnull(y.county, '')) as [company-locationState]

, case(lower(trim(isnull(y.COUNTRY, ''))))
	when lower('UK') then 'GB'
	when lower('Scotland') then 'GB'
	when lower('NULL') then 'GB'
	when lower('England') then 'GB'
	when lower('United Kingdom') then 'GB'
	when lower('London') then 'GB'
	when lower('Wales') then 'GB'
	when lower('CB22 5LZ') then 'GB'
	when lower('0') then 'GB'
	when lower('NE33 4PU') then 'GB'
	when lower('Hertfordshire') then 'GB'
	when lower('Spain') then 'ES'
	when lower('Cambridgeshire') then 'GB'
	else 'GB'
end as [company-locationCountry]

, trim(isnull(y.postcode, '')) as [company-locationZipCode]
--, db-field-not-found as [company-nearestTrainStation]
--, db-field-not-found as [company-headQuarter]

, trim(isnull(y.phone, '')) as [company-phone]

, trim(isnull(y.fax, '')) as [company-fax]
--, db-field-not-found as [company-switchBoard]

, dbo.ufn_RefineWebAddress(
	isnull(z.WebAdr, '')
	--case(y.web_addr)
	--	when 'N/A' then ''
	--	when '0' then ''
	--	else trim( '/ ' from y.web_addr)
	--end
) as [company-website]

, lower(dbo.ufn_TrimSpecialCharacters_V2(isnull(nullif(trim(isnull(y.USER_ID, '')), 'Darren'), 'darren.buckley@aj-chambers.com'), '')) as [company-owners]

--01-Dec-2017 12:40:42
, concat(
	concat('External ID:', x.ID)
	, nullif(
		concat(
			'Entered: '
			, FORMAT(dateadd(day, -2, cast(y.entered as datetime)), 'dd-MMM-yyyy H:mm:ss', 'en-US')
			, char(10)
			, char(10)
		)
		, concat('Entered: ', char(10), char(10))
	)
	, nullif(
		concat('::::::: Alternate address :::::::'
			, char(10)
			, replace(
				dbo.ufn_PopulateLocationAddress(
					concat(ALTADDRESS1, ', ', ALTADDRESS2, ', ', ALTADDRESS3)
					, ALTCITY
					, ALTCOUNTY
					, ALTPOSTCODE
					, isnull(ALTCOUNTRY, '')
					, ', '
				)
				, ',,'
				, ','
			)
		)
		, concat('::::::: Alternate address :::::::', char(10))
	)
) as [company-note]

--, trim(isnull(convert(varchar(255), cast(y.entered as datetime), 120), '')) as [company-note1]

, x.Docs as [company-document]

into VC_Com

from
VC_ComIdx x
left join CLNTINFO_DATA_TABLE y on x.ID = y.CLNT_ID
left join VC_ComWebAdrFix z on x.ID = z.ComId
--where x.Docs is not null
--where y.clnt_id = 77628
order by [company-name]

select
*
--distinct [company-owners]
from VC_Com
order by [company-externalId]
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