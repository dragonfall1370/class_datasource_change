select

cast(ContactID as varchar(20)) as ConExtID

, iif(len(trim(isnull(Mobile, ''))) > 0
	, concat(trim(isnull(Mobile, ''))
		, iif(len(trim(isnull(PersonalMobile, ''))) = 0
			, ''
			, concat(',', trim(isnull(PersonalMobile, '')))
		)
	)
	, trim(isnull(PersonalMobile, ''))
) as mobile_phone

, trim(isnull(PersonalTelephone, '')) as home_phone

, trim(isnull(PersonalEmail, '')) as personal_email

, [dbo].[ufn_PopulateLocationAddressUK](
	x.Address1
	, x.Address2
	, x.Address3
	, x.Town
	, x.County
	, x.PostCode
	, cs.Description
	, '., '
) as [address]

, [dbo].[ufn_TrimSpecifiedCharacters](x.Town, '., ') as city

, [dbo].[ufn_TrimSpecifiedCharacters](x.County, '., ') as [state]

, vcs.Code as country_code

, [dbo].[ufn_TrimSpecifiedCharacters](x.Postcode, '., ') as post_code
from Contacts x
left join Countries cs on x.CountryID = cs.CountryID
left join VCCountries vcs on
	iif(lower(trim(isnull(cs.Description, ''))) = 'uk', 'gb', lower(trim(isnull(cs.Description, '')))) = lower(trim(vcs.Name))
	or iif(lower(trim(isnull(cs.Description, ''))) = 'uk', 'gb', lower(trim(isnull(cs.Description, '')))) = lower(trim(vcs.Code))


select * from Contacts
where ContactID = 818