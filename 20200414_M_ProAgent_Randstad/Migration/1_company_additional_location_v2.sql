declare 
	@string1 nvarchar(50) = N'[企業拠点名]', --location name
	@string2 nvarchar(50) = N'[所在地〒]', --location zip code
	@string3 nvarchar(50) = N'[所在地住所]', --location address
	@splitvalue nchar(1) = nchar(9999);


with com_address as (select [PANO ] as com_ext_id
	, [その他所在地] --additional addresses
	, value as com_address
	from csv_recf
	cross apply string_split(replace([その他所在地], @string1, @splitvalue), @splitvalue)
	where [その他所在地] <> ''
)

, alt_addr as (select com_ext_id
	, com_address
	, left(com_address, charindex(@string2, com_address)- 1) as location_name
	, substring(com_address
				, charindex(@string2, com_address) + len(@string2)
				, charindex(@string3, com_address) - charindex(@string2, com_address) - len(@string2)) as post_code
	, case when len(com_address) - charindex(@string3, com_address) - len(@string3) + 1 > 0
			then right(com_address, len(com_address) - charindex(@string3, com_address) - len(@string3) + 1)
			else NULL end as location_address
	from com_address
	where com_address <> '')

select com_ext_id
, coalesce(nullif(location_name, ''), location_address) as location_name
, concat_ws(', '
	, coalesce('〒' + nullif(post_code, ''), NULL)
	, coalesce(nullif(location_name, ''), NULL)
	, coalesce(nullif(location_address, ''), NULL)
	) as location_address_full
, nullif(location_address, '') as location_address
, post_code
, current_timestamp as insert_timestamp
from alt_addr