with com_address as (select [PANO ]
	, [所在地 都道府県1]
	, [所在地 〒1]
	, [所在地 住所詳細1]
	, [所在地 都道府県2]
	, [所在地 〒2]
	, [所在地 住所詳細2]
	, [その他所在地] --additional addresses
	, replace(replace(replace([その他所在地], '[企業拠点名]', '|'), '[所在地〒]' , '*'), '[所在地住所]', '\') as alt_addr
	from csv_recf
	where 1=1
	--and [PANO ] = 'CPY000463'
	--and [その他所在地] <> ''
)

, alt_addr as (select [PANO ]
	, value as alt_addr
	from com_address
	cross apply string_split(alt_addr, '|')
	where alt_addr <> '')

/* 546 rows
select * from com_address
where [その他所在地] <> ''
*/

--MAIN SCRIPT
select [PANO ] as com_ext_id
, alt_addr
, case when charindex('\', alt_addr) - charindex('*', alt_addr) < 2 then replace(replace(alt_addr, '\', ' '), '*', ' ')
	else trim(replace(replace(alt_addr, '\', ' '), '*', '〒')) end as original_location_name
, left(alt_addr, charindex('*', alt_addr) - 1) as location_name
, case when charindex('\', alt_addr) - charindex('*', alt_addr) < 2 then right(alt_addr, len(alt_addr) - charindex('\', alt_addr))
	else replace(replace(right(alt_addr, len(alt_addr) - charindex('*', alt_addr) + 1), '*', '〒'), '\', ' ') end as location_address
, substring(alt_addr, charindex('*', alt_addr) + 1, charindex('\', alt_addr) - charindex('*', alt_addr) - 1) as post_code
--, replace(replace(alt_addr, '\', ' '), '*', '〒') as location_name
--, right(alt_addr, charindex('\', reverse(alt_addr)) - 1) as location_detail
from alt_addr
where alt_addr <> '' and alt_addr is not NULL
--and com_ext_id = 'CPY018148'