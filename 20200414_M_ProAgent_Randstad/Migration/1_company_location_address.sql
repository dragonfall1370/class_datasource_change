select [PANO ] as com_ext_id
, coalesce(nullif([企業拠点名1], ''), '企業拠点名') as location_name --tbc if using this field as location_name | updated 20200218
--, concat_ws(', '
--	--, coalesce('〒' + nullif([所在地 〒1], ''), NULL)
--	, coalesce(nullif([所在地 都道府県1], ''), NULL)
--	, coalesce(nullif([所在地 住所詳細1], ''), NULL)
--	) as location_name
, concat_ws(', '
	--, coalesce('〒' + nullif([所在地 〒1], ''), NULL)
	, coalesce(nullif([所在地 都道府県1], ''), NULL)
	, coalesce(nullif([所在地 住所詳細1], ''), NULL)
	) as location_address
, [所在地 都道府県1] as location_state
, [所在地 〒1] as post_code
, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([所在地 都道府県1], ''), nullif([所在地 〒1], ''), nullif([所在地 住所詳細1], '')) is not NULL

UNION ALL

select [PANO ]
, coalesce(nullif([企業拠点名2], ''), '企業拠点名') as location_name
--, concat_ws(', '
--	--, coalesce('〒' + nullif([所在地 〒2], ''), NULL)
--	, coalesce(nullif([所在地 都道府県2], ''), NULL)
--	, coalesce(nullif([所在地 住所詳細2], ''), NULL)
--	) as location_name
, concat_ws(', '
	--, coalesce('〒' + nullif([所在地 〒2], ''), NULL)
	, coalesce(nullif([所在地 都道府県2], ''), NULL)
	, coalesce(nullif([所在地 住所詳細2], ''), NULL)
	) as location_address
, [所在地 都道府県2] as location_state
, [所在地 〒2] as post_code
, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([所在地 都道府県2], ''), nullif([所在地 〒2], ''), nullif([所在地 住所詳細2], '')) is not NULL
--total 29242