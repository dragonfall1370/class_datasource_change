--Billing Address (removed)
select [企業 PANO ]
, concat_ws(', '
		, coalesce('〒' + nullif([請求先 〒1],''), NULL)
		, coalesce(nullif([請求先 都道府県1],''), NULL)
		, coalesce(nullif([請求先 住所詳細1],''), NULL)) as location_name
, concat_ws(', '
		, coalesce('〒' + nullif([請求先 〒1],''), NULL)
		, coalesce(nullif([請求先 都道府県1],''), NULL)
		, coalesce(nullif([請求先 住所詳細1],''), NULL)) as location_address
, [請求先 都道府県1] as location_state
, [請求先 〒1] as post_code
, 'BILLING_ADDRESS' as location_type
, concat_ws(char(10)
		, coalesce('[請求先 担当者名]' + nullif([請求先 担当者名1],''), NULL) --contact name
		, coalesce('[TEL] ' + nullif([請求先 TEL1],''), NULL)
		, coalesce('[請求先 部署名]' + nullif([請求先 部署名1],''), NULL) --department name
		, coalesce('[その他請求先]' + nullif([その他請求先],''), NULL)
		) as location_note
, current_timestamp as insert_timestamp
from csv_recf_claim
where coalesce(nullif([請求先 〒1],''), nullif([請求先 都道府県1],''), nullif([請求先 住所詳細1],'')) is not NULL
--coalesce(nullif([請求先 担当者名1],''), nullif([請求先 部署名1],''), nullif([その他請求先],'')) is not NULL

UNION ALL

select [企業 PANO ]
, concat_ws(', '
		, coalesce('〒' + nullif([請求先 〒2],''), NULL)
		, coalesce(nullif([請求先 都道府県2],''), NULL)
		, coalesce(nullif([請求先 住所詳細2],''), NULL)) as location_name
, concat_ws(', '
		, coalesce('〒' + nullif([請求先 〒2],''), NULL)
		, coalesce(nullif([請求先 都道府県2],''), NULL)
		, coalesce(nullif([請求先 住所詳細2],''), NULL)) as location_address
, [請求先 都道府県1] as location_state
, [請求先 〒1] as post_code
, 'BILLING_ADDRESS' as location_type
, concat_ws(char(10)
		, coalesce('[請求先 担当者名]' + nullif([請求先 担当者名2],''), NULL) --contact name
		, coalesce('[TEL] ' + nullif([請求先 TEL2],''), NULL)
		, coalesce('[請求先 部署名]' + nullif([請求先 部署名2],''), NULL) --department name
		) as location_note
, current_timestamp as insert_timestamp
from csv_recf_claim
where coalesce(nullif([請求先 〒2],''), nullif([請求先 都道府県2],''), nullif([請求先 住所詳細2],'')) is not NULL