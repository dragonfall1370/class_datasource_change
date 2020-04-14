--#CF | Business Details | Text Area
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1002 as field_id
	, [事業内容] as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([事業内容], ''), NULL) is not NULL


--#CF | Business Characteristics | Text Area
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1022 as field_id
	, [企業の特徴] as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([企業の特徴], ''), NULL) is not NULL


--#CF | Listed Stock Market | Drop down
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1015 as field_id
	, case [株式公開 New]
		when '未公開' then 'Not Listed'
		when '東証一部' then '東証一部・二部'
		when '東証二部' then '東証一部・二部'
		when '大証一部' then '大証一部・二部'
		when '大証二部' then '大証一部・二部'
		when '名証一部' then '名証一部・二部'
		when '名証二部' then '名証一部・二部'
		when 'マザーズ' then 'マザーズ'
		when 'ヘラクレス' then 'ヘラクレス'
		when 'JASDAQ' then 'JASDAQ'
		when '名古屋セントレックス' then 'セントレックス'
		when '札証' then '札証'
		when '札幌アンビシャス' then '札幌アンビシャス'
		when '福証' then '福証'
		when '福岡Q-Board' then '福岡Q-Board'
		when 'その他取引所' then 'その他取引所'
		when '上場' then '上場'
		when '店頭公開' then '店頭公開'
		when '不明' then '不明'
	else NULL end as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where nullif([株式公開 New],'') is not NULL --5488 rows


--#CF | Fiscal Closing Month | Drop down
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1026 as field_id
	, trim([決算月]) as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([決算月], ''), NULL) is not NULL