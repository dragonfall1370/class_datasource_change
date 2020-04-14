--#New brief tab | Ver2 | 'エントリー履歴'
with cand_status_all as (select distinct [PANO] as cand_id
		, coalesce('【Open (紹介中)】' + '<br/>' + nullif([Open (紹介中)], ''), NULL) as cand_status
		, [Open (紹介中)] as cand_status_date
		, 1 as rn_status
		from csv_status_my_can
		where [PANO] is not NULL
		and nullif([Open (紹介中)],'') is not NULL
		
		UNION ALL
		select distinct [PANO] as cand_id
		, coalesce('【Placement (決定)】' + '<br/>' + nullif([Placement (決定)], ''), NULL) as cand_status
		, [Placement (決定)] as cand_status_date
		, 2 as rn_status
		from csv_status_my_can
		where [PANO] is not NULL
		and nullif([Placement (決定)],'') is not NULL

		UNION ALL
		select distinct [PANO] as cand_id
		, coalesce('【Close (紹介終了)】' + '<br/>' + nullif([Close (紹介終了)], ''), NULL) as cand_status
		, [Close (紹介終了)] as cand_status_date
		, 3 as rn_status
		from csv_status_my_can
		where [PANO] is not NULL
		and nullif([Close (紹介終了)],'') is not NULL

		UNION ALL
		select distinct [PANO] as cand_id
		, coalesce('【Other (その他)】' + '<br/>' + nullif([Other (その他)], ''), NULL) as cand_status
		, [Other (その他)] as cand_status_date
		, 4 as rn_status
		from csv_status_my_can
		where [PANO] is not NULL
		and nullif([Other (その他)],'') is not NULL
		)

/* Checking
select * from cand_status_all
where cand_id = 'CDT127384'
*/
, cand_status as (select cand_id
		, string_agg(cand_status, '<br/>') 
			within group (order by rn_status asc, cand_status_date desc) as cand_status
		from cand_status_all
		where cand_id is not NULL
		group by cand_id)

--MAIN SCRIPT
select c.[PANO ] as cand_ext_id
, 'エントリー履歴' as title
, coalesce('<p>' +
		nullif(concat_ws('<br/>'
			, coalesce('<br/>' + '<strong>【概要】</strong>' + '<br/>' + nullif(cs.cand_status,''), NULL)
			, coalesce('<br/>' + '<strong>【メモ】</strong>' + '<br/>' + nullif(replace(c.[メモ], char(10), '<br/>'),''), NULL)
		), '') + '</p>', NULL) as cand_brief_tab
, current_timestamp as insert_timestamp
, [メモ]
from csv_can c
left join cand_status cs on cs.cand_id = c.[PANO ]
where coalesce(nullif(cs.cand_status, ''), nullif(c.[メモ], '')) is not NULL

/*
--#New brief tab | Entry history (OLD VERSION)
select [PANO ] as cand_ext_id
, 'エントリー履歴' as title
, [メモ] as cand_entry_history
, current_timestamp as insert_timestamp
from csv_can
*/