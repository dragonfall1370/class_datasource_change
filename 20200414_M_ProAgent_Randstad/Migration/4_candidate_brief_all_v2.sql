with cand_status as (select [PANO] as cand_id
		, string_agg(concat_ws('<br/>'
			--, coalesce('【ポジション名】' + nullif([ポジション名], ''), NULL) --removed from 20200204
			, coalesce('【Open (紹介中)】' + nullif([Open (紹介中)], ''), NULL)
			, coalesce('【Placement (決定)】' + nullif([Placement (決定)], ''), NULL)
			, coalesce('【Close (紹介終了)】' + nullif([Close (紹介終了)], ''), NULL)
			, coalesce('【Other (その他)】' + nullif([Other (その他)], ''), NULL)
			), '<br/>') 
			within group (order by coalesce([Open (紹介中)], [Placement (決定)], [Close (紹介終了)], [Other (その他)]) desc) as cand_status
		from csv_status_my_can
		where [PANO] is not NULL
		and coalesce(nullif([Open (紹介中)],''), nullif([Placement (決定)],''), nullif([Close (紹介終了)],''), nullif([Other (その他)],'')) is not NULL
		group by [PANO])

, status_note as (select distinct [PANO] as cand_id
		, string_agg(状況メモ, '<br/>') within group (order by 登録日 desc) as status_note
		from csv_status_my_can
		where nullif([PANO],'') is not NULL
		and nullif(状況メモ, '') is not NULL
		group by [PANO])

--MAIN SCRIPT
select c.[PANO ] as cand_ext_id
, replace(concat_ws('<br/>'
		, coalesce('【社内データメモ】' + '<br/>' + nullif(c.[社内データメモ],''), NULL) --Internal data memo
		, coalesce('【状況メモ】' + '<br/>' + nullif(sn.status_note,''), NULL) --Status note
		--, coalesce('<br/>' + '【概要】' + '<br/>' + nullif(cs.cand_status,''), NULL) --Candidate status | removed from Brief >> New brief tab
		, coalesce('<br/>' + '【連絡希望事項】' + '<br/>' + nullif(c.連絡希望事項, ''), NULL) --Contact request item
		, coalesce('<br/>' + '【免許・資格メモ】' + '<br/>' + nullif(c.免許・資格メモ, ''), NULL) --License Qualification
		, coalesce('【条件メモ】' + '<br/>' + nullif(c.条件メモ, ''), NULL) --Condition note
		, coalesce('【特筆すべき事項】' + '<br/>' + nullif(c.[特筆すべき事項], ''), NULL) --Matters to be noted
		, coalesce('【評価1】' + '<br/>' + nullif(c.[評価1], ''), NULL) --Evaluation 1
		, coalesce('【評価1メモ】' + '<br/>' + nullif(c.[評価1メモ], ''), NULL) --Eva 1 memo
		, coalesce('【評価2】' + '<br/>' + nullif(c.[評価2], ''), NULL) --Evaluation 2
		, coalesce('【評価2メモ】' + '<br/>' + nullif(c.[評価2メモ], ''), NULL) --Eva 2 memo
		, coalesce('【経歴メモ】' + '<br/>' + nullif(c.[経歴メモ], ''), NULL) --Career note
		, '<br/>'
--Academic background
		, coalesce('【学歴1】' + '<br/>' + nullif(concat_ws('<br/>'
			, coalesce('【Period】' + nullif(concat_ws(' - '
				, coalesce(nullif([入学年月 年1], '')  + '/' + nullif([入学年月 月1], ''), NULL) --period from
				, coalesce(nullif([卒業年月 年1], '')  + '/' + nullif([卒業年月 月1], ''), NULL)), ''), NULL) --period to
				, coalesce('【Classification of withdrawal】' + '<br/>' + nullif([卒退区分1], ''), NULL) --classification 1
				, coalesce('【Education category】' + '<br/>' + nullif([学歴区分1], ''), NULL) --education category 1
				, coalesce('【School name】' + '<br/>' + nullif([学校名 学部名 学科名1], ''), NULL) --school name / department name 1
				, coalesce('【Educational note】' + '<br/>' + nullif([学歴メモ1], ''), NULL)--educational note	
				), ''), NULL)
		, coalesce('<br/>' + '【学歴2】' + '<br/>' + nullif(concat_ws('<br/>'
			, coalesce('【Period】' + nullif(concat_ws(' - '
				, coalesce(nullif([入学年月 年2], '')  + '/' + nullif([入学年月 月2], ''), NULL) --period from
				, coalesce(nullif([卒業年月 年2], '')  + '/' + nullif([卒業年月 月2], ''), NULL)), ''), NULL) --period to
				, coalesce('【Classification of withdrawal】' + '<br/>' + nullif([卒退区分2], ''), NULL) --classification 1
				, coalesce('【Education category】' + '<br/>' + nullif([学歴区分2], ''), NULL) --education category 1
				, coalesce('【School name】' + '<br/>' + nullif([学校名 学部名 学科名2], ''), NULL) --school name / department name 1
				, coalesce('【Educational note】' + '<br/>' + nullif([学歴メモ2], ''), NULL)--educational note	
				), ''), NULL)
		, coalesce('<br/>' + '【その他学歴】' + '<br/>' + nullif([その他学歴], ''), NULL)
		), char(10), '<br/>') as candidate_brief
from csv_can c
left join cand_status cs on cs.cand_id = c.[PANO ]
left join status_note sn on sn.cand_id = c.[PANO ]