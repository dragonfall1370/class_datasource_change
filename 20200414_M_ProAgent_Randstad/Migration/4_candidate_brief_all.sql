with cand_status as (select [PANO] as cand_id
		, string_agg(concat_ws('<br/>'
			, coalesce('【ポジション名】' + nullif([ポジション名], ''), NULL)
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
			, 状況メモ as status_note
			, 登録日 as status_date
			from csv_status_my_can
			where nullif([PANO],'') is not NULL and nullif(状況メモ, '') is not NULL)

, cand_status_note as (select cand_id
		, string_agg(status_note, '<br/>') within group (order by status_date desc) as status_note
		from status_note
		group by cand_id)

--MAIN SCRIPT
, cand_brief as (select c.[PANO ] as cand_ext_id
	, concat_ws('<br/>'
			, coalesce('【社内データメモ】' + nullif(c.[社内データメモ],''), NULL) --Internal data memo
			, coalesce('【状況メモ】' + nullif(sn.status_note,''), NULL) --Status note
			--, coalesce('<br/>' + '【概要】' + '<br/>' + nullif(cs.cand_status,''), NULL) --Candidate status --removed 20200103
			, coalesce('<br/>' + '【連絡希望事項】' + nullif(c.連絡希望事項, ''), NULL) --Contact request item
			, coalesce('<br/>' + '【免許・資格メモ】' + nullif(c.免許・資格メモ, ''), NULL) --License Qualification
			, coalesce('【条件メモ】' + nullif(c.条件メモ, ''), NULL) --Condition note
			, coalesce('【特筆すべき事項】' + nullif(c.[特筆すべき事項], ''), NULL) --Matters to be noted
			, coalesce('【評価1】' + nullif(c.[評価1], ''), NULL) --Evaluation 1
			, coalesce('【評価1メモ】' + nullif(c.[評価1メモ], ''), NULL) --Eva 1 memo
			, coalesce('【評価2】' + nullif(c.[評価2], ''), NULL) --Evaluation 2
			, coalesce('【評価2メモ】' + nullif(c.[評価2メモ], ''), NULL) --Eva 2 memo
			, coalesce('【経歴メモ】' + nullif(c.[経歴メモ], '') + '<br/>', NULL) --Career note
	--Academic background
			, coalesce('【学歴1】' + '<br/>' + nullif(concat_ws('<br/>'
				, coalesce('【Period】' + nullif(concat_ws(' - '
						, coalesce(nullif([入学年月 年1], '')  + '/' + nullif([入学年月 月1], ''), NULL) --period from
						, coalesce(nullif([卒業年月 年1], '')  + '/' + nullif([卒業年月 月1], ''), NULL)), ''), NULL) --period to
					, coalesce('【Classification of withdrawal】' + nullif([卒退区分1], ''), NULL) --classification 1
					, coalesce('【Education category】' + nullif([学歴区分1], ''), NULL) --education category 1
					, coalesce('【School name】' + nullif([学校名 学部名 学科名1], ''), NULL) --school name / department name 1
					, coalesce('【Educational note】' + nullif([学歴メモ1], ''), NULL)--educational note	
					), ''), NULL)
			, coalesce('<br/>' + '【学歴2】' + '<br/>' + nullif(concat_ws('<br/>'
				, coalesce('【Period】' + nullif(concat_ws(' - '
						, coalesce(nullif([入学年月 年2], '')  + '/' + nullif([入学年月 月2], ''), NULL) --period from
						, coalesce(nullif([卒業年月 年2], '')  + '/' + nullif([卒業年月 月2], ''), NULL)), ''), NULL) --period to
					, coalesce('【Classification of withdrawal】' + nullif([卒退区分2], ''), NULL) --classification 1
					, coalesce('【Education category】' + nullif([学歴区分2], ''), NULL) --education category 1
					, coalesce('【School name】' + nullif([学校名 学部名 学科名2], ''), NULL) --school name / department name 1
					, coalesce('【Educational note】' + nullif([学歴メモ2], ''), NULL)--educational note	
					), ''), NULL)
			, coalesce('<br/>' + '【その他学歴】' + '<br/>' + nullif([その他学歴], ''), NULL)
			) as candidate_brief
	from csv_can c
	--left join cand_status cs on cs.cand_id = c.[PANO ]
	left join cand_status_note sn on sn.cand_id = c.[PANO ]
	where 1=1
	--and c.[PANO ] = 'CDT100072'
	)

select *
from cand_brief
where candidate_brief <> ''

/* VC temp table

create table mike_tmp_candidate_brief
(cand_ext_id character varying (100)
, candidate_id bigint
, candidate_brief text)

-->> Update candidate brief
update candidate c
set note = m.candidate_brief
from mike_tmp_candidate_brief m
where m.candidate_id = c.id

*/