--candidate work history
/* Currently working? > working_state
1 Current employed
2 Not employed
3 At school */

--Currently working #Inject later
select [PANO ]
, 現在の状況
, case when 現在の状況 = '現職中' then 1
	when 現在の状況 = '離職中' then 2
	when 現在の状況 = '就業経験なし' then 2
	else NULL end as currently_working
from csv_can
where 現在の状況 <> ''

--#Inject | Work History Summary --Candidate Work History
select [PANO ] as cand_ext_id
, concat_ws('<br/>'
	, coalesce('【事業内容1】' + nullif([勤務歴 事業内容1], '') + '<br/>', NULL) --business content 1
	, coalesce('【事業内容2】' + nullif([勤務歴 事業内容2], '') + '<br/>', NULL) --business content 2
	--, coalesce('【雇用形態1】' + nullif([勤務歴 雇用形態1], '') + '<br/>', NULL) --employment status 1
	--, coalesce('【雇用形態2】' + nullif([勤務歴 雇用形態2], '') + '<br/>', NULL) --employment status 2
	, coalesce('【勤務歴メモ】' + '<br/>' + nullif(replace(勤務歴メモ, char(10), '<br/>'), ''), NULL) --other work history
	, coalesce('【その他勤務歴】' + '<br/>' + nullif(replace(その他勤務歴, char(10), '<br/>'), '') + '<br/>', NULL) --other business content
	, coalesce('[自己ＰＲ]' + nullif(trim([自己ＰＲ]),''), NULL) --Self PR
	) as work_history_summary
from csv_can
where coalesce(nullif([勤務歴 事業内容1], ''), nullif([勤務歴 事業内容2], '')
		, nullif([勤務歴 雇用形態1], ''), nullif([勤務歴 雇用形態2], '')
		, nullif(その他勤務歴, ''), nullif(勤務歴メモ, ''), nullif([自己ＰＲ], '')) is not NULL
--and [PANO ] in ('CDT248646', 'CDT249650')


--#Inject | Number of previous employers
select [PANO ] as cand_ext_id
, 転職回数 + 1 as no_previous_employer
from csv_can


--MAIN SCRIPT
select [PANO ]
, [勤務歴 勤務期間 開始年1] --period year from
, [勤務歴 勤務期間 開始月1] --period month from
, [勤務歴 勤務期間 終了年1] --period year to
, [勤務歴 勤務期間 終了月1] --period month to
, [勤務歴 会社名1] as employer1 --company name
, [勤務歴 役職名1] as job_title1 --job title
, [勤務歴 雇用形態1] as employmentstatus1 --employment status
, [勤務歴 担当職務1] as experience1 --experience
, iif(len([勤務歴 勤務期間 開始年1]) < 4, NULL, convert(date
		, coalesce(nullif(nullif([勤務歴 勤務期間 開始年1],''), '0') 
			+ coalesce(nullif([勤務歴 勤務期間 開始月1],''), '01') + '01', NULL), 120)) as startdate1
, iif(len([勤務歴 勤務期間 終了年1]) < 4, NULL, eomonth(convert(date
		, coalesce(nullif(nullif([勤務歴 勤務期間 終了年1],''), '0') 
			+ coalesce(nullif([勤務歴 勤務期間 終了月1],''), '01') + '01', NULL), 120))) as enddate1
, [勤務歴 勤務期間 開始年2] --period year from
, [勤務歴 勤務期間 開始月2] --period month from
, [勤務歴 勤務期間 終了年2] --period year to
, [勤務歴 勤務期間 終了月2] --period month to
, [勤務歴 会社名2] as employer2 --company name
, [勤務歴 役職名2] as job_title2 --job title
, [勤務歴 雇用形態2] as employmentstatus2 --employment status
, [勤務歴 担当職務2] as experience2 --experience
, iif(len([勤務歴 勤務期間 開始年2]) < 4, NULL, convert(date
		, coalesce(nullif(nullif([勤務歴 勤務期間 開始年2],''), '0') 
			+ coalesce(nullif(nullif([勤務歴 勤務期間 開始月2],''), '00'), '01') + '01', NULL), 120)) as startdate2
, iif(len([勤務歴 勤務期間 終了年2]) < 4, NULL, eomonth(convert(date
		, coalesce(nullif(nullif([勤務歴 勤務期間 終了年2],''), '0') 
			+ coalesce(nullif(nullif([勤務歴 勤務期間 終了月2],''), '00'), '01') + '01', NULL), 120))) as enddate2
from csv_can