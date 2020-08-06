--#Inject | Work History Summary --Candidate Work History
select [PANO ] as cand_ext_id
, replace(concat_ws('<br/>'
	, coalesce('<strong>【事業内容1】</strong>' + '<br/>' + nullif([勤務歴 事業内容1], ''), NULL) --business content 1
	, coalesce('<strong>【事業内容2】</strong>'  + '<br/>' + nullif([勤務歴 事業内容2], ''), NULL) --business content 2
	--, coalesce('【雇用形態1】' + nullif([勤務歴 雇用形態1], '') + '<br/>', NULL) --employment status 1 --removed on 20200210
	--, coalesce('【雇用形態2】' + nullif([勤務歴 雇用形態2], '') + '<br/>', NULL) --employment status 2 --removed on 20200210
	, coalesce('<strong>【その他勤務歴】</strong>'  + '<br/>' + nullif(勤務歴メモ, ''), NULL) --other work history
	, coalesce('<br/>' + nullif(その他勤務歴, '') + '<br/>', NULL) --other business content
	, coalesce('<br/>' + nullif(trim([自己ＰＲ]),''), NULL) --Self PR
	), char(10), '<br/>') as work_history_summary
from csv_can
where coalesce(nullif([勤務歴 事業内容1], ''), nullif([勤務歴 事業内容2], '')
		, nullif([勤務歴 雇用形態1], ''), nullif([勤務歴 雇用形態2], '')
		, nullif(その他勤務歴, ''), nullif(勤務歴メモ, ''), nullif([自己ＰＲ], '')) is not NULL;


--Work history summary | VC temp table | if running slow from DB local
create table mike_tmp_work_history_summary (
cand_ext_id character varying (100)
, candidate_id bigint
, work_history_summary text
)

--Update from VC
update candidate c
set experience = m.work_history_summary
from mike_tmp_work_history_summary m
where m.candidate_id = c.id --140284 rows