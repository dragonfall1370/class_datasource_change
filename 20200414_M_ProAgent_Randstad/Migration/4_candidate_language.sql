--Faster if built in temp table
---Language skills
with cand_lang as (select [PANO ] as cand_ext_id
--, [英語 語学名称] as cand_lang
, 'en' as candlanguage --English language code
, case when [英語 総合] = '初級' then '1' --Beginner
	when [英語 総合] = '中級' then '2' --Intermediate
	when [英語 総合] = '上級' then '4' --Fluent
	else '2' end as langlevel
from csv_can
where nullif([英語 語学名称], '') is not NULL and nullif([英語 総合], '') is not NULL --updated 20200106

UNION ALL

select [PANO ]
, 'ja' as candlanguage
, '5' as langlevel --native
from csv_can)

, t1 as (select [PANO ] as cand_ext_id from csv_can)

select --top 10 
cand_ext_id
, (select candlanguage as languageCode
	, langlevel as level
	from cand_lang cl
	where cl.cand_ext_id = t1.cand_ext_id
	order by candlanguage
	for json path) as languages
from t1
--where cand_ext_id = 'CDT000788'


---#Inject | TOEIC
select [PANO ]
, [英語 TOEIC]
from csv_can
where nullif([英語 TOEIC], '') is not NULL


---#CF | Language skill note | Text Area
select [PANO ] as cand_ext_id
, 'add_cand_info' as additional_type
, 1139 as form_id
, 9999 as field_id
, concat_ws(char(10)
	, coalesce('【TOEFL】' + nullif([英語 TOEFL], ''), NULL)
	, coalesce('【語学力メモ】' + nullif([語学力メモ], ''), NULL)
	) as language_skill_note
from csv_can
where coalesce(nullif([英語 TOEFL], ''), nullif([語学力メモ], '')) is not NULL