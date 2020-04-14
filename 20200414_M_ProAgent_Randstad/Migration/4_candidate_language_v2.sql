---Language | VC temp table
create table mike_tmp_pa_languages (
cand_ext_id character varying (100)
, candidate_id bigint
, candlanguage character varying (1000)
, langlevel character varying (1000)
, rn int
)

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

select cand_ext_id
, candlanguage
, langlevel
, row_number() over(partition by cand_ext_id order by candlanguage desc) rn
from cand_lang


---BUILDING JSON
SELECT candidate_id
, json_agg(row_to_json((
        SELECT ColumnName 
			FROM ( SELECT candlanguage, langlevel) 
            AS ColumnName ("languageCode", "level")
        ))  order by rn asc) AS language_json
FROM mike_tmp_pa_languages
--where candidate_id = 264827 --checking
GROUP BY candidate_id


---UPDATE VC LANGUAGES
update candidate c
set skill_details_json = m.language_json::text
from (SELECT candidate_id
			, json_agg(row_to_json((
					SELECT ColumnName 
						FROM ( SELECT candlanguage, langlevel) 
						AS ColumnName ("languageCode", "level")
					))  order by rn asc) AS json
			FROM mike_tmp_pa_languages
			--where candidate_id = 264827 --checking
			GROUP BY candidate_id) m
where m.candidate_id = c.id