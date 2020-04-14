-->> Contact Full Name Romaji <<--
/* Check contact reference
select [採用担当者ID]
, [採用担当者]
, [フリガナ]
from csv_rec
where 1=1 
--and [採用担当者ID] = 'REC-271650'
--and [採用担当者] = '未登録'
*/

with rename as (select com_ext_id
	, kanji
	, trim(kanji_trans) as kanji_trans
	, con_ext_id
	, trim(furigana_trans) as furigana_trans
	from contact_full_name)


select com_ext_id
	, con_ext_id
	, kanji
	, kanji_trans
	, furigana_trans
	, case 
		when charindex(' ', furigana_trans) > 1 then left(furigana_trans, charindex(' ', furigana_trans) - 1)
		else furigana_trans end as last_name_kana --Furigana: Last name appears first
	, case
		when charindex(' ', furigana_trans) > 1 then trim(right(furigana_trans, len(furigana_trans) - charindex(' ', furigana_trans)))
		else '' end as first_name_kana
from rename
where furigana_trans is not NULL

UNION ALL

select com_ext_id
	, con_ext_id
	, kanji
	, kanji_trans
	, furigana_trans
	, case
		when charindex(' ', kanji_trans) > 1 then trim(right(kanji_trans, len(kanji_trans) - charindex(' ', kanji_trans)))
		else coalesce(kanji_trans, 'No last name') end as last_name_kana --Kanji: Last name appears last
	, case 
		when charindex(' ', kanji_trans) > 1 then left(kanji_trans, charindex(' ', kanji_trans) - 1)
		else '' end as first_name_kana --First name not mandatory
from rename
where furigana_trans is NULL --3436


-->> Candidate Full Name Romaji <<--
/* Check contact reference
select [採用担当者ID]
, [採用担当者]
, [フリガナ]
from csv_rec
where 1=1 
--and [採用担当者ID] = 'REC-271650'
--and [採用担当者] = '未登録'
*/

/* Check contact reference
select [採用担当者ID]
, [採用担当者]
, [フリガナ]
from csv_rec
where 1=1 
--and [採用担当者ID] = 'REC-271650'
--and [採用担当者] = '未登録'
*/

with rename as (select [PANO] as cand_ext_id
	, kanji
	, trim(kanji_trans) as kanji_trans
	, trim(furigana_trans) as furigana_trans
	from candidate_full_name)


select cand_ext_id
	, kanji
	, kanji_trans
	, furigana_trans
	, case 
		when charindex(' ', furigana_trans) > 1 then left(furigana_trans, charindex(' ', furigana_trans) - 1)
		else furigana_trans end as last_name_kana --Furigana: Last name appears first
	, case
		when charindex(' ', furigana_trans) > 1 then trim(right(furigana_trans, len(furigana_trans) - charindex(' ', furigana_trans)))
		else furigana_trans end as first_name_kana
from rename
where furigana_trans is not NULL --166262 rows

UNION ALL

select cand_ext_id
	, kanji
	, kanji_trans
	, furigana_trans
	, case
		when charindex(' ', kanji_trans) > 1 then trim(right(kanji_trans, len(kanji_trans) - charindex(' ', kanji_trans)))
		else coalesce(kanji_trans, 'No last name') end as last_name_kana --Kanji: Last name appears last
	, case 
		when charindex(' ', kanji_trans) > 1 then left(kanji_trans, charindex(' ', kanji_trans) - 1)
		else coalesce(kanji_trans, 'No first name') end as first_name_kana --First name appears first
from rename
where furigana_trans is NULL --19 rows