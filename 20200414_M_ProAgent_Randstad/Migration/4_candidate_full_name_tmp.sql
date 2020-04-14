/* VC candidate full name tmp table
create table mike_tmp_candidate_full_name
(cand_ext_id character varying (100)
, candidate_id bigint
, candidate_kanji character varying (1000)
, candidate_furigana character varying (1000)
)

*/

select [PANO ] as cand_ext_id
, [氏名] as candidate_kanji
, [フリガナ] as candidate_furigana
from csv_can


--UPDATE CANDIDATE NAME
update candidate c
set first_name = '　'
, last_name = m.candidate_kanji
, first_name_kana = '　' 
, last_name_kana = m.candidate_furigana
from mike_tmp_candidate_full_name m
where m.candidate_id = c.id -- rows