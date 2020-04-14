--VC TEMP TABLE FOR CANDIDATE MERGED
CREATE TABLE mike_tmp_pa_candidate_merged
(candidate_id bigint
, cand_ext_id character varying (1000)
, reg_date timestamp
, update_date timestamp
, primary_email character varying (1000)
, work_email character varying (1000)
, update_by character varying (1000)
, update_by_user character varying (1000)
, candidate_owner_id character varying (1000)
, candidate_owner character varying (1000)
, registration_route character varying (1000)
)

--PA MAIN SCRIPT
select [PANO ] cand_ext_id
, convert(datetime, [登録日], 120) reg_date
, convert(datetime, [更新日], 120) update_date
, [メール] primary_email
, [携帯メール] work_email
, [更新者ユーザID] update_by
, [更新者] update_by_user
, [人材担当ユーザID] as candidate_owner_id --user ID
, [人材担当] candidate_owner
, [登録経路] registration_route
from csv_can
where [チェック項目] not like '%チャレンジド人材%'