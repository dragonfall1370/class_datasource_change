CREATE TABLE mike_tmp_pa_contact_merged
(company_id bigint
, com_ext_id character varying (1000)
, contact_id bigint
, con_ext_id character varying (1000)
, contact_name character varying (1000)
, contact_name_kana character varying (1000)
, contact_email character varying (1000)
, reg_date timestamp
, update_date timestamp
, update_by character varying (1000)
, update_by_user character varying (1000)
)

--PA MAIN SCRIPT
select [企業 PANO ] com_ext_id
, [採用担当者ID] con_ext_id
, [採用担当者] contact_name
, [フリガナ] contact_name_kana
, [メール] contact_email
, convert(datetime, [登録日], 120) reg_date
, convert(datetime, [更新日], 120) update_date
, [更新者ユーザID] update_by
, [更新者] update_by_user
from csv_rec