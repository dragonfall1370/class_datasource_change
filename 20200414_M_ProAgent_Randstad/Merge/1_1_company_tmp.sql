--VC TEMP TABLE FOR COMPANY MERGED
CREATE TABLE mike_tmp_pa_company_merged
(company_id bigint
, com_ext_id character varying (1000)
, company_name character varying (1000)
, reg_date timestamp
, update_date timestamp
, update_by character varying (1000)
, update_by_user character varying (1000)
, company_owner_id character varying (1000)
, company_owner character varying (1000)
)

--PA MAIN SCRIPT
select [PANO ] com_ext_id
, convert(datetime, [登録日], 120) reg_date
, convert(datetime, [更新日], 120) update_date
, [会社名] company_name
, [更新者ユーザID] update_by
, [更新者] update_by_user
, [企業担当ユーザID] company_owner_id
, [企業担当] company_owner
from csv_recf