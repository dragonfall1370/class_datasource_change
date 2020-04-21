select id as cand_ext_id
, 'Profile' as title
, trim(profile_tab_c) as note
, now() as insert_timestamp
from contact
where recordtypeid IN ('0120Y0000013O5c','0120Y000000RZZV')
and profile_tab_c is not NULL