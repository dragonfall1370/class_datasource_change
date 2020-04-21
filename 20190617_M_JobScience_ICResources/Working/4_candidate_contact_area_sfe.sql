--CONTACT AREA SFE
select distinct c.id as contact_id
, 'AREA' as vcfe
, trim(unnest(string_to_array(c.area_c, ';'))) as vcsfe
, current_timestamp as insert_timestamp
from contact c
where c.recordtypeid in ('0120Y0000013O5d')
and area_c is not null and area_c <> '' --16579 rows


--CANDIDATE AREA SFE
select distinct c.id as candidate_id
, 'AREA' as vcfe
, trim(unnest(string_to_array(c.area_c, ';'))) as vcsfe
, current_timestamp as insert_timestamp
from contact c
where c.recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')
and area_c is not null and area_c <> '' --148705 rows