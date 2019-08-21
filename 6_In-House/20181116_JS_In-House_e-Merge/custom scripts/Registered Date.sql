
select 'add_cand_info' as additional_type,
a.id,
b.createdate as 'custom_value',
1005 as form_id,
1015 as field_id,
GETDATE() as insert_timestamp
from VUser a
left join CndPersInfo b on a.id = b.id
where b.createdate is not null or b.createdate <> ''

