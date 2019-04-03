select 'add_cand_info' as additional_type,
a.id,
case when c.id = 1 then 'Black African'
when c.id = 2 then 'Permit Holders'
when c.id = 3 then 'White'
when c.id = 4 then 'Coloured'
when c.id = 5 then 'Asian'
when c.id = 6 then 'NONE'
when c.id = 7 then 'Open to all'
when c.id = 8 then 'Chinese'
when c.id = 10 then 'Permanent Resident'
when c.id = 9 then 'NONE' else ''
end as 'custom_value',
1005 as form_id,
1015 as field_id,
GETDATE() as insert_timestamp
from VUser a
left join CndProfInfo b on a.id = b.eestatus
left join EEStatus c on b.eestatus = c.id
where b.eestatus is not null


select a.field_value, title_language_code, b.translate from configurable_form_field_value a left join configurable_form_language b on a.title_language_code = b.language_code