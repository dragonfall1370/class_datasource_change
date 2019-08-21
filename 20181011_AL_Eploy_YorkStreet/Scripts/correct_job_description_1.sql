select * from candidate
where id = 63499

update position_description
set full_description = note

update position_description
set
full_description = regexp_replace(note, e'[\\n]{1}', '<br>', 'g' )

update position_description
set note = note || char(10) || isnull(