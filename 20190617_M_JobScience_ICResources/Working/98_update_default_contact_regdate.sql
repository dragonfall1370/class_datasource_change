--UPDATE to pass Default Contact behinds
select insert_timestamp - interval '19 years', * from contact
where last_name ilike '%default%contact%'

update contact
set insert_timestamp = insert_timestamp - interval '19 years'
where last_name ilike '%default%contact%'