--UPDATE TIMESTAMP with JST time
select id, content
from activity
where id between 240546 and 932471 --288389 rows

select id, content
from activity
where id between 932471 and 1022212 --89742 rows

select id, content
from activity
where id between 1022212 and 1044138 --21927 rows


--UPDATE TIMESTAMP
update activity
set insert_timestamp = insert_timestamp - interval '18 hours'
where id between 240546 and 1044138 --updated 400056


update activity_company ac
set insert_timestamp = a.insert_timestamp
from activity a
where a.id = ac.activity_id
and a.id between 240546 and 1044138 --updated 70956


update activity_candidate ac
set insert_timestamp = a.insert_timestamp
from activity a
where a.id = ac.activity_id
and a.id between 240546 and 1044138 --updated 329100