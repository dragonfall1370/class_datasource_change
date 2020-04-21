--Update Job Close Date for some conditions --DM926
select id, name, head_count_open_date, head_count_close_date, external_id
, latest_status, insert_timestamp
into mike_position_bkup_20190813
from position_description
where name ilike '%spec%' --15543
or head_count_open_date < '2019-05-01' --61170

select distinct latest_status
from position_description
where name ilike '%spec%' --15543
or head_count_open_date < '2019-05-01' --61170

select id, name, head_count_open_date, head_count_close_date, external_id
, latest_status, insert_timestamp
--into mike_position_bkup_20190813
from position_description
where name ilike '%spec%' --15543
or (head_count_open_date < '2019-05-01' and head_count_close_date > now())--21333 --to be updated because jobs already closed

--MAIN SCRIPT
update position_description
set head_count_close_date = '2019-08-12 00:00:00'
where name ilike '%spec%' --15543
or (head_count_open_date < '2019-05-01' and head_count_close_date >= now()) --update 21333 rows