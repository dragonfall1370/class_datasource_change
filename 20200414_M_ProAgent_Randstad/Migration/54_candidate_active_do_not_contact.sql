--SET "DO NOT CONTACT" FOR ALL ARCHIVED CANDIDATES
--1--active
--0--passive
--2--do not contact
select id
from candidate
where deleted_timestamp is not NULL

update candidate
set active = 2
, active_reason = '【Archived candidates - DO NOT CONTACT】'
where deleted_timestamp is not NULL