--MAIN SCRIPT
select id
, name
, email
, mail_status
, mail_refresh_date
from user_account
where 1=1
--and name ilike '%hoshino%'
and deleted_timestamp is NULL and locked_user = 0
and id not in (-10, 28969)

--CHECKLIST
select *
from user_account
where id = -10

select *
from system_log
where action_type = 'MAIL_CONNECTION'
order by id desc