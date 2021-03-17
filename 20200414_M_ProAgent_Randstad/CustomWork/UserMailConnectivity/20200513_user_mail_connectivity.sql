with userlist as (select u.id
	, u.name
	, u.email
	--, u.mail_status
	, s.status
	, s.insert_timestamp
	, row_number() over(partition by u.id, s.email order by s.insert_timestamp desc) rn
	from user_account u
	left join (select * from system_log where action_type = 'MAIL_CONNECTION') s on trim(s.email) = trim(u.email)
	where 1=1
	and u.deleted_timestamp is NULL
	and u.locked_user = 0
)

select *
from userlist
where rn = 1
and id not in (-10, 28969)
order by id 