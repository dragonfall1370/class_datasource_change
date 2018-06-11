-- get user emails and full name: run create view one by one
create view user_emails as
(select distinct(u.id), ea.email_address
from email_addr_bean_rel eabr
 left join email_addresses ea on eabr.email_address_id = ea.id
 left join users u on eabr.bean_id = u.id
 where u.id is not null)
 
create view user_emails_2 as
(select u.*, count(*) as rn
from user_emails u join user_emails u1 on u.email_address = u1.email_address
and u.id >= u1.id
group by u.id, u.email_address order by u.email_address)
-- select * from user_emails_2

create view user_emails_main as
(select id,
case 
when rn=1 then email_address
else concat(rn,'_',(email_address))
end as email_address
from user_emails_2)
-- select * from user_emails_main

create view user_info_view as
(select u.id, u.first_name, u.last_name, uea.email_address, if(u.first_name is null, u.last_name, if(u.first_name=u.last_name,u.first_name,concat(u.first_name,' ',u.last_name))) as username
from users u left join user_emails_main uea on u.id = uea.id)

select * from user_info_view
