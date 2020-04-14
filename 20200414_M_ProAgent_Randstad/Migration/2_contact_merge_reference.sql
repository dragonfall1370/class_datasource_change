--CONTACT MERGING CHECK REFERENCES
select c.id, c.first_name, c.last_name
, c.first_name_kana
, c.last_name_kana
, c.email as contact_email
, c.company_id
, c.insert_timestamp
--, co.user_id
--, u.name as user_name
--, u.email as user_email
--, co.index
from contact c
--join contact_owner co on co.contact_id = c.id
--join (select id, name, email, first_name, last_name from user_account where deleted_timestamp is NULL and locked_user = 0) u on u.id = co.user_id
where c.deleted_timestamp is NULL
and c.id = 24000

--Contact counts
select count(*)
from contact
where deleted_timestamp is not NULL --1081

select id, first_name, last_name, email as contact_email, deleted_timestamp
from contact
where id = 24000

--133 contacts having >1 owner
select contact_id, count(*)
from contact_owner
group by contact_id
having count(*) > 1

select * from contact_owner
where contact_id = 24000

select *
from user_account