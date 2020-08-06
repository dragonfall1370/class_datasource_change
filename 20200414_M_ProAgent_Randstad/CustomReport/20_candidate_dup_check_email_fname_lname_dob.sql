with cand_rn as (select id, email, first_name, last_name
	, row_number() over(partition by lower(email), lower(trim(first_name)), lower(trim(last_name)) order by id desc) as rn
	from candidate
	where deleted_timestamp is NULL
	and coalesce(nullif(email, ''), nullif(first_name, ''), nullif(last_name, '')) is not NULL)
	
select *
from cand_rn
where rn > 1 --19 rows

---Dup count on Email-FName-LName
select id, email, first_name, last_name
from candidate
where 1=1
and deleted_timestamp is NULL
and coalesce(nullif(email, ''), nullif(first_name, ''), nullif(last_name, '')) is not NULL
and concat_ws('|', lower(trim(email)), lower(trim(first_name)), lower(trim(last_name))) in
		(select concat_ws('|', lower(trim(email)), lower(trim(first_name)), lower(trim(last_name)))
		from candidate
		where 1=1
		and deleted_timestamp is NULL
		group by concat_ws('|', lower(trim(email)), lower(trim(first_name)), lower(trim(last_name)))
		having count(*) > 1) --35 rows
		
---Dup count on Email-FName-LName-DOB
select id, email, first_name, last_name, date_of_birth
from candidate
where 1=1
and deleted_timestamp is NULL
and coalesce(nullif(email, ''), nullif(first_name, ''), nullif(last_name, ''), date_of_birth::text) is not NULL
and concat_ws('|', lower(trim(email)), lower(trim(first_name)), lower(trim(last_name))) in
		(select concat_ws('|', lower(trim(email)), lower(trim(first_name)), lower(trim(last_name)), date_of_birth::text)
		from candidate
		where 1=1
		and deleted_timestamp is NULL
		group by concat_ws('|', lower(trim(email)), lower(trim(first_name)), lower(trim(last_name)), date_of_birth::text)
		having count(*) > 1) --0 rows
		
--Dup count on Email only
select id, email, first_name, last_name, date_of_birth
from candidate
where 1=1
and deleted_timestamp is NULL
and coalesce(nullif(lower(trim(email)), ''), NULL) is not NULL
and lower(trim(email)) in
		(select lower(trim(email))
		from candidate
		where coalesce(nullif(lower(trim(email)), ''), NULL) is not NULL
		and deleted_timestamp is NULL
		group by lower(trim(email))
		having count(*) > 1) --72 rows

--Dup count on Email, DOB
select id, email, first_name, last_name, date_of_birth
from candidate
where 1=1
and deleted_timestamp is NULL
and nullif(email, '') is not NULL
and date_of_birth is not NULL
and concat_ws('|', lower(trim(email)), date_of_birth::text) in
		(select concat_ws('|', lower(trim(email)), date_of_birth::text)
		from candidate
		where coalesce(nullif(email, ''), date_of_birth::text) is not NULL
		and deleted_timestamp is NULL
		group by concat_ws('|', lower(trim(email)), date_of_birth::text)
		having count(*) > 1) --41 rows
order by concat_ws('|', lower(trim(email)), date_of_birth::text)