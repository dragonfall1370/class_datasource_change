--APPLICABLE ON PRODUCTION MERGED
with merged_skills as (select m.merged_contact_id
	, m.contact_id
	, concat_ws(chr(10), ('【Merged from PA: ' || c.external_id || '】') , c.skills) as merged_skills
	from mike_tmp_contact_dup_check m
	left join contact c on c.id = m.contact_id
	where m.rn = 1
	and c.skills is not NULL)

, skills_group as (select merged_contact_id
	, string_agg(merged_skills, chr(10) || chr(13)) as skills_group
	from merged_skills
	group by merged_contact_id
	) --select * from skills_group

/* AUDIT CHECK
select c.id, c.skills, external_id
, concat_ws(chr(10), '【Existing skills】' || chr(10) || nullif(c.skills, ''), skills_group) as new_skills 
from contact c
join skills_group s on s.merged_contact_id = c.id
*/

update contact c
set skills = concat_ws(chr(10) || chr(13), '【Skills】' || chr(10) || nullif(c.skills, ''), skills_group)
from skills_group s
where s.merged_contact_id = c.id



--MERGED SKILLS
with merged_skills as (select m.merged_contact_id
	, m.contact_id
	, concat_ws(chr(10), ('Merged from PA: ' || c.external_id) , c.skills) as merged_skills
	from mike_tmp_contact_dup_check m
	left join contact c on c.id = m.contact_id
	where m.rn = 1
	and c.skills is not NULL)

, skills_group as (select merged_contact_id
	, string_agg(merged_skills, chr(10)) as skills_group
	from merged_skills
	group by merged_contact_id
	) --select * from skills_group

/*
select c.id, c.skills, external_id
, concat_ws(chr(10), c.skills, skills_group) as new_skills
from contact c
join skills_group s on s.merged_contact_id = c.id
*/

update contact c
set skills = concat_ws(chr(10) || chr(13), c.skills, s.skills_group)
from skills_group s
where s.merged_contact_id = c.id


---ADDITIONAL CONTACTS (dup check 2)
--MERGED SKILLS
with merged_skills as (select m.merged_contact_id
	, m.contact_id
	, concat_ws(chr(10), ('Merged from PA: ' || c.external_id) , c.skills) as merged_skills
	from mike_tmp_contact_dup_check2 m
	left join contact c on c.id = m.contact_id
	where m.rn = 1
	and m.contact_id not in (select contact_id from mike_tmp_contact_dup_check)
	and c.skills is not NULL) --214 rows

, skills_group as (select merged_contact_id
	, string_agg(merged_skills, chr(10)) as skills_group
	from merged_skills
	group by merged_contact_id
	) --select * from skills_group --210 rows

/*
select c.id, c.skills, external_id
, concat_ws(chr(10), c.skills, skills_group) as new_skills
from contact c
join skills_group s on s.merged_contact_id = c.id
*/

update contact c
set skills = concat_ws(chr(10) || chr(13), c.skills, s.skills_group)
from skills_group s
where s.merged_contact_id = c.id --210 rows