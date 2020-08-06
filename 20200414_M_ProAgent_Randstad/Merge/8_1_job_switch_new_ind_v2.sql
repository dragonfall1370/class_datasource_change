select *
into mike_tmp_position_description_industry_20200705
from position_description_industry
where insert_timestamp < '2020-07-04'
and parent_id is NULL

with job_ind as (
	--Job industry from position_description
	select id, vertical_id
	from position_description
	where vertical_id is not NULL
	and deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'JOB%') --15641
	
	UNION ALL
	--Job industry from position_description_industry
	select p.position_id
	, pd.vertical_id
	from position_description_industry p
	left join position_description pd on pd.id = p.position_id
	where p.insert_timestamp < '2020-07-04'
	and p.parent_id is NULL --358
)

--Total job with vertical: 15999 (UNION ALL)
, job_distinct as (select distinct *
from job_ind)--15643 rows

/* NO JOB HAVING MULTIPLE INDUSTRY
select id
from job_distinct
group by id
having count(*) > 1
*/

, job_ind_sub as (select distinct j.id as position_id
	, j.vertical_id
	, NULL::int parent_id
	, m.vc_new_ind_id industry_id
	, 0 seq
	, current_timestamp insert_timestamp
	from job_distinct j
	left join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = j.vertical_id
		
	UNION ALL
	select distinct j.id as position_id
	, j.vertical_id
	, m.vc_new_ind_id parent_id
	, m.vc_sub_ind_id industry_id
	, row_number() over(partition by j.id, m.vc_new_ind_id order by m.vc_sub_ind_id asc) - 1 as seq
	, current_timestamp insert_timestamp
	from job_distinct j
	left join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = j.vertical_id) select * from job_ind_sub

/* AUDIT IF JOB HAVING DIFFERENT INDUSTRIES RATHER THAN SUB INDUSTRY
, job_ind_sub as (select j.*
	, m.vc_new_ind_id parent_id
	, m.vc_sub_ind_id industry_id
	, row_number() over(partition by j.id, m.vc_new_ind_id order by m.vc_sub_ind_id asc) - 1 as seq
	from job_distinct j
	left join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = j.vertical_id)

select *
from job_ind_sub
where id in (select id from job_ind_sub group by id having count(*) > 1)
*/

--MAIN SCRIPT
insert into position_description_industry (position_id, industry_id, parent_id, seq, insert_timestamp)
select position_id
, industry_id
, parent_id
, seq
, insert_timestamp
from job_ind_sub
order by position_id, seq


--UPDATE POSITION_DESCRIPTION > VERTICAL
/* AUDIT
select *
from position_description_industry
where id > 510634
and parent_id is NULL
*/

update position_description pd
set vertical_id = p.industry_id
from position_description_industry p
where pd.id = p.position_id
and p.parent_id is NULL
and p.id > 510634 --(maxid before converting)


/* CHECK IF NULL VALUE AND DELETE CORRECT RECORDS
select * from position_description_industry
where position_id in (
select position_id
from mike_tmp_position_description_industry_20200705
where insert_timestamp < '2020-07-04'
group by position_id
having count(*) > 1)

select *
from position_description_industry
where insert_timestamp < '2020-07-04'
and industry_id < 29019
order by position_id

select *
from position_description_industry
where position_id = 66351
*/

--DELETE OLD VALUES
delete from position_description_industry
where insert_timestamp < '2020-07-04'
and industry_id < 29019 --Earliest ID for NEW Industry