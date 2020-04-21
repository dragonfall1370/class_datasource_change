--BACKUP JOB INDUSTRY
select id, name, vertical_id, external_id, insert_timestamp, deleted_timestamp
into mike_tmp_job_industry_20200417
from position_description
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'JOB%')
and vertical_id is not NULL --13595


--INSERT INTO POSITION_DESCRIPTION_INDUSTRY
with job_ind as (select distinct pd.id position_id
		, NULL::int parent_id
		, m.vc_new_ind_id industry_id
		, 0 seq
		, current_timestamp insert_timestamp
	from position_description pd
	left join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = pd.vertical_id
	where deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'JOB%')
	and vertical_id is not NULL
	--and m.vc_new_ind_id is NULL
	
	UNION ALL
	select distinct pd.id position_id
		, m.vc_new_ind_id parent_id
		, m.vc_sub_ind_id industry_id
		, row_number() over(partition by pd.id, m.vc_new_ind_id order by m.vc_sub_ind_id asc) - 1 as seq
		, current_timestamp insert_timestamp
	from position_description pd
	join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = pd.vertical_id
	where deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'JOB%')
	and vertical_id is not NULL
	--order by pd.id
	) --select * from job_ind where job_id in (57211, 46344, 59402) order by job_id 

/*
select job_id, count(*)
from job_industry
group by job_id
having count(*) > 2
*/
/* --AUDIT THE CHECK
select * 
from mike_tmp_vc_2_vc_new_ind
where vc_ind_id in (29026, 29019, 29024)

select *
from vertical
--where id in (29025, 29029)
--where id in (29026, 29019, 29024)
*/

insert into position_description_industry (position_id, industry_id, parent_id, seq, insert_timestamp)
select position_id
, industry_id
, parent_id
, seq
, insert_timestamp
from job_ind
order by position_id, seq

--UPDATE POSITION_DESCRIPTION > VERTICAL
with job_ind as (select distinct pd.id position_id
		, m.vc_new_ind_id industry_id
		from position_description pd
		left join mike_tmp_vc_2_vc_new_ind m on m.vc_ind_id = pd.vertical_id
		where deleted_timestamp is NULL
		and (external_id is NULL or external_id not ilike 'JOB%')
		and vertical_id is not NULL) --select * from job_ind order by position_id
		
update position_description pd
set vertical_id = j.industry_id
from job_ind j
where pd.id = j.position_id