with job_category as (select uniqueid
	, "100 job cat codegroup  24"
	, a.category
	, a.splitrn
	from f03, unnest(string_to_array("100 job cat codegroup  24", '~')) with ordinality as a(category, splitrn)
	where "100 job cat codegroup  24" is not NULL
	)
	
select distinct jc.uniqueid as job_ext_id
, jc.category
, c.description as final_category
from job_category jc
left join (select * from codes where codegroup = '24') c on c.code = jc.category
where c.description is not NULL
order by jc.uniqueid