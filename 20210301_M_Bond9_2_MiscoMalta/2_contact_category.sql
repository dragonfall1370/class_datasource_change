with person_category as (select uniqueid
	, "127 job cat codegroup  24"
	, a.category
	, a.splitrn
	from f01, unnest(string_to_array("127 job cat codegroup  24", '~')) with ordinality as a(category, splitrn)
	where 1=1
	and "127 job cat codegroup  24" is not NULL
	and "100 contact codegroup  23" = 'Y' --contact filter
	)

select distinct pc.uniqueid as con_ext_id
, pc.category
, c.description as final_category
from person_category pc
left join (select * from codes where codegroup = '24') c on c.code = pc.category
where c.description is not NULL
order by pc.uniqueid