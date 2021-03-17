--JOB CATEGORY AS FE
select code
, trim(description) as fe
from codes
where codegroup = '24'
order by description --70

--PERSON CATEGORY
with person_category as (select uniqueid
	, "127 job cat codegroup  24"
	, a.category
	, a.splitrn
	from f01, unnest(string_to_array("127 job cat codegroup  24", '~')) with ordinality as a(category, splitrn)
	where 1=1
	and "127 job cat codegroup  24" is not NULL
	and "101 candidate codegroup  23" = 'Y' --candidate filter
	)

select distinct pc.uniqueid as cand_ext_id
, pc.category
, c.description as final_category
from person_category pc
left join (select * from codes where codegroup = '24') c on c.code = pc.category
--order by pc.uniqueid
