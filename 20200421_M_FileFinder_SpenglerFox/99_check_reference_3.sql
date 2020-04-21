select j2.idjobfunction
, j2.value
, j.idjobfunction
, j.value
from jobfunction j
join jobfunction j2 on j2.idjobfunction = j.parentid
--order by j.parentid, j.value

UNION ALL
select idjobfunction
, value
, '' idjobfunction
, '' value
from jobfunction
where parentid not in (select idjobfunction from jobfunction)


select *
from jobfunction
order by parentid, value


--FE/SFE
WITH split_fe AS (
	SELECT
	idperson person_id,
	LOWER(TRIM(s.functional_expertise_id)) functional_expertise_id
	FROM personx px, UNNEST(string_to_array(px.idjobfunction_string_list, ',')) s(functional_expertise_id)
)
, cte_functional_expertise AS (
	SELECT
	person_id,
	sf.functional_expertise_id,
	jf.idjobfunction,
	jf.value functional_expertise
	FROM split_fe sf
	LEFT JOIN jobfunction jf ON sf.functional_expertise_id = jf.idjobfunction
)

select distinct functional_expertise
from cte_functional_expertise