with fe_sfe as (select j.parentid as FEID
	, case when j.parentid = '00000000-0000-0000-0000-000000000000' then 'Other' 
			else j2.value end as FE
	, j.idjobfunction as SFEID
	, j.value as SFE
	, 'The list extracted from Job Function' note
	from jobfunction j
	left join jobfunction j2 on j2.idjobfunction = j.parentid
	--order by j2.value, j.value --91 rows
	
	UNION
	--Bank & Fin Skills | 186 values
	select u.parentid as FEID
	, case when u.parentid = '00000000-0000-0000-0000-000000000000' then 'Bank & Fin Skills' 
			else 'Bank & Fin Skills' || ' | ' || u2.value end as FE
	, u.idudskill2 as SFEID
	, u.value as SFE
	, 'The list extracted from Bank & Fin Skills' note
	from udskill2 u
	left join udskill2 u2 on u2.idudskill2 = u.parentid
	--order by u2.value, u.value --186 rows
	)

, split_jobfunction_list AS (
	SELECT idperson
	, s.jobfunction
	FROM personx px, UNNEST(string_to_array(px.idjobfunction_string_list, ',')) s(jobfunction)
	where px.idjobfunction_string_list is not NULL
)

, contact_jobfunction as (SELECT idperson
	, jf.parentid as feid
	, jf.idjobfunction as sfeid
	, jf.value jobfunction
	FROM split_jobfunction_list sj
	LEFT JOIN jobfunction jf ON sj.jobfunction = jf.idjobfunction

	UNION ALL
	select pc.idperson
	, u.parentid as feid
	, u.idudSkill2 as sfeid
	, trim(u.value) as bank_fin
	from personcode pc
	left join udSkill2 u on u.idudSkill2 = pc.codeid --Bank & Fin Skills
	where 1=1
	and pc.idtablemd = '28a7d22f-9046-41db-92d3-c1719fd11625' --Bank & Fin Skills
	--and pc.idperson = '527db5ed-ee11-4412-be3d-cb069f153e31'
	)
	
select cj.idperson con_ext_id
, cj.feid
, fe.fe
, cj.sfeid
, fe.sfe
from contact_jobfunction cj
left join fe_sfe fe on fe.feid = cj.feid and fe.sfeid = cj.sfeid
where 1=1
--and cj.sfeid = '1da4a35b-d199-4f32-8164-d28e71000a97'
--and fe.fe ilike '%Bank & Fin Skills%'