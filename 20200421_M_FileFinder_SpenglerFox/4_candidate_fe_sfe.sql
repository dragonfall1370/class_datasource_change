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
	--Decision Maker
	select u.parentid as FEID
	--, u2.value as FE
	, 'Decision Maker' as FE
	, u.idudskill3 as SFEID
	, u.value as SFE
	, 'The list extracted from Decision Maker' note
	from udskill3 u
	left join udskill3 u2 on u2.idudskill3 = u.parentid
	--where u.parentid <> '00000000-0000-0000-0000-000000000000'
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
	, u.idudSkill3 as sfeid
	, trim(u.value) as decision_m
	from personcode pc
	left join udSkill3 u on u.idudSkill3 = pc.codeid --Decision Makers
	where 1=1
	and pc.idtablemd = 'e81edcd2-7bf2-4e59-b24a-f9278f4f5c5e' --Decision Makers
	)
	
select cj.idperson cand_ext_id
, cj.feid
, fe.fe
, cj.sfeid
, fe.sfe
from contact_jobfunction cj
left join fe_sfe fe on fe.feid = cj.feid and fe.sfeid = cj.sfeid
where 1=1
--and cj.sfeid = '1da4a35b-d199-4f32-8164-d28e71000a97'