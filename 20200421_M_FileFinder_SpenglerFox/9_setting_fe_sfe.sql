/*--FE / SFE list
select j.parentid as FEID
, case when j.parentid = '00000000-0000-0000-0000-000000000000' then 'Other' 
		else j2.value end as FE
, j.idjobfunction as SFEID
, j.value as SFE
from jobfunction j
left join jobfunction j2 on j2.idjobfunction = j.parentid
order by j2.value, j.value --91 rows

--Bank & Fin Skills
select 'Bank & Fin Skills' as FE
, idudskill2 as SFEID
, trim(value) as SFE
from udskill2
order by trim(value)

--Extra Skills 3
select 'Extra Skills 3' as FE
, idudskill5 as SFEID
, trim(value) as SFE
from udSkill5
order by trim(value) */

-->> FINAL FE / SFE <<--
--Job function | 91 values
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
	
	UNION
	--Decision Maker
	select u.parentid as FEID
	--, u2.value as FE
	, case when u.parentid = '00000000-0000-0000-0000-000000000000' then 'Decision Maker' 
			else 'Decision Maker' || ' | ' || u2.value end as FE
	, u.idudskill3 as SFEID
	, u.value as SFE
	, 'The list extracted from Decision Maker' note
	from udskill3 u
	left join udskill3 u2 on u2.idudskill3 = u.parentid
	--where u.parentid <> '00000000-0000-0000-0000-000000000000'
	)
	
select distinct fe, sfe
from fe_sfe
order by fe, sfe