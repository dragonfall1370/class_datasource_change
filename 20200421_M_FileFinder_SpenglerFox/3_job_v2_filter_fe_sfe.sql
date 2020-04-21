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

, interim as (
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where assignmenttitle ilike '%interim%' --408 rows
	
	UNION
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where idcompany in ('826df702-f17e-4939-9566-75dc74e3b21b', 'd6d459aa-4e5e-4771-a0a4-1b99fce610a4')
) --409 rows

, jobleads as (select idassignment
	from "assignment" a
	where a.assignmentno in ('1004879','1007680','1008886','1011960','1013354','2001160','2001522','2001595','2001616','2001645','2001646','2001647') --jobs migrated
	or a.assignmentno in ('2000501','2000876','2000906','2000909','2001060','2001066','2001104','2001122','2001124','2001150','2001158','2001177','2001178','2001179','2001188','2001197','2001225','2001227','2001228','2001231','2001235','2001238','2001239','2001265','2001266','2001276','2001286','2001292','2001306','2001328','2001334','2001335','2001340','2001349','2001353','2001359','2001387','2001392','2001393','2001397','2001398','2001400','2001407','2001421','2001423','2001424','2001443','2001446','2001469','2001484','2001485','2001513','2001518','2001521','2001533','2001536','2001537','2001538','2001549','2001553','2001558','2001565','2001567','2001572','2001583','2001593','2001593','2001596','2001602','2001613','2001618','2001623','2001625','2001628','2001631','2001632','2001634','2001639','2001640','2001648','2001655','2001669','2001670','2001671','1013982','1013999','2000700','2000923','2000924','2000960','2001064','2001102','2001103','2001109','2001112','2001154','2001157','2001175','2001221','2001226','2001229','2001230','2001232','2001233','2001234','2001236','2001237','2001240','2001241','2001242','2001243','2001244','2001245','2001248','2001249','2001350','2001364','2001373','2001380','2001390','2001412','2001419','2001422','2001427','2001434','2001460','2001473','2001494','2001496','2001547','2001548','2001550','2001574','2001585','2001591','2001624','2001627','2001630','2001633','2001642','2001650','2001651','2001653','2001659','2001660','2001661','2001662') --jobs leads
	)
	
, selected_assignment as 
	(select idassignment
	from interim
	
	UNION
	select idassignment
	from jobleads)

/* AUDIT CHECK
select idassignment
, j.parentid
, j.value
from assignmentcode a
left join jobfunction j on j.idjobfunction = a.codeid
where idtablemd = '6051cf96-6d44-4aeb-925e-175726d0f97b' --JobFunction
and idassignment in (select idassignment from selected_assignment)
*/ --693 rows

select idassignment job_ext_id
, fe.feid
, fe.fe
, fe.sfeid
, fe.sfe
from assignmentcode a
left join jobfunction j on j.idjobfunction = a.codeid
left join fe_sfe fe on fe.feid = j.parentid and fe.sfeid = j.idjobfunction
where idtablemd = '6051cf96-6d44-4aeb-925e-175726d0f97b' --JobFunction
and idassignment in (select idassignment from selected_assignment)