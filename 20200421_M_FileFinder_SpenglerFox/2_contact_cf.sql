--#CF | Nationality | 11266 | Multiple selection
	with split_idnationality_string_list AS (
		SELECT idperson
		, s.idnationality
		FROM personx px, UNNEST(string_to_array(px.idnationality_string_list, ',')) s(idnationality)
		where idnationality_string_list is not NULL
	)

	SELECT distinct idperson con_ext_id
	, trim(i.value) field_value
	, 'add_con_info' as additional_type
	, 1008 as form_id
	, 11266 as field_id
	, current_timestamp as insert_timestamp
	FROM split_idnationality_string_list sisl
	LEFT JOIN nationality i ON sisl.idnationality = i.idnationality


--#CF | Gender | 11267 | Drop down
select distinct idperson con_ext_id
, idgender_string
, g.value as field_value
, 'add_con_info' as additional_type
, 1008 as form_id
, 11267 as field_id
from personx p
left join gender g on g.idgender = p.idgender_string
where idgender_string is not NULL

	
--#CF | International | 11268 | Multiple selection
with split_idinternational_string_list AS (
		SELECT idperson
		, s.idinternational
		FROM personx px, UNNEST(string_to_array(px.idinternational_string_list, ',')) s(idinternational)
		where idinternational_string_list is not NULL
	)

	SELECT distinct idperson con_ext_id
	, trim(i.value) field_value
	, 'add_con_info' as additional_type
	, 1008 as form_id
	, 11268 as field_id
	FROM split_idinternational_string_list sisl
	LEFT JOIN international i ON sisl.idinternational = i.idinternational
	
	
--#CF | Language | 11269 | Multiple selection
select distinct pc.idperson con_ext_id
, trim(l.value) as field_value --contact_language
, 'add_con_info' as additional_type
, 1008 as form_id
, 11269 as field_id
, current_timestamp as insert_timestamp
from personcode pc
left join language l on l.idlanguage = pc.codeid
where idtablemd = 'c69e91b3-9f35-4c73-ba46-2e17ad8ce6aa' --language


--#CF | Decision Maker | 11270 | Multiple selection
select distinct pc.idperson as con_ext_id
	, trim(u.value) as field_value
	, 'add_con_info' as additional_type
	, 1008 as form_id
	, 11270 as field_id
	, current_timestamp as insert_timestamp
from personcode pc
--left join tablemdshort ts on ts.idtablemd = pc.idtablemd --table name
left join udskill3 u on u.idudskill3 = pc.codeid --Decision Maker
where 1=1
and pc.idtablemd = 'e81edcd2-7bf2-4e59-b24a-f9278f4f5c5e' --Decision Maker
--and pc.idperson = '527db5ed-ee11-4412-be3d-cb069f153e31'


--#CF | Xmas List | 11270 | Multiple selection
select distinct pc.idperson as con_ext_id
	, trim(u.value) as field_value
	, 'add_con_info' as additional_type
	, 1008 as form_id
	, 11271 as field_id
	, current_timestamp as insert_timestamp
from personcode pc
left join udskill6 u on u.idudskill6 = pc.codeid --Xmas List
where 1=1
and pc.idtablemd = 'b801e205-4990-47d4-b237-46fe899da852' --Xmas List