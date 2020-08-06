--BACKUP COGNITIVE PATHWAY CF#
select *
from additional_form_values
--into mike_tmp_field_11302_20200417 --backup without timestamp filter
where field_id = 11302
and insert_timestamp between '2020-02-20' and '2020-03-02' --delete with timestamp filter


--DELETE OLD MAPPING | 146006
delete from additional_form_values
where field_id = 11302
and insert_timestamp between '2020-02-20' and '2020-03-02'

--UPDATE NOT FOUND TRANSLATION VALUE
with cognitive as (select a.additional_id
, c.external_id
, c.deleted_timestamp
, a.insert_timestamp
from additional_form_values a
join candidate c on c.id = a.additional_id
where a.field_id = 11302
and field_value is NULL
and c.external_id in ('CDT198067','CDT198096','CDT198135','CDT198217','CDT198235','CDT198250','CDT198269','CDT198490','CDT198497','CDT198552','CDT198564','CDT198598','CDT198641','CDT198703','CDT198704','CDT199017','CDT199083','CDT199112','CDT199127','CDT199157','CDT199313','CDT199357','CDT199778','CDT199784','CDT200006','CDT200079','CDT200341','CDT200693','CDT200714','CDT200858','CDT201267','CDT201319','CDT201379','CDT201573','CDT201985','CDT202039','CDT202219','CDT202365','CDT202768','CDT202899','CDT203222','CDT203251','CDT203309','CDT203358','CDT203503','CDT203763','CDT203923','CDT203934','CDT204175','CDT204326','CDT204418','CDT204586','CDT204684','CDT204736','CDT204767','CDT204891','CDT204908','CDT204915','CDT204971','CDT205008','CDT205050','CDT205148','CDT205482','CDT205539','CDT205553','CDT205624','CDT205715','CDT206195','CDT206309','CDT206380','CDT206460','CDT206517','CDT206908','CDT206950','CDT207021','CDT207243','CDT207298','CDT207308','CDT207608','CDT207668','CDT207693','CDT207770','CDT207934','CDT208173','CDT208341','CDT208395','CDT208717','CDT208884','CDT209199','CDT209331','CDT209587','CDT209655','CDT209803','CDT210021','CDT210111','CDT210119','CDT210459','CDT210463','CDT210536','CDT210685','CDT210688','CDT211243','CDT212319','CDT212399','CDT212587','CDT212606','CDT213267','CDT213603','CDT214044','CDT214716','CDT214827','CDT215023','CDT215226','CDT215230','CDT215252','CDT215255','CDT215287','CDT215470','CDT215739','CDT215762','CDT216292','CDT216603','CDT217316','CDT217546','CDT217905','CDT218417','CDT220305','CDT220388','CDT220691','CDT220728','CDT220876','CDT221086','CDT221264','CDT221334','CDT221444','CDT221566','CDT221584','CDT221663','CDT221665','CDT222061','CDT222141','CDT222700','CDT223038','CDT223448','CDT223550','CDT223740','CDT223917','CDT224285','CDT224422','CDT225226','CDT225439','CDT226705','CDT227013','CDT227434','CDT247665','CDT254934','CDT258711','CDT259133','CDT260280','CDT260376')
)

update additional_form_values a
set field_value = '74' --LinkedIn value
from cognitive c
where a.additional_id = c.additional_id
and a.field_id = 11302


update additional_form_values
set field_value = '72' --indeed value
where field_id = 11302
and field_value is NULL --1096


-->> APPEND COGNITIVE &  TO CANDIDATE NOTES
with cognitive_value as (select cffv.form_id as join_form_id
		, cffv.field_id as join_field_id
		, cfl.translate as join_field_translate
		, cffv.field_value as join_field_value
		from configurable_form_language cfl
		left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
		where cfl.language = 'en' --input language
		and cffv.field_id = 11302 --Cognitive pathway
)		

, cognitive as (select a.additional_id as vc_pa_candidate_id --only candidates from PA
		, a.field_value
		, c.join_field_translate
		from additional_form_values a
		left join cognitive_value c on c.join_field_value = a.field_value
		where a.field_id = 11302
)

, merged_notes as (select m.vc_pa_candidate_id
	, m.cand_ext_id
	, concat_ws('<br>'
		, ('【登録経路】' || cs."name")
		, ('【認知経路】' || co.join_field_translate)
		) as merged_notes
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	left join cognitive co on co.vc_pa_candidate_id = m.vc_pa_candidate_id
	left join candidate_source cs on cs.id = c.candidate_source_id
	where 1=1
	and c.note is not NULL
) --select vc_pa_candidate_id from merged_notes group by vc_pa_candidate_id having count(*) > 1

update candidate c
set note = concat_ws('<br>' || '<br>', c.note, n.merged_notes)
from merged_notes n
where n.vc_pa_candidate_id = c.id


-->> MARK DUPLICATE CANDIDATE IN COGNITIVE
select *
into mike_tmp_candidate_11302_to_dup_value_20200417
from additional_form_values
where field_id = 11302
and additional_id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)


--MAIN SCRIPT
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_pa_candidate_id as additional_id
, 1139 form_id
, 11302 field_id
, '128' field_value
from mike_tmp_candidate_dup_check
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;