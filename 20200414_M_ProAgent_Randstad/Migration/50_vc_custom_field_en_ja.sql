--CUSTOM FIELD LABEL
with en_field_label as (select cff.form_id
			, cff.id as field_id
			, cff.position
			, cff.block
			, cff.field_type
			, cff.name as field_type_name
			, cff.label_language_code
			, cff.field_key
			, cfl.translate as field_label
			, cfl.id as translate_id
			from configurable_form_field cff
			inner join configurable_form_language cfl on cfl.language_code = cff.label_language_code
			where 1=1
			and cfl.language = 'en')

, ja_field_label as (select cff.form_id
			, cff.id as field_id
			, cff.position
			, cff.block
			, cff.field_type
			, cff.name as field_type_name
			, cff.label_language_code
			, cff.field_key
			, cfl.translate as field_label
			, cfl.id as translate_id
			from configurable_form_field cff
			inner join configurable_form_language cfl on cfl.language_code = cff.label_language_code
			where 1=1
			and cfl.language = 'ja')
			
--CUSTOM FIELD VALUE LABEL
, en_field_value_label as (select cffv.form_id
			, cffv.field_id
			, cffv.field_value
			, cffv.id as field_value_id
			, cffv.title_language_code
			, cfl.translate as field_label
			, cfl.id as translate_id
			from configurable_form_field_value cffv
			inner join configurable_form_language cfl on cfl.language_code = cffv.title_language_code
			where 1=1
			and cfl.language = 'en')
	
, ja_field_value_label as (select cffv.form_id
			, cffv.field_id
			, cffv.field_value
			, cffv.id as field_value_id
			, cffv.title_language_code
			, cfl.translate as field_label
			, cfl.id as translate_id
			from configurable_form_field_value cffv
			inner join configurable_form_language cfl on cfl.language_code = cffv.title_language_code
			where 1=1
			and cfl.language = 'ja')
			
