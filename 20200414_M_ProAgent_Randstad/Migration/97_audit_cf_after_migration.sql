---UTILITY SCRIPT FOR AUDITING CUSTOM FIELDS
select distinct cff.form_id
--, cf.type
, cff.id as field_id
, cfl.translate
, case cff.field_type
	when 1 then 'TEXT_FIELD'
	when 2 then 'TEXT_AREA'
	when 3 then 'CHECK_BOXES'
	when 4 then 'DROP_DOWN'
	when 5 then 'RADIO_BUTTON'
	when 6 then 'DATE_PICKER'
	when 7 then 'MULTIPLE_SELECTION'
	when 8 then 'TABLE'
	end as field_type
--, cff.field_key, cffv.field_value, cfl2.translate, cfl2.language_code
--, cfls.tab_id, cfls.static_field_id, cfls.category
--, cft.name, cft.entity, cft.layout_type, cft.country_code
from configurable_form_field cff
left join configurable_form_language cfl on cff.label_language_code = cfl.language_code
left join configurable_form_field_value cffv on cff.id = cffv.field_id
left join configurable_form_language cfl2 on cffv.title_language_code = cfl2.language_code
left join configurable_form cf on cf.id = cff.form_id
left join configurable_form_layout_settings cfls on cfls.configurable_form_field_id = cff.id --onboarding/country specific
left join configurable_form_tab cft on cft.id = cfls.tab_id --onboarding/country specific
where 1=1
--and cff.id = 1015
--and cft.layout_type = 'compliance'
--and cft.layout_type = 'summary'
and cf.type = 'add_job_info'
--order by cffv.field_id, cffv.title_language_code, cff.id
and cfl.language = 'en'
--and cfl2.language = 'en'
order by cff.form_id


---AUDIT CUSTOM VALUES
select cffv.form_id as join_form_id
, cffv.field_id as join_field_id
, cfl.translate as join_field_translate
, cffv.field_value as join_field_value
from configurable_form_language cfl
left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
where cfl.language = 'en'
and cffv.field_id = 1113