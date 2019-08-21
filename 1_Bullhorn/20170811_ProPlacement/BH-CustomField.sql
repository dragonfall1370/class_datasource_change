select id,external_id,first_name,last_name,note from candidate where first_name = 'Seymour'

-----CUSTOM FIELD-----
select count(*) from additional_form_values where field_id = 1016
select * from additional_form_values where additional_id = 128033
update additional_form_values set field_value = 123456 where additional_id = 128033


select l.language_code,l.translate, fl.field_id, fl.field_value from configurable_form_language l
left join configurable_form_field_value fl on l.language_code = fl.title_language_code where fl.title_language_code is not null


with 
  t as (  select l.translate, fl.field_id, fl.field_value from configurable_form_language l
        left join configurable_form_field_value fl on l.language_code = fl.title_language_code where fl.title_language_code is not null)
, t2 as (SELECT translate, field_id, field_value, ROW_NUMBER() OVER (PARTITION BY translate ORDER BY field_id) as r FROM t)
select *  from t2 where r = 1 limit 200
