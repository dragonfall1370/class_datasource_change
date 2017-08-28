
-----CANDIDATE - CHECKING-----
select id,external_id,first_name,last_name,note,edu_details_json,keyword from candidate where external_id::int in (34871,34888,34917) or (first_name = 'Naman' and last_name = 'Chhabra' )


select ca.id, ca.external_id, ca.first_name, ca.last_name, ca.insert_timestamp, ca.note, ca.candidate_source_id, cs.name from candidate ca
left join candidate_source cs on cs.id = ca.candidate_source_id
where --ca.external_id::int in (469)
first_name in ('Findlay','Sonia','Post','Katharina')
ca.candidate_source_id is not null
ca.id = '19988'


select   c.id, c.external_id, c.first_name, c.last_name
        ,afv.*
        --,fv.translate
from candidate c
left join additional_form_values afv on afv.additional_id = c.id --where c.external_id::int in (684)
left join (select l.translate, fl.field_id, fl.field_value
           from configurable_form_language l
           left join configurable_form_field_value fl on l.language_code = fl.title_language_code
           where fl.title_language_code is not NULL
           --and fl.field_id in (1020)
          ) fv on fv.field_id = afv.field_id --fv.field_value = afv.field_value
where --afv.field_id in (1020) and
--additional_id in (83407)
c.external_id::int in (427)
limit 200




-----CONTACT - CHECKING-----
select id,external_id,first_name,last_name,note from contact where external_id = '3495' or (first_name = 'Abhishek' and last_name = 'Advani' )


select   c.id, c.external_id, c.first_name, c.last_name
        ,afv.*
        ,fv.translate
from contact c
left join additional_form_values afv on afv.additional_id = c.id
left join (select l.translate, fl.field_id, fl.field_value
           from configurable_form_language l
           left join configurable_form_field_value fl on l.language_code = fl.title_language_code
           where fl.title_language_code is not NULL
           --and fl.field_id in (1020,1021)
          ) fv on fv.field_value = afv.field_value
where afv.field_id in (1020) and
--additional_id in (33075)
c.external_id::int in (11151) 
limit 200



-----------
select * from configurable_form_field_value
select count(*) from additional_form_values where field_id = 1020
update additional_form_values set field_value = 123456 where additional_id = 128033


-----CUSTOM FIELD-----
select l.language_code,l.translate, fl.field_id, fl.field_value
from configurable_form_language l
left join configurable_form_field_value fl on l.language_code = fl.title_language_code
where fl.title_language_code is not NULL
and fl.field_id = 1024


with 
  t as (  select l.translate, fl.field_id, fl.field_value
                  --l.*
                  --,fl.*
          from configurable_form_language l
          left join configurable_form_field_value fl on l.language_code = fl.title_language_code
          where fl.title_language_code is not NULL
          and fl.field_id = 1020
          )
, t2 as (SELECT translate, field_id, field_value, ROW_NUMBER() OVER (PARTITION BY translate ORDER BY field_id) as r FROM t)
select *  from t2 where r = 1 limit 200
