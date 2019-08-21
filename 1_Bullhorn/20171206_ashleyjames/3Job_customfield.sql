
        select   c.id, c.external_id, c.name, afv.field_value
        from position_description c
        left join additional_form_values afv on afv.additional_id = c.id
        left join (select l.translate, fl.field_id, fl.field_value
                   from configurable_form_language l
                   left join configurable_form_field_value fl on l.language_code = fl.title_language_code
                   where fl.title_language_code is not NULL
                  ) fv on fv.field_value = afv.field_value
        where afv.field_value is not null


select * from additional_form_values 


insert into additional_form_values (additional_type,additional_id,form_id,field_id,field_value ) 
select  'add_job_info'
        ,id 
        ,1006 
        ,1016
        ,external_id 
select count(*) from position_description where external_id is not null and external_id::int not in (234,175)

