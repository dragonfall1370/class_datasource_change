--Update format of "Business characteristic" to remove "【Merged from PA: CPY000000】"
select *
--into mike_bkup_additional_form_values_field_id_1022_20200714
from additional_form_values
where field_id = 1022


select additional_id
, field_value
, position('【Merged from PA:】' in field_value) --should be 1
, overlay(field_value placing '' from position('【Merged from PA:' in field_value) for length('【Merged from PA: CPY000000】') + 1) as new_field_value
from additional_form_values
where field_id = 1022
and additional_id in (select vc_company_id from mike_tmp_company_dup_check)
--and field_value ilike '%【Merged from PA: CPY027117】%' --check case
and position('【Merged from PA:' in field_value) = 1 --change condition at the beginning of the text


update additional_form_values
set field_value = overlay(field_value placing '' from position('【Merged from PA:' in field_value) for length('【Merged from PA: CPY000000】') + 1)
where field_id = 1022
and additional_id in (select vc_company_id from mike_tmp_company_dup_check)
and position('【Merged from PA:' in field_value) > 1 