--CF# 11317 | HP PUBLIC APPROVAL
--BEFORE SWITCH
select *
--into mike_tmp_hp_approval_11317_20200415
from additional_form_values
where field_id = 11317 --144317
--and field_value = '1' --97526 | 申請中
and field_value = '2' --43432 | 却下
--and field_value = '3' --3359 | 承認

--Update to switch from 却下 to 承認
update additional_form_values a
set field_value = '3'
from mike_tmp_hp_approval_11317_20200415 m
where a.additional_id = m.additional_id
and a.field_id = 11317
and a.field_value = '2'
and m.field_value = '2'

--Update to switch from 承認 to 却下
update additional_form_values a
set field_value = '2'
from mike_tmp_hp_approval_11317_20200415 m
where a.additional_id = m.additional_id
and a.field_id = 11317
and a.field_value = '3'
and m.field_value = '3'

select *
from mike_tmp_hp_approval_11317_20200415
where field_value = '2'

--AFTER SWITCH
select *
--into mike_tmp_hp_approval_11317_20200415
from additional_form_values
where field_id = 11317 --144317
--and field_value = '1' --97526 | 申請中
--and field_value = '2' --3359 | 承認
and field_value = '3' --43432 | 却下