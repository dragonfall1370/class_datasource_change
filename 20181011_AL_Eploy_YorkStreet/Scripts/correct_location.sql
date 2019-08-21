SELECT id, state, state_code, city, city_code, post_code, district, 
       address, latitude, longitude, insert_timestamp, country, country_code, 
       location_name, nearest_train_station, geo_name_id, desired_location_candidate_id, 
       personal_location_candidate_id, current_location_candidate_id, 
       trigger_index_update_timestamp, company_id, offer_personal_info_location_id, 
       location_type, phone_number, note
  FROM public.common_location
  where
  --id = 29181
  note is not null
  limit 1


-- update public.common_location
-- set country_code = 'GB'
-- where note is not null
-- and country_code is null

-- update public.common_location
-- set address = 'UK'
-- where note is not null
-- and address is null

-- update public.common_location
-- set location_name = concat(
--  case
--   when length(trim(coalesce(state, ''))) = 0 then ''
--   else trim(coalesce(state, '')) || ','
--  end
--  , case when trim(coalesce(country_code, '')) = 'GB' then 'UK' end
-- )
-- where note is not null
--and address is null

select
concat(
case
 when length(trim(coalesce(state, ''))) = 0 then ''
 else trim(coalesce(state, '')) || ','
end
, case when trim(coalesce(country_code, '')) = 'GB' then 'UK' end
)
from public.common_location
where id = 29191

select * from contact_location
where contact_id = 33885


select * from company_location

SELECT id, company_id, first_name, phone, email, skype, method_of_contact, 
       insert_timestamp, deleted_timestamp, status, board, last_name, 
       middle_name, job_title, profile_picture_filename, contact_client_type, 
       org_contact_id, trigger_index_update_timestamp, contact_owners, 
       user_account_id, first_name_kana, middle_name_kana, last_name_kana, 
       nick_name, linkedin, report_to, note, external_id, customer_probability, 
       date_of_birth, mobile_phone, home_phone, switchboard_phone, switchboard_phone_ext, 
       personal_email, current_location_id, preferred_time_from, preferred_time_to, 
       twitter, job_level, hierarchy, department, is_assistant, start_date, 
       skills, facebook, gender_title, candidate_source_id, company_admin_time_temp, 
       hot_end_date, xing, contact_owner_ids
  FROM public.contact
  where deleted_timestamp is null
	and current_location_id > 0

  select * from common_location
  where id = 29181


select * from contact_location