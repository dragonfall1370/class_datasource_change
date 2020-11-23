update contact set insert_timestamp = '1900-01-01 01:01:01' where external_id like 'default%';
delete from position_agency_consultant where user_id = -1;
-- update candidate SET candidate_source_id = 29093 where candidate_source_id is null;
-- update candidate SET currency_type = 'pound' where currency_type is null;
update candidate SET currency_type = (select currency_type from user_account where id = -10) where currency_type is null;;
-- update compensation SET currency_type = 'pound' where currency_type is null;
update compensation SET currency_type = (select currency_type from user_account where id = -10) where currency_type is null
-- update compensation set country_code = 'GB' where country_code is null;
update compensation set country_code = (select country.code from country left join user_account on user_account.user_location = country.native_name where id = -10) where country_code is null;
update position_description SET currency_type = (select currency_type from user_account where id = -10) where currency_type is null;
update position_description SET forecast_annual_fee_currency = currency_type; --update position_description SET forecast_annual_fee_currency = 'pound' where forecast_annual_fee_currency is null;
-- update offer set currency_type = 'pound' where currency_type is null;
update offer set currency_type = (select currency_type from user_account where id = -10) where currency_type is null;
-- update offer set country_code = 'GB' where country_code is null;
update offer set country_code = (select country.code from country left join user_account on user_account.user_location = country.native_name where id = -10) where country_code is null;
-- update contact set contact_owners = t.JsonData from (SELECT contact_id, concat('[',string_agg( concat('"',owner_id,'"'), ', '),']') as JsonData FROM tmp_contact_owner GROUP BY contact_id ) t where contact.id = t.contact_id;
-- update candidate set candidate_owner_json = t.JsonData from (SELECT candidate_id,json_agg(row_to_json((SELECT ColumnName FROM (SELECT owner_id,'false',0) AS ColumnName ("ownerId","primary","ownership")))) AS JsonData FROM tmp_candidate_owner GROUP BY candidate_id ) t where candidate.id = t.candidate_id;
update position_description set position_sub_type = 1  where position_type = 1;
update offer set position_sub_type = 1 where position_type = 1;
insert into activity_company (activity_id,company_id,insert_timestamp) select id,company_id, insert_timestamp  from activity where company_id is not null and type = 'company' and insert_timestamp is not null;
insert into activity_contact (activity_id,contact_id,insert_timestamp) select id,contact_id, insert_timestamp from activity where contact_id is not null and type = 'contact' and insert_timestamp is not null and contact_id in (select id from contact);
insert into activity_job (activity_id,job_id,insert_timestamp) select id, position_id, insert_timestamp from activity where position_id is not null and type = 'job' and insert_timestamp is not null and position_id in (select id from position_description)
insert into activity_candidate (activity_id,candidate_id,insert_timestamp) select id,candidate_id,insert_timestamp from activity where candidate_id is not null and type = 'candidate' and insert_timestamp is not null;
