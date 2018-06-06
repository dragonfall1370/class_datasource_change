delete from additional_form_values where additional_type = 'add_cand_info' and additional_id=1234 and field_id=1266;

insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value, insert_timestamp, field_date_value)
values ('add_cand_info', 1234, (select id from configurable_form where type = 'add_cand_info'), 1266, '1,2,3', now(), null);

additional_id = candidate_id (external ID)
-> 
form_id = can be candidate/contact/company/job form (configurable_form)
field_id = 1001 (configurable_form_field)
field_value = can be multiple values
-----
Main script
-----
DO $$ 
 BEGIN
  insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value, insert_timestamp, field_date_value)
  values ('add_cand_info', (select id from candidate where external_id = 'aaaaa'), (select id from configurable_form where type = 'add_cand_info'), 1266, '999', now(), null);
 EXCEPTION
     WHEN OTHERS THEN RAISE NOTICE 'value existed';
 END;
$$;

----------
Industry custom script
----------
DO $$ 
 BEGIN
  delete from candidate_industry where candidate_id = (select id from candidate where external_id = 'aaaaa');
  insert into candidate_industry (vertical_id, candidate_id, insert_timestamp)
  values (28844, (select id from candidate where external_id = 'aaaaa'), now());
 EXCEPTION
     WHEN OTHERS THEN RAISE NOTICE 'value existed';
 END;
$$;


-------
FE custom script
-------
DO $$ 
 BEGIN
  delete from candidate_functional_expertise where candidate_id = (select id from candidate where external_id = 'A P M Ferlin Jayatissa');
  insert into candidate_functional_expertise (functional_expertise_id, candidate_id, insert_timestamp)
  values (2995, (select id from candidate where external_id = 'A P M Ferlin Jayatissa'), now());
 EXCEPTION
     WHEN OTHERS THEN RAISE NOTICE 'value existed';
 END;
$$;