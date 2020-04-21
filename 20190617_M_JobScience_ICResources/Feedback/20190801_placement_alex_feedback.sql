--FEEDBACK FROM ALEX 20190801
CREATE TABLE placement_update_20190801
(job_id character varying(250),
job_title character varying(250),
candidate_id character varying(250),
candidate_fname character varying(250),
candidate_lname character varying(250),
company_id character varying(250),
companyname character varying(250),
contact_company_name character varying(250),
hiring_manager_company character varying(250),
placement_id character varying(250),
lookup character varying(250),
new_contact character varying(250),
new_contact_company_name character varying(250)
)

COPY placement_update_20190801
FROM 'H:\ic-resources\Feedback\20190731_all_placement_w_hiring_manager - Updates from Alex.csv' DELIMITER ',' CSV HEADER;

select *
from placement_update_20190801

--CHECK IN JOBSCIENCE DB
select p.placement_id
, p.job_id
, p.candidate_id
, p.new_contact
, p.new_contact_company_name
, c.firstname as new_fname_check
, c.lastname as new_lname_check
, c.accountid as company_id
, ac.name as new_company_check
from placement_update_20190801 p
left join contact c on c.id = p.new_contact --check new contact
left join account ac on ac.id = c.accountid --check new contact company
where 1=1
--and ac.name is NULL --15 rows
--and p.new_contact_company_name <> ac.name --19 rows | slightly different
--and p.new_contact_company_name = ac.name