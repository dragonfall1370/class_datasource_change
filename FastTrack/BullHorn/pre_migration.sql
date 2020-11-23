
with inserted AS (insert into company(external_id, name, insert_timestamp) values ('default','Default-Company','1900-01-01 01:01:01') RETURNING id) insert into contact(external_id, company_id, last_name) select 'default', id, 'Default-Contact' FROM inserted;
-- create table tmp_contact_owner (contact_id bigint, owner_id int, owner_email varchar(255)); --select * from tmp_contact_owner;
-- create table tmp_candidate_owner (candidate_id bigint, owner_id int, owner_email varchar(255)); --select * from tmp_candidate_owner;
alter table activity add column userMessageID int4; --Bullhorn
delete from activity_company where company_id not in (select id from company);
delete from activity_contact --where contact_id not in (select id from contact);
delete from activity_job where job_id not in (select id from position_description);
delete from activity_candidate where candidate_id not in (select id from candidate);
--delete from activity;
alter table document_types add column tmp varchar(100); update document_types set tmp = lower(name); select * from document_types; --DOCUMENT TYPE
insert into document_types(name, kind, code, tmp) values ('BH Formatted CV','1','formatted_cv','bh formatted cv');
alter table candidate_group add column tmp varchar(100); update candidate_group set tmp = lower(name); select * from candidate_group;
alter table company_department add column tmp varchar(100); update company_department set tmp = lower(department_name); select * from company_department;
alter table candidate_document add column tmp varchar(400); update candidate_document set tmp = uploaded_filename; -- select * from candidate_document limit 10; -- candidate_document
alter table candidate_source add column tmp varchar(400); update candidate_source set tmp = lower(name); --SOURCE
alter table vertical add column tmp varchar(2000); update vertical set tmp = trim(lower(name)); select * from vertical; --IND
alter table functional_expertise alter column id set default nextval('functional_experties_id_seq'); alter table functional_expertise add column tmp varchar(2000); update functional_expertise set tmp = trim(lower(name)); select * from functional_expertise; --FE
alter table sub_functional_expertise add column tmp varchar(2000); update sub_functional_expertise set tmp = trim(lower(name)); select * from sub_functional_expertise; --SFE
alter table offer add column tmp int; --PlacementID
alter table company_location add column tmp varchar(500); --Company_Contact_Worklocation: 
alter table contact_location add column tmp varchar(500);
alter table common_location add column tmp varchar(500); --Contact Location
