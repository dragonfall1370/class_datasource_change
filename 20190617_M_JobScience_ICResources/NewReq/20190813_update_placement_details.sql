--From DMPFRA remote desktop
CREATE TABLE placement_14082019
(
	placement_id character varying(50)
	, VCJobID integer
	, VCCandidateID integer
	, VCContactID integer
	, VCCompanyID integer
	, VCCompanyName character varying(2000)
)

copy placement_14082019
from 'H:\ic-resources\Working\20190814_placement_updates.csv' DELIMITER ',' CSV HEADER;\

--To VC production
CREATE TABLE mike_dm919_placement_14082019
(
	placement_id character varying(50)
	, VCJobID integer
	, VCCandidateID integer
	, VCContactID integer
	, VCCompanyID integer
	, VCCompanyName character varying(2000)
)

--BACKUP CURRENT OFFER PERSONAL INFO
select *
into mike_offer_personal_info_bkup_20190814
from offer_personal_info --9305 rows


--CHECK REFERENCES
select op.offer_id, op.client_company_id, op.client_company_name
, m.vccompanyid
, com.name as new_company_name
, op.client_contact_id, op.client_contact_name
, op.client_contact_email
, m.vccontactid
, concat_ws(' ', trim(con.first_name), trim(con.last_name))
, o.position_candidate_id
, pc.candidate_id
, m.vccandidateid
, pc.position_description_id
, m.vcjobid
, pc.status
from offer_personal_info op
inner join offer o on o.id = op.offer_id
inner join position_candidate pc on pc.id = o.position_candidate_id
inner join mike_dm919_placement_14082019 m on m.vccandidateid = pc.candidate_id and m.vcjobid = pc.position_description_id
inner join company com on com.id = m.vccompanyid
inner join contact con on con.id = m.vccontactid
where m.vccompanyid = 26391 --check 1 row

--MAIN SCRIPT
update offer_personal_info
set client_company_id = m.vccompanyid
, client_company_name = com.name
, client_contact_id = m.vccontactid
, client_contact_name = concat_ws(' ', trim(con.first_name), trim(con.last_name))
, client_contact_email = con.email
from offer o
inner join position_candidate pc on pc.id = o.position_candidate_id
inner join mike_dm919_placement_14082019 m on m.vccandidateid = pc.candidate_id and m.vcjobid = pc.position_description_id
inner join company com on com.id = m.vccompanyid
inner join contact con on con.id = m.vccontactid
where o.id = offer_personal_info.offer_id --update 392 rows
--and offer_personal_info.offer_id = 1983
--and m.vccompanyid = 26391 (same company of this offer)