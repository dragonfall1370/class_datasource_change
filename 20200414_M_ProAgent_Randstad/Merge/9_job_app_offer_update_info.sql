with update_info as (select pc.id
	, pc.position_description_id
	, pd.name as job_title
	, pd.company_id as client_company_id
	, com.name as client_company_name
	, pd.company_id_bkup
	, pd.contact_id as client_contact_id
	, pd.contact_id_bkup
	--, con.first_name
	--, con.last_name
	, concat_ws(' ', con.first_name, con.last_name) as client_contact_name
	, con.email as client_contact_email
	, con.phone as client_contact_phone
	, pc.candidate_id
	, pc.candidate_id_bkup
	, can.first_name
	, can.last_name
	, can.email
	, can.phone
	, pc.insert_timestamp
	from position_candidate pc
		left join position_description pd on pd.id = pc.position_description_id
			left join company com on com.id = pd.company_id
			left join contact con on con.id = pd.contact_id
		left join candidate can on can.id = pc.candidate_id
		left join candidate can2 on can2.id = pc.candidate_id_bkup --comparision
	where pc.status >=200
	--and pc.insert_timestamp > '2020-07-04'
	order by pc.insert_timestamp desc
	)
	
--Comparison | used for update offer_personal_info
select o.id as offer_id
, o.position_candidate_id
--Current data
, opi.first_name
, opi.last_name
, opi.email
, opi.phone
, opi.client_company_id
, opi.client_company_name
, opi.client_contact_id
, opi.client_contact_name
, opi.client_contact_email
, opi.client_contact_phone
--New modified data
, u.first_name first_name_new
, u.last_name last_name_new
, u.email email_new
, u.phone phone_new
, u.client_company_id client_company_id_new
, u.client_company_name client_company_name_new
, u.client_contact_id client_contact_id_new
, u.client_contact_name client_contact_name_new
, u.client_contact_email client_contact_email_new
, u.client_contact_phone client_contact_phone_new
--into mike_tmp_offer_merged_update_info
from offer o
left join offer_personal_info opi on opi.offer_id = o.id
left join update_info u on u.id = o.position_candidate_id
where u.insert_timestamp > '2020-07-04' --limit the number of offers added


---UPDATE OFFER_PERSONAL_INFO
update offer_personal_info o
set
first_name = m.first_name_new
, last_name = m.last_name_new
, email = m.email_new
, phone = m.phone_new
, client_company_id = m.client_company_id_new
, client_company_name = m.client_company_name_new
, client_contact_id = m.client_contact_id_new
, client_contact_name = m.client_contact_name_new
, client_contact_email = m.client_contact_email_new
, client_contact_phone = m.client_contact_phone_new
from mike_tmp_offer_merged_update_info m
where o.offer_id = m.offer_id