--Communications without emails
with company_split as (select c.__pk as communication_id
	, _fk_company_list
	, trim(value) as company_id
	, _fk_consultant
	, _fk_contact_to_list
	, [subject]
	, [type]
	, replace(body, char(11), char(10)) communication_body
	, email_body
	, email_from_name
	, email_recipient_names
	, stamp_created
	from [20191030_153228_communications] c
	cross apply string_split(_fk_company_list, char(11))
	where _fk_email is NULL)

/*--Audit Communications
select *
from [20191030_153228_communications]
where _fk_email is NULL --3648
*/

, contact_split as (select communication_id
	, company_id
	, _fk_consultant
	, _fk_contact_to_list
	, trim(value) as contact_id
	, [subject]
	, [type]
	, communication_body
	, email_body
	, email_from_name
	, email_recipient_names
	, stamp_created
	from company_split c
	cross apply string_split(_fk_contact_to_list, char(11)))

select communication_id
	, c._fk_consultant
	, company_id
	, contact_id
	, con.name_first
	, con.name_last
	, [subject]
	, c.[type]
	, communication_body
	, email_body
	, email_from_name
	, email_recipient_names
	, c.stamp_created
from contact_split c
inner join [20191030_153350_contacts] con on con.__pk = c.contact_id