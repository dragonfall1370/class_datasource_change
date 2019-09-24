--DUPLICATION REGCONITION
with SplitEmail as (select distinct id
	, translate(value, '!'':"<>[]();,', '            ') as SplitEmail --to translate special characters
	from Contacts
	cross apply string_split(trim(emailsprimary),char(10))
	where emailsprimary like '%_@_%.__%')
			
, dup as (select id
	, trim(SplitEmail) as EmailAddress
	, row_number() over(partition by trim(SplitEmail) order by id asc) as rn --distinct email if emails exist more than once
	, row_number() over(partition by id order by trim(SplitEmail)) as Contactrn --distinct if contacts may have more than 1 email
	from SplitEmail
	where SplitEmail like '%_@_%.__%')

, PrimaryEmail as (select id
	, case when rn > 1 then concat(rn,'_',trim(EmailAddress))
	else trim(EmailAddress) end as PrimaryEmail
	from dup
	where EmailAddress is not NULL and EmailAddress <> ''
	and Contactrn = 1)

--CA Global Division		
, Division as (select cv.contacts_id
	, cv.cf_value
	, cf.label as division
	from contacts_custom_fields_value cv
	left join contacts_custom_fields_153605 cf on cf.id = cv.cf_value
	where cv.cf_id = 153605) --no contact having > 1 value

--CONTACT FINAL DOCUMENTS (after creating companyDocument table)
, Documents as (select data_item_id, string_agg(cast(concat(id
				, right(filename, charindex('.',reverse(filename)))) as nvarchar(max)),',') as contactDocuments
	from attachments
	where data_item_type = 'contact'
	group by data_item_id)

--MAIN SCRIPT
select concat('CG',c.id) as [contact-externalId]
	, case when c.companyid in ('0') or companyid is NULL or c.companyid not in (select id from companies) then 'CG999999999'
		else concat('CG',c.companyid) end as [contact-companyId]
	, case when c.firstname = '' or c.firstname is NULL then concat('Firstname','_',c.id)
		else c.firstname end as [contact-firstName]
	, case when c.lastname = '' or c.lastname is NULL then 'Lastname'
		else c.lastname end as [contact-lastName]
	, c.title as [contact-jobTitle]
	, pe.PrimaryEmail as [contact-email]
	, c.emailssecondary as PersonalEmail --CUSTOM SCRIPT #: Personal Email
	, concat_ws(', ',nullif(c.phoneswork,''),nullif(c.phonesother,'')) as [contact-phone]
	, c.phonescell as ContactMobile --CUSTOM SCRIPT #: Contact Mobile
	, case when c.ownerid in ('0') then NULL
		else trim(u.username) end as [contact-owners]
	, d2.contactDocuments as [contact-document]
	, concat_ws(char(10),concat('Contact External ID: ',c.id)
		, concat('Created: ', convert(nvarchar(20),c.datecreated,120))
		, concat('Updated: ', convert(nvarchar(20),c.datemodified,120))
		, coalesce('Status: ' + nullif(case when c.statusid = 5334650 then 'Procurement Team Contact'
											when c.statusid = 3304036 then 'Employee: Placement'
											when c.statusid = 3304042 then 'Human Resources'
											when c.statusid = 3304039 then 'Line Manager'
											when c.statusid = 3304048 then 'Contractor'
											when c.statusid = 3656098 then 'Prospective Client'
											when c.statusid = 3304045 then 'Emplyee: Left Company'
											else NULL end,''), NULL)
		, coalesce('Contact Location Address: ' + nullif(concat_ws(', ', nullif(c.addressstreet,'')
			, nullif(c.addresscity,''), nullif(c.addressstate,''), nullif(c.addresspostalcode,''), nullif(cc.country,''), nullif(c.countrycode,'')), ', '), NULL)
		, coalesce('CA Global Division: ' + nullif(d.division,''), NUll)
		, coalesce('***Notes: ' + nullif(convert(nvarchar(max),c.notes),''),NULL)
		) as [contact-note]
from contacts c
left join PrimaryEmail pe on pe.id = c.id
left join users u on u.id = c.ownerid
left join Division d on d.contacts_id = c.id
left join Documents d2 on d2.data_item_id = c.id
left join country_code_nationality cc on cc.countrycode = c.countrycode --for country reference

UNION ALL

select 'CG999999999','CG999999999','Default','Contact','','','','','','','','This is default contact from data migration'

/* DEFAULT CONTACT

select concat('CG',id) as [contact-companyId]
, concat('CG9999999',id) as [contact-externalId]
, concat('Default contact - ',name) as [contact-lastName]
, 'This is default contact for each company from data migration' as [contact-note]
from companies

*/