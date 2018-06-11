-- Get email address for Contact
create view Temp_Contact_Emails_Addr1 as
(select ac.id, ac.contact_id, eabr.email_address_id, ea.email_address, c.first_name, c.last_name, eabr.date_modified, max(eabr.date_modified)
from email_addr_bean_rel eabr
 left join email_addresses ea on eabr.email_address_id = ea.id
 left join accounts_contacts ac on eabr.bean_id = ac.contact_id
 left join contacts c on ac.contact_id = c.id
 where ac.contact_id is not null and c.id is not null
 and eabr.primary_address = 1
 and eabr.bean_module = 'Contacts'
-- where eabr.bean_id in (select contact_id from accounts_contacts)
group by ac.id)
-- select * from Temp_Contact_Emails_Addr1 order by email_address
-- Count no of duplicate emails in contacts
create view Temp_Contact_Emails_Addr_3 as
(select tce.*, count(*) as rn
from Temp_Contact_Emails_Addr1 tce join Temp_Contact_Emails_Addr1 tcea on tce.email_address = tcea.email_address
and tce.id >= tcea.id and tce.email_address like '%_@_%.__%'
group by tce.email_address, tce.id)
-- select * from Temp_Contact_Emails_Addr_3 order by email_address

create view Contact_Email_Addr1 as
(select id, contact_id,
case 
when rn=1 then email_address
else concat(rn,'_',(email_address))
end as email
from Temp_Contact_Emails_Addr_3)
-- select * from Contact_Email_Addr
-- -----------------------------------Get contact address
create view contact_location as 
	(select ac.id, ac.contact_id,
		ltrim(rtrim(concat(
			if(c.primary_address_street = '' or c.primary_address_street,'',c.primary_address_street)
			, if(c.primary_address_city = '' or c.primary_address_city is NULL,'',concat(', ',c.primary_address_city))
			, if(c.primary_address_state = '' or c.primary_address_state is NULL,'',concat(', ',c.primary_address_state))
			, if(c.primary_address_postalcode = '' or c.primary_address_postalcode is NULL,'',concat(', ',c.primary_address_postalcode))
			, if(c.primary_address_country = '' or c.primary_address_country is NULL,'',if(c.primary_address_country = 'NL',', Netherlands',concat(', ',c.primary_address_country))))))
		as 'locationName'
	from accounts_contacts ac left join contacts c on ac.contact_id = c.id)
-- -----------------------------------MAIN SCRIPT
select
	concat('BNS_',ac.account_id) as'contact-companyId'
    , a.name as 'companyName'
	, concat('BNS_',ac.id) as 'contact-externalId'
	, if(c.first_name = '' or c.first_name is null, 'NoFirstName',first_name) as 'contact-firstName'
	, if(c.last_name = '' or c.last_name is null, 'NoFirstName',last_name) as 'contact-lastName'
	, coalesce(cc.gewenste_functietitel_c,c.title) as 'contact-jobTitle'
    , cea.email as 'contact-email'
    , c.phone_work as 'contact-phone'
    , ue.email_address as 'contact-owners'
    , if(cc.linkedin_url_c = '' or cc.linkedin_url_c is null, if(cc.linkedin_profiel_c like '%linkedin.com%',cc.linkedin_profiel_c,null), if(cc.linkedin_url_c like '%linkedin.com%',cc.linkedin_url_c, null)) as 'contact-linkedin'
    , left(concat('Contact External ID: BNS_',ac.id,char(10),
		 concat(char(10),'Contact custom ID: BNS_',ac.contact_id,char(10)), -- '
         concat(char(10),'Company External ID: BNS_',ac.account_id,char(10)),
		 if(c.phone_home = '' or c.phone_home is NULL,'',Concat(char(10), 'Home Phone: ', c.phone_home, char(10))),
         if(c.phone_mobile = '' or c.phone_mobile is NULL,'',Concat(char(10), 'Mobile Phone: ', c.phone_mobile, char(10))),
		 if(c.phone_other = '' or c.phone_other is NULL,'',Concat(char(10), 'Other Phone: ', c.phone_other, char(10))),
		 if(cl.locationName = '' or cl.locationName is null, '', concat(char(10),'Address: ',if(left(cl.locationName,2)=', ',right(cl.locationName,length(cl.locationName)-2),cl.locationName),char(10))),
		 if(cc.geslacht_c = ''  or cc.geslacht_c is NULL,'',Concat(char(10), 'Gender (Geslacht): ', cc.geslacht_c, char(10))),
		 if(c.birthdate is NULL,'',Concat(char(10), 'Birthdate: ', c.birthdate, char(10))),
         if(cc.her_publicatie_datum_c is NULL,'',Concat(char(10), 'Date of Publication: ', cc.her_publicatie_datum_c, char(10))),
         if(cc.opleidingsniveau_c = '' or cc.opleidingsniveau_c is NULL,'',Concat(char(10), 'Education (Opleidingsniveau): ', cc.opleidingsniveau_c, char(10))),
         if(c.date_entered is NULL,'',Concat(char(10), 'Date/time entered: ', c.date_entered, char(10))),
         if(c.date_modified is NULL,'',Concat(char(10), 'Date/Time modified: ', c.date_modified, char(10))),
         if(c.description = '' or c.description is NULL,'',Concat(char(10), 'Notes: ', char(10), c.description))
         ),32000)
 as 'contact-note'
from accounts_contacts ac left join contacts c on ac.contact_id = c.id
					 left join contacts_cstm cc on c.id = cc.id_c
                     left join accounts a on ac.account_id = a.id
                     left join Contact_Email_Addr1 cea on ac.id = cea.id
                     left join user_emails_main ue on c.assigned_user_id = ue.id
                     left join contact_location cl on ac.id = cl.id
where ac.contact_id is not null and a.id is not null-- and ac.contact_id = 'dd40aa66-be84-086b-fa6e-4d1900a3cc31'-- and ac.contact_id = '4f7765a8-8888-5312-2a73-4940f0f54d17'
UNION ALL
select 'BNS_9999999','','BNS_9999999','Default','Contact','','','','','','This is default contact from Data Import'