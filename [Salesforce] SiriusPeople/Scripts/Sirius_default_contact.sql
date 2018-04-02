--DEFAULT CONTACT MAIN SCRIPT
select concat(ID,'-DC') as 'contact-externalId'
, ID as 'contact-companyId'
, 'Default' as 'contact-lastName'
, concat('Contact-',ID) as 'contact-firstName'
, 'This is default contact for company' as 'contact-note'
from Company