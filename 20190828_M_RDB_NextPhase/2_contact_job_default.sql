--JOB links with same contacts but new companies
with dif_job as (select j.JobId
				, j.ClientId
				, j.JobRefNo
				, j.ClientContactId
				, cc.ClientId as Contact_ClientId
				, j.JobTitle
				from jobs j
				left join ClientContacts cc on cc.ClientContactId = j.ClientContactId
				where j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49) --due to deleted companies
				and (j.ClientId <> cc.ClientId or j.ClientContactId is NULL)
				)
, dif_contact as (select JobId, ClientId
				--, concat_ws('_',ClientId,JobId) as dif_contact
				, ClientId as dif_contact
				, 'Default contact' as contact_lname
				, 'Default contact for this company' as contact_note
				from dif_job
				)
select distinct concat('NP', ClientId) as [contact-companyId]
, concat('NP_DEF', dif_contact) as [contact-externalId]
, contact_lname
, contact_note
from dif_contact