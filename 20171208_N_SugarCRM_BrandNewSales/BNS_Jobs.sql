-- Note: 
-- + To job have linked to contacts, get contact_id from temp_job_contact1
-- + To job have linked to only company, get contact_id from temp_get_contact_for_job_linked_to_company
-- + to job not linked to any company, get the default contact id from default company
-- Create temp table for job linked to contact
create view temp_job_contact1 as
(select oc.*, ac.account_id, ac.id as 'contactId'
 from opportunities_contacts oc left join accounts_contacts ac on oc.contact_id = ac.contact_id 
 where ac.contact_id is not null)
-- select * from temp_job_contact1 order by contactid
 
--  create temp table to get the contact for jobs linked to the companies (have contacts)
-- 1. get the latest contact of a company
create view temp_get1contact_1account_updated as
(select id, contact_id, account_id, max(date_modified)
from accounts_contacts
group by account_id)
-- select * from temp_get1contact_1account_updated order by id

-- 2.  Create a default contact for those companies have no contacts to link the jobs to these contacts
 -- NOTICE: MUST READ: these contact will also have to be imported to the system first using the script from below table
create view temp_default_contact_for_company_updated2 as
(select concat('BNS_',a.id) as'contact-companyId'
 , a.name as 'companyName'
 , a.id as 'original_companyID'
 , concat('BNS_DefaultCon_',a.id) as 'contact-externalId'
 , concat('DefaultCon_',a.id) as 'contactExternalId'
 , if(a.name = '' or a.name is null, concat(a.id,' - Default Contact'), concat (a.name,' - Default Contact')) as 'contact-LastName'
 from accounts a where a.id not in (select account_id from accounts_contacts))
-- select * from temp_default_contact_for_company_updated2 order by 'contactexternalId'

-- 3. Combine 2 table to get contact for jobs linked to company
create view temp_get_contact_for_job_linked_to_company2 as
(select ao.opportunity_id, ao.account_id, coalesce(tgca.id,tdcc.contactExternalId) as contact_id
from accounts_opportunities ao 
	left join temp_get1contact_1account_updated tgca on ao.account_id = tgca.account_id
    left join temp_default_contact_for_company_updated2 tdcc on ao.account_id = tdcc.original_companyID)
-- select * from temp_get_contact_for_job_linked_to_company2 where opportunity_id = '66dba590-59dc-ddbb-0d4f-55aea85700eb'

-- Check duplicate job names
create view dup as (
select o.id, o.name, count(*) as rn
from opportunities o join opportunities o1 on o.name = o1.name
and o.id >= o1.id 
group by o.name, o.id)
-- select * from dup

--MAIN SCRIPT
select 
case 
	when tjc.contactId is not null then concat('BNS_',tjc.contactId)
	when tgcj.contact_id is not null then concat('BNS_',tgcj.contact_id)
	else 'BNS_9999999' end as 'position-contactId'
, concat('BNS_',o.id) as 'position-externalId'
, o.name as 'position-title(old)'
, if(o.id in (select id from dup where dup.rn > 1)
	, if(dup.name = '' or dup.name is NULL,concat('No job title-',dup.id),concat('DUPLICATE',dup.rn,' - ',dup.name))
	, if(o.name = '' or o.name is null,concat('No job title -',o.id),o.name)) as 'position-title'
, ue.email_address as 'position-owners'
, left(o.date_entered,10) as 'position-startDate'
, replace(facturatiedatum_c,' ','') as 'position-endDate'
, case 
	when aantal_vacatures_c = 0  then 1
    when aantal_vacatures_c is null then 1
    else aantal_vacatures_c end as 'position-headcount'
, left(
	concat('Job External ID: BNS_',o.id,char(10)
	, if(o.sales_stage = '' or o.sales_stage is NULL,'',concat(char(10),'Sales Stage: ',o.sales_stage))),32000)
 as 'position-note'
from opportunities o 
	left join opportunities_cstm oc on o.id = oc.id_c
	left join user_emails_main ue on o.assigned_user_id = ue.id
    left join temp_job_contact1 tjc on o.id = tjc.opportunity_id
    left join temp_get_contact_for_job_linked_to_company2 tgcj on o.id = tgcj.opportunity_id
    left join dup on o.id = dup.id
order by o.name