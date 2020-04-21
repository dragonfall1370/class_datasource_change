select *
from contact
where firstname = 'David'
and lastname = 'Rose' --0030Y00000csUJeQAM

--PLACEMENT INFO
with placementfilter as (select j.id as job_id
, j.name
, con.id as candidate_id
, con.firstname
, con.lastname
, con.recordtypeid
, a.id as company_id
, a.name as companyname
, j.ts2_contact_c
, con2.firstname as contact_fname
, con2.lastname as contact_lname
, con2.accountid as contact_company_id
, a2.name as contact_company_name
, row_number() over(partition by j.id, con.id order by p.createddate desc) as rn
, p.id as placement_id
from ts2_placement_c p
left join ts2_job_c j on p.ts2_job_c = j.id
left join contact con on p.ts2_employee_c = con.id --candidate
left join contact con2 on con2.id = j.ts2_contact_c --contact
left join account a on a.id = p.ts2_client_c --company
left join account a2 on a2.id = con2.accountid
where 1=1
and ts2_employee_c = '0030Y00000csUJeQAM'
or (con2.id in (select id from contact where accountid = '0010Y00000lKZKhQAO')
or j.ts2_account_c = '0010Y00000lKZKhQAO')
)

select firstname
, lastname
, name as jobtitle
, companyname
, contact_fname
, contact_lname
, contact_company_name
, placement_id
from placementfilter
where rn = 1