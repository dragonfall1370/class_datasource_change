select p.ts2_hiring_manager_c
, ts2_job_c, c.firstname
, c.lastname
, p.id
from ts2_placement_c p
left join contact c on c.id = p.ts2_hiring_manager_c
where p.ts2_client_c = '0010Y00000lKZBEQA4'

select id, ts2_contact_c, ts2_account_c
from ts2_job_c
where id = 'a0K0Y000005OJXLUA4'

select *
from contact
where id = '0030Y00000cqpIbQAI'

select *
from contact
where id = '0030Y00000cqowGQAQ' --Iain Lochhead

--0030Y00000cqjpCQAQ (hr), 0030Y00000eJpskQAC (inv)
select *
from contact
where id in ( '0030Y00000cqowGQAQ', '0030Y00000cqjpCQAQ', '0030Y00000eJpskQAC' )

select *
from account
where id = '0010Y00000lKZKhQAO' --EducationCity

select *
from ts2_placement_c
where id = 'a0Q0Y000009FLrvUAG'

--Check placement for company HiLight Semiconductor Ltd Bristol
select p.id as placement_id
, p.name as placement
, p.ts2_job_c as job_id
, j.name as job_title
, p.ts2_employee_c as candidate_id
, c2.firstname as candidate_fname
, c2.lastname as candidate_lname
, p.ts2_client_c as company_id
, a.name as company_name
, j.ts2_contact_c as contact_id
, con2.firstname as contact_fname
, con2.lastname as contact_lname
, p.ts2_hiring_manager_c
, c.firstname as hiring_manage_fname
, c.lastname as hiring_manage_lname
, p.ts2_accounts_payable_c as payable_contact
, c3.firstname as payable_fname
, c3.lastname as payable_lname
, p.invoice_contact_c as invoice_contact
, c4.firstname as invoice_fname
, c4.lastname as invoice_lname
from ts2_placement_c p
left join contact c on c.id = p.ts2_hiring_manager_c
left join contact c2 on c2.id = p.ts2_employee_c --candidate
left join contact c3 on c3.id = p.ts2_accounts_payable_c --payable contact
left join contact c4 on c4.id = p.invoice_contact_c --invoice contact
left join ts2_job_c j on j.id =  p.ts2_job_c
left join account a on a.id = p.ts2_client_c --company
left join contact con2 on con2.id = j.ts2_contact_c --contact
where p.ts2_client_c = '0010Y00000lKZBEQA4'

--Contact: David Rose
select *
from ts2_placement_c
where ts2_employee_c = '0030Y00000csUJeQAM'
and ts2_job_c = 'a0K0Y000005OHHuUAO'

select *
from contact
where id in ('0030Y00000eJpqqQAC', '0030Y00000cqfy2QAA', '0030Y00000cqfy2QAA') --payable, hiring, inv

select c.id as contact_id
, firstname
, lastname
, a.name as company_name
from contact c
left join account a on c.accountid = a.id
where c.firstname = 'Karl'
and c.lastname = 'Heeks'
and c.recordtypeid = '0120Y0000013O5d'

--Contact: Paul Downing
select c.id as contact_id
, firstname
, lastname
, a.name as company_name
from contact c
left join account a on c.accountid = a.id
where firstname = 'Paul'
and lastname = 'Downing'

--
select p.id
, p.ts2_hiring_manager_c
, j.ts2_contact_c
from ts2_placement_c p
left join ts2_job_c j on j.id =  p.ts2_job_c
where p.ts2_hiring_manager_c <> j.ts2_contact_c

select p.id
, p.ts2_hiring_manager_c 
from ts2_placement_c p
where p.ts2_hiring_manager_c is NULL or p.ts2_hiring_manager_c = ''