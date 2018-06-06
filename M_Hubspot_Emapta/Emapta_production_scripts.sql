--USER OWNERS
select id, name, email from user_account
where locked_user = 0 and deleted_timestamp is NULL

--LIST OF EXISTING JOBS
select pd.id, pd.name, pd.company_id, cc.name, pd.contact_id, c.first_name, c.last_name, c.email
from position_description pd
left join contact c on c.id = pd.contact_id
left join company cc on cc.id = pd.company_id
where c.deleted_timestamp is NULL and cc.deleted_timestamp is NULL

--CONTACTS
select c.id, c.first_name, c.last_name, c.email, c.company_id, cc.name from contact c
left join company cc on cc.id = c.company_id
where c.deleted_timestamp is NULL

--COMPANIES
select id, name from company
where deleted_timestamp is NULL