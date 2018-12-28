
select [4 RefNumber Numeric], [150 Department Alphanumeric]
 from f01
where [150 Department Alphanumeric] is not null

/*
update table contact
set department = t.department
from #temp_department as t
where t.contactid = replace(contact.external_id, 'BB - ', '')
*/