select a.id as com_ext_id
, a.name as company_name
, a.parentid as parent_com_ext_id
, a2.name as parent_company_name
from account a
left join account a2 on a.parentid = a2.id --parent reference value
where a.parentid <> '000000000000000AAA' --2393 rows