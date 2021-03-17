select uniqueid as cand_ext_id
, "3 status codegroup   4"
, case when c.description = 'Active' then 1
		when c.description = 'Pre-Registered' then 0
		when c.description = 'Temp to Perm' then 3
		when c.description = 'Placed by Us' then 2
		end as active
from f01 f
left join (select * from codes where codegroup = '4') c on c.code = f."3 status codegroup   4"
where "3 status codegroup   4" is not NULL
and "101 candidate codegroup  23" = 'Y'