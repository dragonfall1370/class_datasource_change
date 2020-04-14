---CHECK INDUSTRY
select *
from vertical
order by name

--CHECK FE/SFE
select --distinct 
fe.id as fe_id
, fe.name
, sfe.id as sfe_id
--, sfe.*
, sfe.name as sfe_name
from sub_functional_expertise sfe
join functional_expertise fe on fe.id = sfe.functional_expertise_id
where 1=1
and fe.id > 3043
order by sfe.functional_expertise_id, sfe.name


select *
from candidate_source
order by id desc