--Company Trading Name
with company_longname as (SELECT idcompany
		, idalias
		, createdon
		, row_number() over (partition by idcompany order by createdon::date desc) rn
		FROM "company_alias")
		
select cl.idcompany
, cl.idalias
, a.aliasname
from company_longname cl
left join "alias" a on a.idalias = cl.idalias
where cl.rn = 1