with company_industry as (SELECT idcompany
	, TRIM(s.idindustry) idindustry
	FROM companyx cx, UNNEST(string_to_array(cx.idindustry_string_list, ',')) s (idindustry))
	
select distinct idcompany as com_ext_id
, ci.idindustry
, trim(i.value) as company_industry
, current_timestamp as insert_timestamp
from company_industry ci
left join industry i on i.idindustry = ci.idindustry


/* CHECK INDUSTRY
with company_industry as (SELECT idcompany
	, TRIM(s.idindustry) idindustry
	FROM companyx cx, UNNEST(string_to_array(cx.idindustry_string_list, ',')) s (idindustry))
	
select idcompany
, ci.idindustry
, i.value as company_industry
, current_timestamp as insert_timestamp
from company_industry ci
left join industry i on i.idindustry = ci.idindustry

UNION ALL

select c.idcompany
, c.idudpracticegroup
, '[Practice Group] ' || u.value as practice_group
, current_timestamp as insert_timestamp
from companyext c
left join udpracticegroup u on u.idudpracticegroup = c.idudpracticegroup
where c.idudpracticegroup is not NULL
*/