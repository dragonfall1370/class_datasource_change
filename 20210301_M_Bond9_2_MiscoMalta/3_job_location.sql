select pd.id
, pd.company_id
, pd.company_location_id
, cl.id
, cl.address
, cl.post_code
, cl.country_code
from position_description pd
left join company_location cl on cl.company_id = pd.company_id
where cl.id is not NULL and (cl.address is not NULL or cl.country_code is not NULL)

--MAIN SCRIPT
update position_description pd
set company_location_id = cl.id
from company_location cl
where pd.company_id = cl.company_id
and cl.id is not NULL and (cl.address is not NULL or cl.country_code is not NULL) --5469

--NONE
select company_id
from company_location
group by company_id
having count(*) > 1