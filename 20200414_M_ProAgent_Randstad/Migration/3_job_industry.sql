--Job Industry get from Vincere Company | position_industry limit to 1 value
with com_industry as (select industry_id, company_id, insert_timestamp
					, row_number() over(partition by company_id order by industry_id asc) as rn
				from company_industry)

				
update position_description pd
set vertical_id = ci.industry_id
from (select * from com_industry where rn = 1) ci 
where 1=1
and ci.company_id = pd.company_id
and pd.external_id is not NULL
and pd.deleted_timestamp is NULL