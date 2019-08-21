/* Insert new company industry and new industry */
---------------
/* VERSION 2 */

select concat('PRTR',c.company_id) as ComExtID
, i.NewIndustry
, getdate() as Insert_timestamp
from PRTR_company_new_industry c
left join (select distinct IndustryNo, NewIndustry from PRTR_new_industry) i on i.IndustryNo = c.NewIndustry
order by c.company_id

---------------
/* VERSION 1 */

--LIST OF COMPANY INDUSTRY
select listvalue as Industry
, getdate() as Insert_timestamp 
from common.Lists --35 values
where meta_2 = 'Company_sector'
and listkey = 'tblCompanyIndustries'
order by listvalue

--INSERT COMPANY INDUSTRY (CUSTOM SCRIPT)
select concat('PRTR',c.company_id) as ComExtID
, c.company_sector_id
, l.listvalue as Industry
, getdate() as Insert_timestamp
from company.Companies c
left join (select listkey, listvalue, k_id, meta_2
			from common.Lists
			where meta_2 = 'Company_sector'
			and listkey = 'tblCompanyIndustries') as l on l.k_id = c.company_sector_id
where c.company_sector_id > 0 --20483 rows
order by c.company_id

-- >>> LIST OF INDUSTRY <<< -- (to be matched with Candidate Industries)
select listvalue as Industry
, charindex('.',listvalue)
, ltrim(right(listvalue,len(listvalue)-charindex('.',listvalue))) as NewIndustryName
, getdate() as Insert_timestamp 
from common.Lists --35 values
where listkey = 'tblLookupJobExpBizCat'
order by listvalue