/* POPULATE NEW INDUSTRY - new industry file received */
select distinct NewIndustry
, getdate() as Insert_timestamp 
from PRTR_new_industry
order by NewIndustry

---------------
/* VERSION 2 */
with CandidateIndustry as (select c.cn_id
, c.job_company_industry
, l.listvalue as CandidateIndustry
, l2.listvalue as CandidateIndustryCompany
, getdate() as Insert_timestamp
from candidate.Jobs c
left join (select listkey, listvalue, k_id, meta_2
			from common.Lists
			where listkey = 'tblLookupJobExpBizCat') as l on l.k_id = c.job_company_industry
left join (select listkey, listvalue, k_id, meta_2
			from common.Lists
			where meta_2 = 'Company_sector'
			and listkey = 'tblCompanyIndustries') as l2 on l2.k_id = c.job_company_industry
where c.job_company_industry > 0 --20483 rows
)

/* PRTR update new Industry for Company/Candidate */

--MAIN SCRIPT
select distinct concat('PRTR',ci.cn_id) as CandidateExtID
, ci.CandidateIndustry
, ci.CandidateIndustryCompany
, i.NewIndustry --
, ci.Insert_timestamp
from CandidateIndustry ci
left join PRTR_new_industry i on i.OldIndustry = ci.CandidateIndustry
where ci.CandidateIndustryCompany is not NULL
order by CandidateExtID desc


---------------
/* VERSION 1 */
with CandidateIndustry as (select c.cn_id
, c.job_company_industry
, l.listvalue as CandidateIndustry
, l2.listvalue as CandidateIndustryCompany
, getdate() as Insert_timestamp
from candidate.Jobs c
left join (select listkey, listvalue, k_id, meta_2
			from common.Lists
			where listkey = 'tblLookupJobExpBizCat') as l on l.k_id = c.job_company_industry
left join (select listkey, listvalue, k_id, meta_2
			from common.Lists
			where meta_2 = 'Company_sector'
			and listkey = 'tblCompanyIndustries') as l2 on l2.k_id = c.job_company_industry
where c.job_company_industry > 0 --20483 rows
)

select distinct concat('PRTR',cn_id) as CandidateExtID
, CandidateIndustry
, CandidateIndustryCompany
, Insert_timestamp
from CandidateIndustry
where CandidateIndustryCompany is not NULL
order by CandidateExtID desc