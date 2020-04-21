--Candidate company name/number
select c.id as candidate_id
, c.limited_company_c
, case when a."name" is NULL then c.umbrella_company_name_c
	else a."name" end as company_name
, a.reg_no_c as company_number --candidate company number (Reg Number)
, c.umbrella_company_name_c
from contact c
left join account a on a.id = c.limited_company_c
where c.recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')
and (c.limited_company_c is not NULL --1123 rows
		or c.umbrella_company_name_c is not NULL) --1124 rows
		
/* AUDIT 1 candidate with umbrella company --0030Y00000csQxEQAU
--Candidate company name/number
select c.id as candidate_id
, c.limited_company_c
, a."name" as company_name
, a.reg_no_c as company_number --candidate company number (Reg Number)
, c.umbrella_company_name_c
from contact c
left join account a on a.id = c.limited_company_c
where c.recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')
and (c.limited_company_c is NULL --1123 rows
		and c.umbrella_company_name_c is not NULL) --1124 rows
*/