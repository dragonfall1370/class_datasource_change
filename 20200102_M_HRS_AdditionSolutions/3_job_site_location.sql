with company_address as (select __pk as address_id
		, _fk_company
		, _fk_site
		, type
		, row_number() over(partition by _fk_company order by _fk_site desc, __pk desc) as rn
		from [20191030_153215_addresses]
		where _fk_company is not NULL)

--Site Location
select concat('AS', j.__pk) as job_ext_id
	, concat('AS', j._fk_company) as com_ext_id
	, j._fk_site
	, s.site_name
	, s.notes
	, a.address_id
	, a.type
from [20191030_155620_jobs] j
left join [20191030_160039_sites] s on s.__pk = j._fk_site
left join (select * from company_address where rn = 1) a on a._fk_company = s._fk_company
where j._fk_site is not NULL