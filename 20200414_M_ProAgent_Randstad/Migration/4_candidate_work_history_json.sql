--After injecting to cand_work_history
select distinct cand_ext_id
, getdate() as insert_timestamp --removed from json
	, (select jobTitle as jobTitle
	, currentEmployer as currentEmployer
	--, case when rn = 1 then 1 else 0 end as cbEmployer
	, convert(varchar(10), dateRangeFrom, 120) as dateRangeFrom
	, convert(varchar(10), dateRangeTo, 120) as dateRangeTo
	, company as company
		from cand_work_history where cand_ext_id = m.cand_ext_id
		order by rn asc
		for json path
		) as cand_work_history
into vc_cand_work_history
from cand_work_history m -- rows (distinct to get 1 unique json for unique candidate)
--where m.cand_ext_id = 'CDT154379'
order by cand_ext_id