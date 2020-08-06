select pc.id as jobappid
	, pc.position_description_id
	, pc.candidate_id
	, pd.external_id
	, c.external_id
	, pc.rejected_date
	from position_candidate pc
	left join position_description pd on pd.id = pc.position_description_id
	left join candidate c on c.id = pc.candidate_id
	where 1=1
	and pd.id = '96218'
	and c.id = '123781'
	--and pc.status >= 200
	
	
	select pc.id as jobappid
	, pc.position_description_id
	, pc.candidate_id
	, pd.external_id
	, c.external_id
	, pc.rejected_date
	from position_candidate pc
	left join position_description pd on pd.id = pc.position_description_id
	left join candidate c on c.id = pc.candidate_id
	where 1=1
	and pd.external_id = 'JOB001689'
	and c.external_id = 'CDT002832'