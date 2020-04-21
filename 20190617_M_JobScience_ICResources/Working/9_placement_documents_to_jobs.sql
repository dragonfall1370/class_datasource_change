select a.parentid as placement_id
			, a.id as document_id
			, concat(a.id,'.',substring(a.name from '\.([^\.]*)$')) as filename
			, a.name as actual_filename
			, p.ts2_job_c as job_ext_id --put placement documents into job documents
			, p.ts2_employee_c as cand_ext_id
			, a.createddate::timestamp
			from attachment a
			left join ts2_placement_c p on p.id = a.parentid --reference value
			where a.parentid is not null and substring(a.name from '\.([^\.]*)$') <> ''
			and a.parentid in (select id from ts2_placement_c) --593 rows
			
UNION ALL

select
      cdl.linkedentityid as placement_id
      , cv.id as document_id
			, concat(cv.id,'.',substring(cv.pathonclient from '\.([^\.]*)$')) as filename
			--, cv.islatest, cv.title, cv.description, cv.reasonforchange
			, cv.pathonclient as actual_filename
			, p.ts2_job_c as job_ext_id --put placement documents into job documents
			, p.ts2_employee_c as cand_ext_id
			, cv.createddate::timestamp
      from contentdocumentlink cdl
      left join contentversion cv on cv.contentdocumentid = cdl.contentdocumentid
			left join ts2_placement_c p on p.id = cdl.linkedentityid --reference value
      where cv.contentdocumentid is not null and substring(cv.pathonclient from '\.([^\.]*)$') <> ''
      and cdl.linkedentityid in (select id from ts2_placement_c) --483 rows