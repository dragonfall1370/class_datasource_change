select at.parentid, at.id as fileid
				, concat(at.id,'.',substring(at.name from '\.([^\.]*)$')) filename
				, at.name as actual_filename
				, at.createddate::timestamp as createddate
	    from attachment at
        left join (select id, name from Account) a on a.id = at.parentid --COMPANY
        left join (select id, concat(firstname,' ',lastname) as fullname, email, title
					from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = at.parentid --CONTACT
        left join (select Id, name, ts2_job_number_c from ts2_job_c) j on j.id = at.parentid --JOB
        left join (select id, concat(firstname,' ',lastname) as fullname, email, title 
					from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = at.parentid
	    where at.parentid is not null and substring(at.name from '\.([^\.]*)$') <> ''
		and (a.id is not null or con.id is not null or j.id is not null or can.id is not null)
                   --and a.id is not null --1444
                   and con.id is not null
                   --and j.id is not null --356
                   --and can.id is not null

UNION

select a.linkedentityid, a.fileid, filename, actual_filename, createddate
          from (
                   select
                        cdl.linkedentityid --, cdl.id, cdl.contentdocumentid, cdl.isdeleted --, cdl.*
                          , a.id as "company_id", a.name as "company_name"
                          , con.id as "contact_id", con.fullname as "contact_name", con.title as "Contact Title"
                          , j.id as "job_id", j.name as "job_title", j.ts2_job_number_c as "Job Number"
                          , can.id as "candidate_id", can.fullname as "candidate_name", can.title as "Candidate Title"
                          , cv.id as "fileid", concat(cv.id,'.',substring(cv.pathonclient from '\.([^\.]*)$')) "filename"
						, cv.id as "file_name", cv.islatest, cv.title, cv.description, cv.reasonforchange
						, cv.pathonclient as "actual_filename", cv.createddate::timestamp
						, cv.filetype, cv.contentsize --cv.isdeleted,
                   from contentdocumentlink cdl
                   left join contentversion cv on cv.contentdocumentid = cdl.contentdocumentid
                   left join (select id, name from Account) a on a.id = cdl.linkedentityid --COMPANY
                   left join (select id, concat(firstname,' ',lastname) as fullname, email, title
								from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = cdl.linkedentityid --CONTACT
                   left join (select Id, name, ts2_job_number_c from ts2_job_c) j on j.id = cdl.linkedentityid --JOB
                   left join (select id, concat(firstname,' ',lastname) as fullname, email, title 
								from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = cdl.linkedentityid --CANDIDATE
                   where cv.contentdocumentid is not null and substring(cv.pathonclient from '\.([^\.]*)$') <> ''
                   and (a.id is not null or con.id is not null or j.id is not null or can.id is not null)
                   --and a.id is not null --1444
                   and con.id is not null
                   --and j.id is not null --356
                   --and can.id is not null
				   ) a