select concat('REBEL',m.cand_ref) as CanExternalId, -10 as userId
		--, event_make_datetime
		--, iif(CHARINDEX('-',content_date)<>0,concat(substring(content_date,6,2),'/',substring(content_date,9,2),'/',left(content_date,4),right(content_time,9)),concat(content_date,right(content_time,9))) as InsertTimeStamp
		, content_date, content_time
		, case 
				--when event_make_datetime = '00/00/0000 00:00:00' then getdate()
				when content_date like '%/%' then convert(datetime,concat(content_date,right(content_time,9)),103)
				else convert(datetime,concat(substring(content_date,6,2),'/',substring(content_date,9,2),'/',left(content_date,4),right(content_time,9)),103) end as InsertTimeStamp
		, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		, concat('--MIGRATED FROM MMSENTCONTENTS--',
				--iif(event_make_datetime = '' or event_make_datetime = 'null', '', concat(char(10), 'Event Make Date Time: ',event_make_datetime)),
				iif(CHARINDEX('-',content_date)<>0,concat(char(10), 'Content Date Time: ', substring(content_date,6,2),'/',substring(content_date,9,2),'/',left(content_date,4),right(content_time,9)),concat(char(10), 'Content Date Time: ',content_date,right(content_time,9))),
				iif(content_subject = '' or content_subject = 'null', '', concat(char(10),'Content Subject: ',content_subject)),
				iif(cst.consult_name = '' or cst.consult_name = 'null', '', concat(char(10),'Consultant User: ',cst.consult_name)),
				iif(cst1.consult_name = '' or cst1.consult_name = 'null', '', concat(char(10),'Consultant From: ',cst1.consult_name)),
				concat(char(10),'Content Method: E'),
				iif(content_mode = '' or m.content_mode = 'null', '', concat(char(10),'Content Mode: ',content_mode)),
				iif(m.cand_ref = '0' or m.cand_ref = 'null', '', concat(char(10),'Relate to Candidate: ',coalesce(cand_fname + ' ' + cand_sname,''))),
				iif(m.cand_email = '' or m.cand_email = 'null', '', concat(char(10),'Candidate Email: ',m.cand_email)),
				iif(m.job_ref = '0' or m.job_ref = 'null', '', concat(char(10),'Relate to Job: ',job_title)),
				iif(m.cont_ref = '0' or m.cont_ref = 'null', '', concat(char(10),'Relate to Contact: ',coalesce(cont_fname + ' ' + cont_sname,''))),
				concat(char(10),'Content Ref: ', m.content_ref)
				) as commentContent
from mmsentcontent m left join consultant cst on m.consult_user = cst.consult_ref
					 left join consultant cst1 on m.consult_from = cst1.consult_ref
					 left join candidate can on m.cand_ref = can.cand_ref
					 left join contact con on m.cont_ref = con.cont_ref
					 left join jobs j on m.job_ref = j.job_ref
where m.cand_ref <> '0'-- and consult_user <> consult_from and consult_from <> 0

--select * from mmsentcontent where consult_user <> consult_from
--select * from eventreason
--select * from eventtype
--select distinct page_owner from mmsentcontent