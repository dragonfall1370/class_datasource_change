select concat('REBEL',e.cand_ref) as CanExternalId, -10 as userId
		, case 
				when event_make_datetime = '00/00/0000 00:00:00' then getdate()
				when event_make_datetime like '%/%' then convert(datetime,event_make_datetime,103)
				else convert(datetime,concat(left(event_make_datetime,4),'/',substring(event_make_datetime,9,2),'/',substring(event_make_datetime,6,2),' ',right(event_make_datetime,8)),120) end as InsertTimeStamp
		, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		, concat('--MIGRATED FROM EVENTS--',
				--iif(event_make_datetime = '' or event_make_datetime = 'null', '', concat(char(10), 'Event Make Date Time: ',event_make_datetime)),
				iif(CHARINDEX('-',event_make_datetime)<>0,concat(char(10), 'Event Make Date Time (d/m/y): ', substring(event_make_datetime,6,2),'/',substring(event_make_datetime,9,2),'/',left(event_make_datetime,4),right(event_make_datetime,9)),concat(char(10), 'Event Make Date Time (d/m/y): ',event_make_datetime)),
				iif(consult_name = '' or consult_name = 'null', '', concat(char(10),'Consultant: ',consult_name)),
				iif(e.cand_ref = '0' or e.cand_ref = 'null', '', concat(char(10),'Relate to Candidate: ',coalesce(cand_fname + ' ' + cand_sname,''))),
				iif(e.job_ref = '0' or e.job_ref = 'null', '', concat(char(10),'Relate to Job: ',job_title)),
				iif(e.cont_ref = '0' or e.cont_ref = 'null', '', concat(char(10),'Relate to Contact: ',coalesce(cont_fname + ' ' + cont_sname,''))),
				iif(e.client_ref = '0' or e.client_ref = 'null', '', concat(char(10),'Relate to Company: ',client_name)),
				iif(event_detail = '' or event_detail = 'null', '', concat(char(10),'Event Detail: ',event_detail)),
				iif(et.event_description = '' or et.event_description = 'null', concat(char(10),'Event Type: ',et.event_type), concat(char(10),'Event Type: ',et.event_type,' (',ltrim(rtrim(et.event_description)),')')),
				iif(event_status = '' or event_status = 'null', '', concat(char(10),'Event Status: ',event_status)),
				iif(e.reason = '0' or e.reason = 'null', '', concat(char(10),'Event Reason: ',er.reason)),
				iif(offer_salary = '' or offer_salary = 'null', '', concat(char(10),'Offer Salary: ',offer_salary)),
				iif(e.content_ref = '0', '', concat(char(10),'Content Reference: ',e.content_ref, iif(mmsc.content_subject in ('','null'),'',concat(' - ',mmsc.content_subject)))),
				iif(int_ref = '0', '', concat(char(10),'Int Ref: ',int_ref)),
				iif(shortlist_ref = '0', '', concat(char(10),'Shortlist Ref: ',shortlist_ref)),
				iif(place_ref = '0', '', concat(char(10),'Place Ref: ',place_ref)),
				concat(char(10),'Event Ref: ', e.event_ref)
				) as commentContent
from events e left join consultant cst on e.consult_ref = cst.consult_ref
			  left join mmsentcontent mmsc on e.content_ref = mmsc.content_ref
			  left join eventtype et on e.event_type = et.event_type
			  left join eventreason er on e.reason = er.id
			  left join candidate can on e.cand_ref = can.cand_ref
			  left join contact con on e.cont_ref = con.cont_ref
			  left join client com on e.client_ref = com.client_ref
			  left join jobs j on e.job_ref = j.job_ref
where e.cand_ref <> '0'

--select * from events where event_make_datetime = 'null'
--select * from eventreason
--select * from eventtype
--select distinct place_ref from events