with 
tmp1 (id, notes) as (SELECT req_id,STUFF((SELECT char(10) + cast(notes as varchar(max)) from req_notes WHERE req_id = a.req_id FOR XML PATH ('')), 1, 1, '')  AS URLList FROM req_notes AS a GROUP BY a.req_id)

, tmp2 (req_id, jobfiles) as (SELECT req_id, STUFF((SELECT ',' + file_title from req_attachments WHERE req_id = ra.req_id FOR XML PATH ('')), 1, 1, '')  AS URLList FROM req_attachments AS ra GROUP BY ra.req_id)

, tmp3 as (select
	id, concat(
		case when (req.id = '' OR req.id is NULL) THEN '' ELSE rq.comments END,
		case when (cast(createdate as varchar(max))  = '' OR createdate is NULL) THEN '' ELSE concat('Create date: ', convert(varchar(10),createdate,120),char(10)) END
		,'Job description files: ',tmp2.jobfiles
	) as jobNote
	from req
	left join tmp2 on req.id = tmp2.req_id
	left join (SELECT req_id,STUFF((SELECT ' Parent Skill: ' + parent_skill + char(10) + ' Child Skill: ' + child_skill + char(10) + char(10) from req_skills WHERE req_id = a.req_id FOR XML PATH ('')), 1, 1, '')  AS comments FROM req_skills AS a GROUP BY a.req_id) rq on req.id = rq.req_id)

select --top 30 
req.id as 'position-externalId'
, mgr_id as 'position-contactId'
, rtrim(ltrim(job_board_title)) as 'position-title'
, max_positions as 'position-headcount'
, users.user_email as 'position-owners'
, (iif(req_eor = 'Contract','CONTRACT'
, iif(req_eor = 'Permanent','PERMANENT'
, iif(req_eor = 'Contract to Perm','TEMPORARY_TO_PERMANENT','')))) as 'position-jobType'
, cast(payrate as int) as 'position-payRate'
, convert(varchar(10),createdate,120) as 'position-startDate'
, convert(varchar(10),enddate,120) as 'position-endDate'
, tmp3.jobNote as 'position-note'
from req
left join tmp1 on req.id = tmp1.id
left join users on req.req_sales_rep = users.ID
left join tmp3 on req.id = tmp3.id
--where tmp3.jobNote like '%Skill%'